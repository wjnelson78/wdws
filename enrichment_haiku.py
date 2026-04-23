"""Haiku-based enrichment pass over core.documents.full_content.

Reads full_content (emails + OCR'd documents), sends to claude-haiku-4-5 with a
tool-use schema, and stores structured metadata in documents.metadata.enrichment_v1.

Fields extracted:
  - summary:          1-sentence plain-English summary
  - parties:          list of people/organizations mentioned
  - case_citations:   list of legal citations ("Smith v. Jones, 123 F.3d 456")
  - dates:            list of {date, context} objects
  - action_items:     list of things the reader should do
  - topics:           subject-area tags (employment, insurance, motion_practice, ...)
  - importance:       high | medium | low | informational

Uses Anthropic tool-use for reliable structured output. Resumable — skips docs
that already have enrichment_v1.

Usage:
    python enrichment_haiku.py --limit 5              # smoke test
    python enrichment_haiku.py --limit 100 --concurrency 10
    python enrichment_haiku.py --all --concurrency 10 # everything enrichable
    python enrichment_haiku.py --report               # summary of completed work
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import psycopg2
import psycopg2.extras

sys.path.insert(0, "/opt/wdws")
from anthropic import Anthropic  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("enrichment-haiku")

MODEL_ID = "claude-haiku-4-5-20251001"
MAX_INPUT_CHARS = 80_000   # ~20k tokens — Haiku handles 200k but truncate for cost
MAX_OUTPUT_TOKENS = 1500
INPUT_COST_PER_M = 1.0
OUTPUT_COST_PER_M = 5.0

DATABASE_URL = ""
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
with open("/opt/wdws/.env") as f:
    for line in f:
        if line.startswith("DATABASE_URL="):
            DATABASE_URL = line.split("=", 1)[1].strip()
        elif line.startswith("ANTHROPIC_API_KEY=") and not ANTHROPIC_API_KEY:
            ANTHROPIC_API_KEY = line.split("=", 1)[1].strip()

ENRICHMENT_TOOL = {
    "name": "record_enrichment",
    "description": "Record structured metadata extracted from a document.",
    "input_schema": {
        "type": "object",
        "properties": {
            "summary": {
                "type": "string",
                "description": "A single sentence that states what the document is and its main point.",
            },
            "parties": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Names of people or organizations that are substantively involved — senders, recipients, plaintiffs, defendants, providers, attorneys. Exclude boilerplate footers and signature blocks.",
            },
            "case_citations": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Legal citations in canonical form (e.g. 'Smith v. Jones, 123 F.3d 456 (9th Cir. 2020)'). Empty list if none.",
            },
            "dates": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "date": {"type": "string", "description": "ISO 8601 if possible, else the raw string"},
                        "context": {"type": "string", "description": "What this date is — 'hearing', 'filed', 'deadline', 'service', etc."},
                    },
                    "required": ["date", "context"],
                },
                "description": "Dates that matter for legal/medical/business purposes. Exclude random timestamps and signature dates.",
            },
            "action_items": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Things the recipient is being asked or told to do, written as imperative sentences. Empty if none.",
            },
            "topics": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Subject-area tags in snake_case: employment_discrimination, insurance_dispute, medical_records, ada_accommodation, motion_practice, etc.",
            },
            "importance": {
                "type": "string",
                "enum": ["high", "medium", "low", "informational"],
                "description": "high = needs action soon / legal deadline / material adverse event. medium = substantive but not urgent. low = minor update. informational = FYI / automated / newsletter.",
            },
        },
        "required": ["summary", "parties", "case_citations", "dates", "action_items", "topics", "importance"],
    },
}


def enrich_one(client: Anthropic, text: str, filename: str, doc_type: str) -> dict:
    """Call Haiku with the enrichment tool. Returns {data, input_tokens, output_tokens, cost_usd, error}."""
    truncated = text[:MAX_INPUT_CHARS]
    if len(text) > MAX_INPUT_CHARS:
        truncated += "\n\n[... content truncated after 80,000 characters ...]"

    system_prompt = (
        "You are a legal/medical document analyst. Extract structured metadata from the "
        "document below. Focus on facts present in the text — do not speculate. If a field "
        "has no applicable content, return an empty list. Respond only via the "
        "record_enrichment tool."
    )
    user_content = (
        f"Filename: {filename}\n"
        f"Document type: {doc_type}\n\n"
        f"---\n{truncated}\n---"
    )

    try:
        msg = client.messages.create(
            model=MODEL_ID,
            max_tokens=MAX_OUTPUT_TOKENS,
            system=system_prompt,
            tools=[ENRICHMENT_TOOL],
            tool_choice={"type": "tool", "name": "record_enrichment"},
            messages=[{"role": "user", "content": user_content}],
        )
    except Exception as e:
        return {"error": f"{type(e).__name__}: {str(e)[:200]}",
                "input_tokens": 0, "output_tokens": 0, "cost_usd": 0.0}

    tool_blocks = [b for b in msg.content if b.type == "tool_use"]
    if not tool_blocks:
        return {"error": "no_tool_use_in_response",
                "input_tokens": msg.usage.input_tokens,
                "output_tokens": msg.usage.output_tokens,
                "cost_usd": (msg.usage.input_tokens * INPUT_COST_PER_M / 1e6
                             + msg.usage.output_tokens * OUTPUT_COST_PER_M / 1e6)}

    cost = (msg.usage.input_tokens * INPUT_COST_PER_M / 1e6
            + msg.usage.output_tokens * OUTPUT_COST_PER_M / 1e6)
    return {
        "data": tool_blocks[0].input,
        "input_tokens": msg.usage.input_tokens,
        "output_tokens": msg.usage.output_tokens,
        "cost_usd": cost,
    }


def persist(conn, doc_id: str, result: dict, meta: dict) -> None:
    payload: dict[str, Any] = {
        "model": MODEL_ID,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "input_tokens": result["input_tokens"],
        "output_tokens": result["output_tokens"],
        "cost_usd": round(result["cost_usd"], 6),
        "truncated": meta["truncated"],
    }
    if "error" in result:
        payload["error"] = result["error"]
    else:
        payload.update(result["data"])

    with conn.cursor() as cur:
        cur.execute(
            """UPDATE core.documents
               SET metadata = jsonb_set(
                   COALESCE(metadata, '{}'::jsonb),
                   '{enrichment_v1}',
                   %s::jsonb, true)
               WHERE id = %s""",
            (json.dumps(payload), doc_id),
        )
    conn.commit()


def select_candidates(conn, limit: int) -> list[dict]:
    """Prefer emails first (high count, clean text, fast) then everything else."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """SELECT id::text AS id, filename, document_type, domain,
                      LENGTH(full_content) AS chars
               FROM core.documents
               WHERE legal_hold = false
                 AND purged_at IS NULL
                 AND full_content IS NOT NULL
                 AND LENGTH(full_content) > 100
                 AND NOT (metadata ? 'enrichment_v1')
               ORDER BY
                 CASE WHEN document_type = 'email' THEN 0 ELSE 1 END,
                 RANDOM()
               LIMIT %s""",
            (limit,),
        )
        return cur.fetchall()


def fetch_content(conn, doc_id: str) -> str:
    with conn.cursor() as cur:
        cur.execute("SELECT full_content FROM core.documents WHERE id = %s",
                    (doc_id,))
        row = cur.fetchone()
        return row[0] if row else ""


def report(conn) -> None:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT
              COUNT(*) AS n,
              COUNT(*) FILTER (WHERE (metadata->'enrichment_v1'->>'error') IS NOT NULL) AS fail,
              SUM((metadata->'enrichment_v1'->>'input_tokens')::int)  AS in_tok,
              SUM((metadata->'enrichment_v1'->>'output_tokens')::int) AS out_tok,
              SUM((metadata->'enrichment_v1'->>'cost_usd')::numeric)  AS cost
            FROM core.documents WHERE metadata ? 'enrichment_v1'
        """)
        s = cur.fetchone() or {}

        cur.execute("""
            SELECT importance, COUNT(*) AS n FROM (
              SELECT metadata->'enrichment_v1'->>'importance' AS importance
              FROM core.documents WHERE metadata ? 'enrichment_v1'
                AND (metadata->'enrichment_v1'->>'error') IS NULL
            ) t GROUP BY importance ORDER BY n DESC
        """)
        imp_breakdown = cur.fetchall()

    print("=" * 60)
    print(f"Enriched docs:             {s.get('n') or 0}")
    print(f"Failures:                  {s.get('fail') or 0}")
    print(f"Input tokens:              {s.get('in_tok') or 0:,}")
    print(f"Output tokens:             {s.get('out_tok') or 0:,}")
    print(f"Total cost (Haiku 4.5):    ${float(s.get('cost') or 0):.2f}")
    print()
    print("Importance breakdown:")
    for r in imp_breakdown:
        print(f"  {r['importance']:15s}  {r['n']:6d}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--all", action="store_true", help="process all enrichable docs")
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--report", action="store_true")
    args = parser.parse_args()

    if not ANTHROPIC_API_KEY:
        log.error("ANTHROPIC_API_KEY not set")
        return 1

    client = Anthropic(api_key=ANTHROPIC_API_KEY)
    with psycopg2.connect(DATABASE_URL) as conn:
        if args.report:
            report(conn)
            return 0

        limit = 100_000 if args.all else args.limit
        candidates = select_candidates(conn, limit)
        if not candidates:
            log.info("no candidates — everything enrichable already has enrichment_v1")
            return 0
        log.info("Selected %d candidate(s), concurrency=%d", len(candidates), args.concurrency)

        totals = {"ok": 0, "fail": 0, "cost": 0.0, "in_tok": 0, "out_tok": 0}
        db_lock = threading.Lock()
        t_start = time.time()

        def _worker(idx: int, row: dict) -> None:
            text = fetch_content(conn, row["id"])
            meta = {"truncated": len(text) > MAX_INPUT_CHARS}
            r = enrich_one(client, text, row["filename"], row["document_type"])
            with db_lock:
                persist(conn, row["id"], r, meta)
                totals["cost"] += r["cost_usd"]
                totals["in_tok"] += r["input_tokens"]
                totals["out_tok"] += r["output_tokens"]
                if "error" in r:
                    totals["fail"] += 1
                    log.warning(
                        "[%d/%d] ERR %s %s: %s (cum ok=%d fail=%d $%.3f)",
                        idx, len(candidates), row["id"][:8], row["document_type"],
                        r["error"], totals["ok"], totals["fail"], totals["cost"],
                    )
                else:
                    totals["ok"] += 1
                    data = r["data"]
                    log.info(
                        "[%d/%d] OK  %s %s imp=%s  parties=%d cites=%d dates=%d actions=%d  $%.4f "
                        "(cum ok=%d fail=%d $%.3f)",
                        idx, len(candidates), row["id"][:8], row["document_type"],
                        data.get("importance", "?"),
                        len(data.get("parties", [])),
                        len(data.get("case_citations", [])),
                        len(data.get("dates", [])),
                        len(data.get("action_items", [])),
                        r["cost_usd"],
                        totals["ok"], totals["fail"], totals["cost"],
                    )

        if args.concurrency <= 1:
            for i, row in enumerate(candidates, start=1):
                _worker(i, row)
        else:
            with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
                futures = {ex.submit(_worker, i + 1, r): r for i, r in enumerate(candidates)}
                for fut in as_completed(futures):
                    try:
                        fut.result()
                    except Exception as e:
                        log.error("worker crashed: %s", e)

        elapsed = time.time() - t_start
        log.info("Done. ok=%d fail=%d cost=$%.3f elapsed=%.1fs (%.1f min)",
                 totals["ok"], totals["fail"], totals["cost"], elapsed, elapsed / 60)
        report(conn)
    return 0


if __name__ == "__main__":
    sys.exit(main())
