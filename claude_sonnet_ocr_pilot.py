"""Claude Sonnet OCR pilot — stores output in a sidecar column, no overwrites.

Selects 100 PDFs (stratified: 30 small / 40 medium / 30 large by existing char count),
fetches each blob from MinIO, renders pages to PNG at 200 DPI, calls claude-sonnet-4-6
with a verbatim transcription prompt per page, concatenates, and stores the result in
core.documents.metadata.claude_sonnet_ocr_v1 alongside the existing falcon output.

Usage:
    python claude_sonnet_ocr_pilot.py --limit 5       # smoke test
    python claude_sonnet_ocr_pilot.py --limit 100     # full pilot
    python claude_sonnet_ocr_pilot.py --report        # summarize completed work
"""
from __future__ import annotations

import argparse
import base64
import io
import json
import logging
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any

import psycopg2
import psycopg2.extras
from pdf2image import convert_from_bytes

sys.path.insert(0, "/opt/wdws")
import blob_storage  # noqa: E402
from anthropic import Anthropic  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("claude-ocr-pilot")

DATABASE_URL = os.environ.get("DATABASE_URL") or _env("DATABASE_URL")
MODEL_ID = "claude-sonnet-4-6"
MAX_OUTPUT_TOKENS = 4000
DPI = int(os.environ.get("CLAUDE_PILOT_DPI", "200"))
PAGE_PROMPT = (
    "Transcribe all text from this page verbatim. "
    "Preserve line breaks, paragraphs, bullet structure, and table layout. "
    "Do not summarize, interpret, or add commentary. "
    "Output only the raw text as it appears on the page. "
    "If the page is truly blank, output exactly: [BLANK PAGE]"
)

INPUT_COST_PER_M = 3.0
OUTPUT_COST_PER_M = 15.0


def _env(key: str) -> str:
    path = "/opt/wdws/.env"
    if not os.path.exists(path):
        return ""
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line.startswith(f"{key}="):
                return line.split("=", 1)[1]
    return ""


if not DATABASE_URL:
    DATABASE_URL = _env("DATABASE_URL")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY") or _env("ANTHROPIC_API_KEY")


@dataclass
class DocResult:
    doc_id: str
    filename: str
    pages: int = 0
    total_chars: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    duration_sec: float = 0.0
    cost_usd: float = 0.0
    error: str | None = None
    per_page_text: list[str] = field(default_factory=list)

    @property
    def full_text(self) -> str:
        return "\n\n".join(self.per_page_text)


def select_candidates(conn, limit: int) -> list[dict]:
    """Stratified sample: 30% small / 40% medium / 30% large by existing char count.

    Skips docs that already have claude_sonnet_ocr_v1 populated so the script is
    resumable — pass --limit 100 again and it picks up where it left off.
    """
    tiers = [
        ("small", "LENGTH(full_content) < 2000", max(1, int(limit * 0.3))),
        ("medium", "LENGTH(full_content) BETWEEN 2000 AND 20000", max(1, int(limit * 0.4))),
        ("large", "LENGTH(full_content) > 20000", max(1, int(limit * 0.3))),
    ]
    all_rows: list[dict] = []
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        for label, where, tier_limit in tiers:
            cur.execute(
                f"""
                SELECT id::text AS id, filename, storage_uri,
                       LENGTH(full_content) AS falcon_chars,
                       total_chunks AS falcon_chunks,
                       '{label}' AS tier
                FROM core.documents
                WHERE legal_hold = false
                  AND purged_at IS NULL
                  AND storage_backend = 'minio'
                  AND storage_uri IS NOT NULL
                  AND filename ~* '\\.pdf$'
                  AND (metadata->'claude_sonnet_ocr_v1') IS NULL
                  AND {where}
                ORDER BY RANDOM()
                LIMIT %s
                """,
                (tier_limit,),
            )
            rows = cur.fetchall()
            all_rows.extend(rows)
            log.info("tier=%s selected=%d", label, len(rows))
    return all_rows[:limit]


def ocr_page(client: Anthropic, png_bytes: bytes) -> tuple[str, int, int]:
    """Send one page to Claude Sonnet. Returns (text, input_tokens, output_tokens)."""
    b64 = base64.standard_b64encode(png_bytes).decode("ascii")
    msg = client.messages.create(
        model=MODEL_ID,
        max_tokens=MAX_OUTPUT_TOKENS,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": "image/png",
                            "data": b64,
                        },
                    },
                    {"type": "text", "text": PAGE_PROMPT},
                ],
            }
        ],
    )
    text = msg.content[0].text if msg.content else ""
    return text, msg.usage.input_tokens, msg.usage.output_tokens


def process_doc(
    client: Anthropic, row: dict, dpi: int = DPI
) -> DocResult:
    r = DocResult(doc_id=row["id"], filename=row["filename"])
    t0 = time.time()
    try:
        blob = blob_storage.get_bytes(row["storage_uri"])
        if not blob:
            r.error = "blob_missing"
            return r
        if not blob.startswith(b"%PDF-"):
            r.error = "not_a_pdf"
            return r
        images = convert_from_bytes(blob, dpi=dpi, fmt="png")
        for idx, img in enumerate(images, start=1):
            buf = io.BytesIO()
            img.save(buf, format="PNG", optimize=True)
            page_png = buf.getvalue()
            text, it, ot = ocr_page(client, page_png)
            r.per_page_text.append(text)
            r.input_tokens += it
            r.output_tokens += ot
            r.pages += 1
            log.info(
                "  %s page %d/%d → %d chars (in=%d out=%d)",
                row["id"][:8], idx, len(images), len(text), it, ot,
            )
    except Exception as e:
        r.error = f"{type(e).__name__}: {str(e)[:200]}"
        log.error("  %s FAILED: %s", row["id"][:8], r.error)
    r.duration_sec = time.time() - t0
    r.total_chars = len(r.full_text)
    r.cost_usd = (
        r.input_tokens * INPUT_COST_PER_M / 1_000_000
        + r.output_tokens * OUTPUT_COST_PER_M / 1_000_000
    )
    return r


def persist(conn, row: dict, r: DocResult) -> None:
    payload: dict[str, Any] = {
        "model": MODEL_ID,
        "text": r.full_text,
        "chars": r.total_chars,
        "pages": r.pages,
        "duration_sec": round(r.duration_sec, 2),
        "input_tokens": r.input_tokens,
        "output_tokens": r.output_tokens,
        "cost_usd": round(r.cost_usd, 4),
        "falcon_chars": row.get("falcon_chars"),
        "tier": row.get("tier"),
        "dpi": DPI,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    if r.error:
        payload["error"] = r.error
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE core.documents
            SET metadata = jsonb_set(
                COALESCE(metadata, '{}'::jsonb),
                '{claude_sonnet_ocr_v1}',
                %s::jsonb,
                true)
            WHERE id = %s
            """,
            (json.dumps(payload), r.doc_id),
        )
    conn.commit()


def report(conn) -> None:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
              COUNT(*) AS n,
              SUM((metadata->'claude_sonnet_ocr_v1'->>'chars')::int) AS claude_total_chars,
              SUM((metadata->'claude_sonnet_ocr_v1'->>'falcon_chars')::int) AS falcon_total_chars,
              SUM((metadata->'claude_sonnet_ocr_v1'->>'pages')::int) AS total_pages,
              SUM((metadata->'claude_sonnet_ocr_v1'->>'input_tokens')::int) AS total_in,
              SUM((metadata->'claude_sonnet_ocr_v1'->>'output_tokens')::int) AS total_out,
              SUM((metadata->'claude_sonnet_ocr_v1'->>'cost_usd')::numeric) AS total_cost,
              AVG((metadata->'claude_sonnet_ocr_v1'->>'duration_sec')::numeric) AS avg_sec
            FROM core.documents
            WHERE metadata ? 'claude_sonnet_ocr_v1'
              AND (metadata->'claude_sonnet_ocr_v1'->>'error') IS NULL
            """
        )
        stats = cur.fetchone() or {}
    print("=" * 60)
    print(f"Pilot completed docs:     {stats.get('n') or 0}")
    print(f"Total pages:              {stats.get('total_pages') or 0}")
    print(f"Falcon total chars:       {stats.get('falcon_total_chars') or 0:,}")
    print(f"Claude total chars:       {stats.get('claude_total_chars') or 0:,}")
    if stats.get('falcon_total_chars'):
        delta = stats['claude_total_chars'] - stats['falcon_total_chars']
        pct = 100 * delta / stats['falcon_total_chars']
        print(f"  Δ vs Falcon:            {delta:+,} ({pct:+.1f}%)")
    print(f"Input tokens:             {stats.get('total_in') or 0:,}")
    print(f"Output tokens:            {stats.get('total_out') or 0:,}")
    print(f"Total cost (Sonnet 4.6):  ${float(stats.get('total_cost') or 0):.2f}")
    print(f"Avg duration/doc:         {float(stats.get('avg_sec') or 0):.1f}s")


def main() -> int:
    global DPI
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=5, help="max docs to process")
    parser.add_argument("--dpi", type=int, default=DPI, help="PDF render DPI")
    parser.add_argument("--concurrency", type=int, default=1, help="parallel docs (1=serial)")
    parser.add_argument("--report", action="store_true", help="print summary and exit")
    args = parser.parse_args()

    DPI = args.dpi

    if not ANTHROPIC_API_KEY:
        log.error("ANTHROPIC_API_KEY not set")
        return 1

    client = Anthropic(api_key=ANTHROPIC_API_KEY)

    with psycopg2.connect(DATABASE_URL) as conn:
        if args.report:
            report(conn)
            return 0

        candidates = select_candidates(conn, args.limit)
        if not candidates:
            log.info("no candidates — all selected docs already have claude_sonnet_ocr_v1")
            return 0
        log.info("Selected %d candidate(s), concurrency=%d", len(candidates), args.concurrency)

        totals = {"ok": 0, "fail": 0, "cost": 0.0, "done": 0}
        db_lock = threading.Lock()
        t_start = time.time()

        def _worker(idx: int, row: dict) -> None:
            log.info(
                "[%d/%d] START %s  %s (tier=%s, falcon=%d chars)",
                idx, len(candidates), row["id"][:8], row["filename"][:50],
                row["tier"], row["falcon_chars"],
            )
            r = process_doc(client, row, dpi=args.dpi)
            with db_lock:
                persist(conn, row, r)
                totals["cost"] += r.cost_usd
                totals["done"] += 1
                if r.error:
                    totals["fail"] += 1
                    log.warning(
                        "[%d/%d] ERR %s %s — $%.4f (cum ok=%d fail=%d $%.2f)",
                        idx, len(candidates), row["id"][:8], r.error, r.cost_usd,
                        totals["ok"], totals["fail"], totals["cost"],
                    )
                else:
                    totals["ok"] += 1
                    falcon_chars = row["falcon_chars"] or 0
                    delta_pct = 100 * (r.total_chars - falcon_chars) / max(falcon_chars, 1)
                    log.info(
                        "[%d/%d] OK  %s pages=%d falcon=%d claude=%d (%+.1f%%) $%.4f %.1fs "
                        "(cum ok=%d fail=%d $%.2f)",
                        idx, len(candidates), row["id"][:8],
                        r.pages, falcon_chars, r.total_chars, delta_pct,
                        r.cost_usd, r.duration_sec,
                        totals["ok"], totals["fail"], totals["cost"],
                    )

        if args.concurrency <= 1:
            for i, row in enumerate(candidates, start=1):
                _worker(i, row)
        else:
            with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
                futures = {
                    ex.submit(_worker, i + 1, row): row
                    for i, row in enumerate(candidates)
                }
                for fut in as_completed(futures):
                    try:
                        fut.result()
                    except Exception as e:
                        log.error("worker crashed: %s", e)

        elapsed = time.time() - t_start
        log.info(
            "Done. ok=%d fail=%d total_cost=$%.2f elapsed=%.1fs (%.1f min)",
            totals["ok"], totals["fail"], totals["cost"], elapsed, elapsed / 60,
        )
        report(conn)
    return 0


if __name__ == "__main__":
    sys.exit(main())
