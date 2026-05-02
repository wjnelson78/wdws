"""
WA Court Rules Phase 4 Ingest Driver
=====================================
Ingests 1,145 parsed rules from phase3_parsed.jsonl into wa_court_rules.* schema,
with embeddings via HF endpoint at 172.16.81.187:9098 (BGE-M3, 1024-dim).

Locked operator-approved plan (2026-04-30 22:50 PT):
  Tier 1 (515 rules): GR, RPC, CR, ER, RAP, CJC, GALR — Pause-Checkpoint A
  Tier 2 (630 rules): CrR, CrRLJ, CRLJ, JuCR, SCCAR, RALJ, APR, ELC — Pause-Checkpoint B

Per-rule path:
  1. INSERT core.documents (FK target for wa_court_rules.rules.document_id)
  2. Generate BGE-M3 embedding(s) via HF .187 (chunk only if full_text > 6000 chars)
  3. INSERT core.document_chunks (with embeddings)
  4. INSERT wa_court_rules.rules
  5. INSERT wa_court_rules.rule_history rows

Two-pass FK resolution after rule INSERTs:
  Pass 1: range_doc_id (resolve range_canonical_citation → UUID)
  Pass 2: target_rule_id on cross_references (resolve target tuple → UUID; NULL if not found)

Halt protocol — write HALT_PHASE4.md and exit non-zero on:
  - CHECK constraint violation
  - Pattern (b) FAIL on any priority rule set
  - HF endpoint degradation (>5 sec response or 5xx)
  - Authority resolution rate <70% on cross-refs to in-scope targets
  - Embedding wall-time >2x estimate (~6 min for 1,145 rules)
  - range_doc_id FK resolution leaves any expansion child unresolved

Cumulative report → phase4_cumulative_report.md
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import sys
import time
import traceback
from pathlib import Path
from typing import Any

import httpx
import psycopg2
import psycopg2.extras

# ── Config ────────────────────────────────────────────────────────────────────

DSN = os.environ.get("DATABASE_URL", "postgresql://wdws:NEL2233obs@127.0.0.1:5432/wdws")
PARSED_JSONL = Path("/opt/wdws/wa_court_rules_research/phase3_parsed.jsonl")
REPORT_PATH = Path("/opt/wdws/wa_court_rules_research/phase4_cumulative_report.md")
HALT_PATH = Path("/opt/wdws/wa_court_rules_research/HALT_PHASE4.md")

HF_ENDPOINT = "http://172.16.81.187:9098"
HF_API_TOKEN = os.environ.get("HF_API_TOKEN", "")
if not HF_API_TOKEN:
    # Load from .env
    env_path = Path("/opt/wdws/.env")
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line.startswith("HF_API_TOKEN="):
            HF_API_TOKEN = line.split("=", 1)[1].strip().strip('"').strip("'")
            break
assert HF_API_TOKEN, "HF_API_TOKEN missing from env + /opt/wdws/.env"

EMBEDDING_MODEL_ID = 2  # BAAI/bge-m3
EMBEDDING_DIMS = 1024
CHUNK_TARGET_CHARS = 6000
HF_BATCH_SIZE = 32
HF_TIMEOUT = 30.0

PUBLICATION_DATE = dt.date(2026, 4, 30)

# ── Tier order (operator-locked) ──────────────────────────────────────────────

RULE_SET_NAMES = {
    "GR":    "General Rules",
    "RPC":   "Rules of Professional Conduct",
    "CR":    "Superior Court Civil Rules",
    "ER":    "Rules of Evidence",
    "RAP":   "Rules of Appellate Procedure",
    "CJC":   "Code of Judicial Conduct",
    "GALR":  "Guardian ad Litem Rules",
    "CrR":   "Superior Court Criminal Rules",
    "CrRLJ": "Criminal Rules for Limited Jurisdiction",
    "CRLJ":  "Civil Rules for Limited Jurisdiction",
    "JuCR":  "Juvenile Court Rules",
    "SCCAR": "Superior Court Civil Arbitration Rules",
    "RALJ":  "Limited Jurisdiction Appeal Rules",
    "APR":   "Admission and Practice Rules",
    "ELC":   "Lawyer Conduct Enforcement Rules",
}

TIER_1_ORDER = ["GR", "RPC", "CR", "ER", "RAP", "CJC", "GALR"]
TIER_2_ORDER = ["CrR", "CrRLJ", "CRLJ", "JuCR", "SCCAR", "RALJ", "APR", "ELC"]

EXPECTED_COUNTS = {
    "GR": 58, "RPC": 64, "CR": 101, "ER": 67, "RAP": 183, "CJC": 35, "GALR": 7,
    "CrR": 65, "CrRLJ": 76, "CRLJ": 115, "JuCR": 99, "SCCAR": 27, "RALJ": 45,
    "APR": 70, "ELC": 133,
}

# Anchors for canaries
CANARIES_TIER_1 = {
    "GR":   {"semantic": "access to court records", "anchor": "GR 31",  "extras": ["GR 33", "GR 34"], "fts": "records"},
    "RPC":  {"semantic": "attorney competence",     "anchor": "RPC 1.1", "extras": ["RPC 1.5", "RPC 5.5"], "fts": "competence"},
    "CR":   {"semantic": "summary judgment",        "anchor": "CR 56",  "extras": ["CR 12", "CR 26"], "fts": "subpoena"},
    "ER":   {"semantic": "relevance of evidence",   "anchor": "ER 401", "extras": ["ER 403", "ER 802"], "fts": "hearsay"},
    "RAP":  {"semantic": "appeal procedure",        "anchor": "RAP 18.13A", "extras": [], "fts": "appellate"},
    "CJC":  {"semantic": "judicial impartiality",   "anchor": "CJC 2.11", "extras": ["CJC 1.1"], "fts": "judge"},  # 2026-05-01: was ["CJC Canon 1"], invalid per Phase 0 Decision 5 (Canon-level rows not ingested)
    "GALR": {"semantic": "guardian ad litem scope", "anchor": "GALR 1", "extras": ["GALR 5"], "fts": "guardian"},
}
CANARIES_TIER_2 = {
    "CrR":   {"semantic": "criminal procedure scope", "anchor": "CrR 1.1", "extras": ["CrR 3.5"], "fts": "criminal"},
    "CrRLJ": {"semantic": "limited jurisdiction criminal", "anchor": "CrRLJ 1.1", "extras": [], "fts": "criminal"},
    "CRLJ":  {"semantic": "limited jurisdiction civil",    "anchor": "CRLJ 30",   "extras": ["CRLJ 92.04"], "fts": "civil"},
    "JuCR":  {"semantic": "juvenile court",  "anchor": "JuCR 1.1", "extras": ["JuCR 5A.1"], "fts": "juvenile"},
    "SCCAR": {"semantic": "civil arbitration", "anchor": "SCCAR 1.1", "extras": [], "fts": "arbitration"},
    "RALJ":  {"semantic": "appeal limited jurisdiction", "anchor": "RALJ 1.1", "extras": [], "fts": "appeal"},
    "APR":   {"semantic": "admission and practice", "anchor": "APR 15P", "extras": ["APR Regs 101"], "fts": "admission"},
    "ELC":   {"semantic": "lawyer discipline procedure", "anchor": "ELC 1.1", "extras": ["ELC 5.1", "ELC 13.1"], "fts": "discipline"},
}

# ── HF embedding client ───────────────────────────────────────────────────────

_hf_client: httpx.Client | None = None


def hf_client() -> httpx.Client:
    global _hf_client
    if _hf_client is None:
        _hf_client = httpx.Client(
            timeout=HF_TIMEOUT,
            headers={
                "Authorization": f"Bearer {HF_API_TOKEN}",
                "Content-Type": "application/json",
            },
        )
    return _hf_client


_embed_call_count = 0
_embed_total_seconds = 0.0
_embed_max_seconds = 0.0


def hf_embed_batch(texts: list[str]) -> list[list[float]]:
    """Call HF .187 /embed; raise on any failure (no silent fallback)."""
    global _embed_call_count, _embed_total_seconds, _embed_max_seconds
    if not texts:
        return []
    out: list[list[float]] = []
    client = hf_client()
    for i in range(0, len(texts), HF_BATCH_SIZE):
        chunk = texts[i : i + HF_BATCH_SIZE]
        # Truncate to ~32k chars per text (well under 8192-token max for BGE-M3)
        chunk = [(t or "")[:32768] for t in chunk]
        t0 = time.time()
        r = client.post(f"{HF_ENDPOINT}/embed", json={"inputs": chunk})
        elapsed = time.time() - t0
        _embed_call_count += 1
        _embed_total_seconds += elapsed
        _embed_max_seconds = max(_embed_max_seconds, elapsed)
        if elapsed > 5.0:
            raise RuntimeError(
                f"HF endpoint degradation: batch of {len(chunk)} took {elapsed:.2f}s (>5s threshold)"
            )
        if r.status_code >= 500:
            raise RuntimeError(f"HF endpoint 5xx: {r.status_code} {r.text[:200]}")
        r.raise_for_status()
        vecs = r.json()
        if len(vecs) != len(chunk):
            raise RuntimeError(f"HF returned {len(vecs)} vectors for {len(chunk)} inputs")
        for v in vecs:
            if len(v) != EMBEDDING_DIMS:
                raise RuntimeError(f"HF returned {len(v)}-dim vector, expected {EMBEDDING_DIMS}")
        out.extend(vecs)
    return out


def vec_literal(vec: list[float]) -> str:
    return "[" + ",".join(f"{v:.6f}" for v in vec) + "]"


# ── Halt machinery ────────────────────────────────────────────────────────────

class HaltError(Exception):
    pass


def write_halt(reason: str, details: str) -> None:
    HALT_PATH.write_text(
        f"# Phase 4 HALT\n\n"
        f"**Time:** {dt.datetime.now().isoformat()}\n\n"
        f"**Reason:** {reason}\n\n"
        f"## Details\n\n```\n{details}\n```\n"
    )
    print(f"\n!!! HALT: {reason}\n{details}\n", file=sys.stderr, flush=True)


# ── Chunking ──────────────────────────────────────────────────────────────────

def chunk_text(full_text: str, breadcrumb: str) -> list[str]:
    if not full_text or not full_text.strip():
        return []
    full = f"{breadcrumb}\n\n{full_text}"
    if len(full) <= CHUNK_TARGET_CHARS:
        return [full]
    # Hard slice for long rules
    out = []
    paras = full_text.split("\n\n")
    buf = ""
    overhead = len(breadcrumb) + 4
    target = CHUNK_TARGET_CHARS - overhead
    for p in paras:
        if len(buf) + len(p) + 2 <= target:
            buf = (buf + "\n\n" + p) if buf else p
        else:
            if buf:
                out.append(f"{breadcrumb}\n\n{buf}")
                buf = ""
            if len(p) > target:
                # Hard slice the paragraph
                for i in range(0, len(p), target):
                    out.append(f"{breadcrumb}\n\n{p[i : i + target]}")
            else:
                buf = p
    if buf:
        out.append(f"{breadcrumb}\n\n{buf}")
    return out


# ── Ingest ────────────────────────────────────────────────────────────────────

def ensure_rule_set(conn, code: str) -> None:
    """Lazy-populate rule_sets row on first sight of code."""
    name = RULE_SET_NAMES[code]
    tier = 1 if code in TIER_1_ORDER else 2
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO wa_court_rules.rule_sets
                (rule_set_code, rule_set_name, promulgating_court, statutory_authority,
                 publication_date, tier, is_in_scope)
            VALUES (%s, %s, 'WA Supreme Court', 'RCW 2.04.190', %s, %s, TRUE)
            ON CONFLICT (rule_set_code) DO NOTHING
        """, (code, name, PUBLICATION_DATE, tier))


def refine_ghost_flags(body: str, is_repealed: bool, is_reserved: bool):
    """Pre-INSERT refinement: if body contains both 'RESCINDED' and 'RESERVED'
    markers (case-insensitive), set both flags. Some rules are simultaneously
    rescinded AND reserved (e.g., GR 8: 'RESCINDED and RESERVED, LAWS OF 2002,
    CH. 136'). Operator-approved 2026-05-01 (Phase 4 boundary). Mirrored from
    phase1_regex_test.py:refine_ghost_flags; T17.24 covers this contract.
    """
    body_upper = body.upper() if body else ""
    has_rescinded = "RESCINDED" in body_upper or "RESCINDS" in body_upper
    has_reserved = "RESERVED" in body_upper or "[RESERVED]" in body_upper
    if has_rescinded and has_reserved:
        return (True, True)
    return (is_repealed, is_reserved)


def ingest_one_rule(conn, rec: dict) -> str:
    """Returns the new wa_court_rules.rules.id."""
    code = rec["rule_set_code"]
    citation = rec["canonical_citation"]
    full_text = rec.get("full_text") or ""

    # Pre-INSERT refinement: detect both-RESCINDED-and-RESERVED bodies
    # (operator-approved 2026-05-01 per Phase 4 boundary halt resolution).
    # Mutates rec in place so both metadata and INSERT see refined flags.
    refined_repealed, refined_reserved = refine_ghost_flags(
        full_text,
        rec.get("is_repealed", False),
        rec.get("is_reserved", False),
    )
    if (refined_repealed, refined_reserved) != (rec.get("is_repealed", False), rec.get("is_reserved", False)):
        print(f"  refine_ghost_flags: {citation} {rec.get('is_repealed')}/{rec.get('is_reserved')} -> {refined_repealed}/{refined_reserved} (both RESCINDED+RESERVED markers detected)", flush=True)
    rec["is_repealed"] = refined_repealed
    rec["is_reserved"] = refined_reserved

    # 1. core.documents
    source_path = f"wa-court-rule:{citation}"
    title_str = f"{citation} — {rec['rule_title']}"
    metadata = {
        "type": "wa_court_rule",
        "rule_set_code": code,
        "rule_number": rec["rule_number"],
        "rule_title": rec["rule_title"],
        "canonical_citation": citation,
        "publication_date": PUBLICATION_DATE.isoformat(),
        "is_repealed": rec.get("is_repealed", False),
        "is_reserved": rec.get("is_reserved", False),
        "parent_canon": rec.get("parent_canon"),
        "parent_regulation_handle": rec.get("parent_regulation_handle"),
        "range_canonical_citation": rec.get("range_canonical_citation"),
        "range_start": rec.get("range_start"),
        "range_end": rec.get("range_end"),
        "effective_date": rec.get("effective_date"),
        "last_amended_date": rec.get("last_amended_date"),
        "source_pdf_path": rec.get("source_pdf_path"),
    }
    content_hash = hashlib.sha256(full_text.encode("utf-8")).hexdigest()
    filename = source_path.replace("wa-court-rule:", "").replace(" ", "_") + ".txt"

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO core.documents
                (domain, document_type, title, source_path, filename,
                 full_content, content_hash, metadata,
                 classification, confidentiality, legal_hold,
                 privilege, privilege_classified_at, privilege_classified_by)
            VALUES
                ('legal', 'court_rule', %s, %s, %s,
                 %s, %s, %s::jsonb,
                 'public', 'public', FALSE,
                 'none', NOW(), 'rule:court_rule_is_public')
            RETURNING id
        """, (title_str, source_path, filename,
              full_text, content_hash, json.dumps(metadata)))
        document_id = cur.fetchone()[0]

        # 2. + 3. Embed + insert chunks (only if there's full_text)
        chunks: list[str] = []
        if full_text and full_text.strip():
            breadcrumb = f"{RULE_SET_NAMES[code]} ({code}) — {citation}: {rec['rule_title']}"
            chunks = chunk_text(full_text, breadcrumb)

        if chunks:
            vecs = hf_embed_batch(chunks)
            chunk_rows = []
            for i, (ctext, vec) in enumerate(zip(chunks, vecs)):
                chunk_id = f"{document_id}:{i}"
                chunk_rows.append((
                    chunk_id, document_id, i, len(chunks), ctext,
                    vec_literal(vec), EMBEDDING_MODEL_ID,
                    len(ctext.split()), ctext, dt.datetime.now(dt.timezone.utc),
                ))
            psycopg2.extras.execute_values(cur, """
                INSERT INTO core.document_chunks
                    (id, document_id, chunk_index, total_chunks, content,
                     embedding, embedding_model_id, token_count, embedded_content,
                     embedded_at)
                VALUES %s
            """, chunk_rows, template="(%s, %s, %s, %s, %s, %s::halfvec(1024), %s, %s, %s, %s)")
            cur.execute("UPDATE core.documents SET total_chunks=%s WHERE id=%s",
                        (len(chunks), document_id))
        else:
            cur.execute("UPDATE core.documents SET total_chunks=0 WHERE id=%s", (document_id,))

        # 4. wa_court_rules.rules
        eff_date = rec.get("effective_date")
        amended = rec.get("last_amended_date")
        cur.execute("""
            INSERT INTO wa_court_rules.rules
                (document_id, rule_set_code, rule_number, rule_title, canonical_citation,
                 is_repealed, is_reserved,
                 range_doc_id, range_start, range_end,
                 parent_canon, parent_regulation_handle,
                 full_text, source_pdf_path,
                 has_comment, comment_text,
                 effective_date, last_amended_date, publication_date)
            VALUES (%s, %s, %s, %s, %s,
                    %s, %s,
                    NULL, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s, %s)
            RETURNING id
        """, (
            document_id, code, rec["rule_number"], rec["rule_title"], citation,
            rec.get("is_repealed", False), rec.get("is_reserved", False),
            rec.get("range_start"), rec.get("range_end"),
            rec.get("parent_canon"), rec.get("parent_regulation_handle"),
            (full_text if full_text else None),
            rec.get("source_pdf_path"),
            rec.get("has_comment", False), rec.get("comment_text"),
            eff_date, amended, PUBLICATION_DATE,
        ))
        rule_id = cur.fetchone()[0]

        # 5. rule_history
        history = rec.get("history") or []
        if history:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO wa_court_rules.rule_history
                    (rule_id, amendment_date, amendment_text, sort_order)
                VALUES %s
            """, [(rule_id, h.get("amendment_date"), h["amendment_text"], h["sort_order"])
                  for h in history])

    return rule_id


# ── FK Resolution Passes ──────────────────────────────────────────────────────

def resolve_range_doc_ids(conn, citation_to_uuid: dict[str, str], parsed_records: list[dict]) -> dict[str, Any]:
    """Pass 1: resolve range_canonical_citation → range_doc_id UUID."""
    unresolved: list[tuple[str, str]] = []
    resolved = 0
    with conn.cursor() as cur:
        for rec in parsed_records:
            range_canon_cite = rec.get("range_canonical_citation")
            if not range_canon_cite:
                continue
            child_cite = rec["canonical_citation"]
            target_uuid = citation_to_uuid.get(range_canon_cite)
            if not target_uuid:
                unresolved.append((child_cite, range_canon_cite))
                continue
            child_uuid = citation_to_uuid[child_cite]
            cur.execute("""
                UPDATE wa_court_rules.rules
                SET range_doc_id = %s
                WHERE id = %s
            """, (target_uuid, child_uuid))
            resolved += 1
    return {"resolved": resolved, "unresolved": unresolved}


def insert_cross_references(conn, citation_to_uuid: dict[str, str], parsed_records: list[dict]) -> dict[str, Any]:
    """Pass 2: insert cross_references with target_rule_id FK + self-cite filter."""
    inserted = 0
    skipped_self = 0
    resolved_inscope = 0
    unresolved_inscope = 0
    resolved_xjur = 0  # cross-jurisdiction (RCW/USC/WAC/etc.)

    in_scope_codes = set(TIER_1_ORDER) | set(TIER_2_ORDER)

    rows: list[tuple] = []
    with conn.cursor() as cur:
        for rec in parsed_records:
            src_code = rec["rule_set_code"]
            src_num = rec["rule_number"]
            src_uuid = citation_to_uuid[rec["canonical_citation"]]
            for xref in (rec.get("cross_references") or []):
                t_set = xref.get("target_set_code")
                t_num = xref.get("target_rule_number")
                t_jur = xref.get("target_jurisdiction") or "state-wa-court-rule"
                # Self-cite filter
                if t_set == src_code and t_num == src_num:
                    skipped_self += 1
                    continue

                # Resolve target_rule_id only for state-wa-court-rule cites
                target_uuid = None
                if t_jur == "state-wa-court-rule" and t_set and t_num:
                    target_cite = f"{t_set} {t_num}"
                    target_uuid = citation_to_uuid.get(target_cite)
                    if t_set in in_scope_codes:
                        if target_uuid:
                            resolved_inscope += 1
                        else:
                            unresolved_inscope += 1
                else:
                    if target_uuid:
                        resolved_xjur += 1

                rows.append((
                    src_uuid,
                    xref.get("target_citation") or f"{t_set} {t_num}".strip(),
                    target_uuid,
                    t_jur,
                    t_set,
                    t_num,
                    xref.get("target_subsection"),
                    xref.get("target_cmt_index"),
                    xref.get("context_snippet"),
                    xref.get("ref_type") or "reference",
                ))

        if rows:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO wa_court_rules.cross_references
                    (source_rule_id, target_citation, target_rule_id,
                     target_jurisdiction, target_set_code, target_rule_number,
                     target_subsection, target_cmt_index, context_snippet, ref_type)
                VALUES %s
            """, rows)
            inserted = len(rows)

    in_scope_total = resolved_inscope + unresolved_inscope
    in_scope_rate = (resolved_inscope / in_scope_total) if in_scope_total else 1.0
    return {
        "inserted": inserted,
        "skipped_self": skipped_self,
        "resolved_inscope": resolved_inscope,
        "unresolved_inscope": unresolved_inscope,
        "in_scope_resolution_rate": in_scope_rate,
    }


# ── Canaries ──────────────────────────────────────────────────────────────────

def canary_pattern_b(conn, anchors: list[str]) -> list[tuple[str, bool, str]]:
    """Deterministic citation lookup. Returns list of (anchor, ok, msg)."""
    results = []
    with conn.cursor() as cur:
        for cite in anchors:
            cur.execute("""
                SELECT canonical_citation, full_text IS NOT NULL AS has_text,
                       LENGTH(COALESCE(full_text,'')) AS tlen
                FROM wa_court_rules.rules WHERE canonical_citation=%s
            """, (cite,))
            rows = cur.fetchall()
            if len(rows) != 1:
                results.append((cite, False, f"expected 1 row, got {len(rows)}"))
            else:
                _, has_text, tlen = rows[0]
                results.append((cite, True, f"row found, full_text len={tlen}"))
    return results


def canary_pattern_c(conn, code: str, fts_term: str) -> tuple[bool, str]:
    """Postgres FTS against full_text+title via core.documents tsvector."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM core.documents d
            JOIN wa_court_rules.rules r ON r.document_id = d.id
            WHERE r.rule_set_code = %s
              AND d.full_content_tsv @@ plainto_tsquery('english', %s)
        """, (code, fts_term))
        n = cur.fetchone()[0]
    return (n > 0, f"FTS '{fts_term}' on {code}: {n} hits")


def canary_pattern_a(conn, code: str, semantic_query: str, anchor_cite: str) -> tuple[bool, str]:
    """Semantic search via HF embedding + halfvec cosine on chunks scoped to rule_set_code.
    LOG-ONLY (Phase 6 §H2)."""
    try:
        qvec = hf_embed_batch([semantic_query])[0]
        qlit = vec_literal(qvec)
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT r.canonical_citation, MIN(c.embedding <=> %s::halfvec(1024)) AS dist
                FROM core.document_chunks c
                JOIN wa_court_rules.rules r ON r.document_id = c.document_id
                WHERE r.rule_set_code = %s
                GROUP BY r.canonical_citation
                ORDER BY dist ASC
                LIMIT 10
            """, (qlit, code))
            rows = cur.fetchall()
        top10 = [r[0] for r in rows]
        ok = anchor_cite in top10
        return (ok, f"top10 for '{semantic_query}' in {code}: {top10[:5]}... anchor={anchor_cite} {'HIT' if ok else 'MISS'}")
    except Exception as e:
        return (False, f"pattern (a) error: {e}")


# ── Tier driver ───────────────────────────────────────────────────────────────

def run_tier(conn, tier_num: int, codes: list[str], parsed_by_code: dict[str, list[dict]],
             citation_to_uuid: dict[str, str],
             already_ingested: dict[str, int]) -> dict[str, Any]:
    """Ingest one tier's rule sets and run canaries between sets.

    Resume-aware: if a set is already at expected row count (from a prior
    partial run), skip ingest but hydrate citation_to_uuid from DB so FK
    resolution + canaries still work. Halt on partial-ingest mismatch.
    """
    results: dict[str, Any] = {"sets": {}}
    canary_table = CANARIES_TIER_1 if tier_num == 1 else CANARIES_TIER_2

    for code in codes:
        recs = parsed_by_code[code]
        expected = EXPECTED_COUNTS[code]
        if len(recs) != expected:
            raise HaltError(f"{code}: parsed {len(recs)} records, expected {expected}")

        if already_ingested.get(code) == expected:
            # Resume path — set already correctly ingested in prior run.
            # Hydrate citation_to_uuid from DB so FK resolution works.
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT canonical_citation, id FROM wa_court_rules.rules
                    WHERE rule_set_code = %s
                """, (code,))
                for cite, rid in cur.fetchall():
                    citation_to_uuid[cite] = str(rid)
            inserted = 0
            elapsed = 0.0
            rate = 0.0
            print(f"  [{code}] resume: skip ingest (already at {expected} rows); "
                  f"hydrated {expected} citation->uuid mappings", flush=True)
        else:
            ensure_rule_set(conn, code)
            t0 = time.time()
            inserted = 0
            for rec in recs:
                try:
                    rid = ingest_one_rule(conn, rec)
                    citation_to_uuid[rec["canonical_citation"]] = rid
                    inserted += 1
                except psycopg2.errors.CheckViolation as e:
                    conn.rollback()
                    raise HaltError(
                        f"CHECK constraint violation on {rec['canonical_citation']}: {e}\n"
                        f"Row: {json.dumps({k: v for k, v in rec.items() if k != 'full_text'}, default=str)[:1500]}"
                    )
                except Exception as e:
                    conn.rollback()
                    raise HaltError(
                        f"INSERT failed on {rec['canonical_citation']}: {e}\n"
                        f"{traceback.format_exc()}"
                    )
            conn.commit()
            elapsed = time.time() - t0
            rate = inserted / elapsed if elapsed else 0
            print(f"  [{code}] inserted={inserted} expected={expected} elapsed={elapsed:.1f}s rate={rate:.1f}/s",
                  flush=True)

            # Update rule_count
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE wa_court_rules.rule_sets SET rule_count=%s, last_ingested_at=NOW()
                    WHERE rule_set_code=%s
                """, (inserted, code))
            conn.commit()

        # Canaries (between sets in tier)
        canary = canary_table.get(code, {})
        anchors = [canary.get("anchor")] + canary.get("extras", [])
        anchors = [a for a in anchors if a]
        pat_b = canary_pattern_b(conn, anchors)
        pat_b_pass = all(ok for _, ok, _ in pat_b)
        if not pat_b_pass:
            failed = [(a, m) for a, ok, m in pat_b if not ok]
            raise HaltError(f"Pattern (b) FAIL on {code}: {failed}")
        # Pattern (c) FTS — log only
        pat_c_ok, pat_c_msg = canary_pattern_c(conn, code, canary.get("fts", code.lower()))
        # Pattern (a) semantic — log only (Phase 6 §H2)
        pat_a_ok, pat_a_msg = canary_pattern_a(conn, code, canary.get("semantic", ""), canary.get("anchor", ""))

        results["sets"][code] = {
            "inserted": inserted, "expected": expected,
            "elapsed_s": round(elapsed, 1), "rate_per_s": round(rate, 1),
            "pattern_b": [(a, ok, m) for a, ok, m in pat_b],
            "pattern_c": (pat_c_ok, pat_c_msg),
            "pattern_a": (pat_a_ok, pat_a_msg),
        }

    return results


# ── Main ──────────────────────────────────────────────────────────────────────

def preflight(conn) -> dict[str, int]:
    """Resume-aware pre-flight. Returns dict[set_code -> row_count] of
    already-ingested rule sets. Empty dict on clean-state runs.

    Resume mode added 2026-05-01 per Phase 4 boundary recovery (Path B).
    Avoids destructive wipe-and-rerun when a partial ingest already
    committed correct rows.
    """
    print("=== Pre-flight ===", flush=True)
    # Capture already-ingested set codes (resume-aware)
    already_ingested: dict[str, int] = {}
    with conn.cursor() as cur:
        cur.execute("""
            SELECT rule_set_code, COUNT(*) AS row_count
            FROM wa_court_rules.rules
            GROUP BY rule_set_code
        """)
        for set_code, count in cur.fetchall():
            already_ingested[set_code] = count
    if already_ingested:
        print(f"  [resume] already-ingested sets: {already_ingested}", flush=True)
        # Verify each already-ingested set matches expected count exactly
        # (partial ingest is dangerous — halt if mismatched)
        for code, count in already_ingested.items():
            expected = EXPECTED_COUNTS.get(code)
            if expected is None:
                raise HaltError(f"already-ingested set {code} not in EXPECTED_COUNTS")
            if count != expected:
                raise HaltError(
                    f"resume FAIL: {code} has {count} rows, expected {expected}. "
                    f"Partial ingest detected — operator review required."
                )
        print(f"  [ok] all already-ingested sets at expected row count; safe to resume", flush=True)
    else:
        print(f"  [ok] wa_court_rules.rules empty (0); clean-state run", flush=True)

    # 7 CHECK constraints
    expected_checks = {
        "chk_at_most_one_parent_pointer", "chk_comment_consistency",
        "chk_parent_canon_only_cjc", "chk_parent_regulation_handle_only_apr",
        "chk_range_pair", "chk_range_self_consistency",
        "chk_rule_text_status_invariant",
        "chk_repealed_text_meaningful",  # added by extension #2 (2026-05-01)
    }
    with conn.cursor() as cur:
        cur.execute("""
            SELECT conname FROM pg_constraint
            WHERE conrelid='wa_court_rules.rules'::regclass AND contype='c'
        """)
        present = {r[0] for r in cur.fetchall()}
    missing = expected_checks - present
    if missing:
        raise HaltError(f"Missing CHECK constraints on wa_court_rules.rules: {missing}")
    print(f"  [ok] All 8 CHECK constraints present on wa_court_rules.rules", flush=True)

    # chk_cross_ref_jurisdiction allows state-wa-court-rule
    with conn.cursor() as cur:
        cur.execute("""
            SELECT pg_get_constraintdef(oid) FROM pg_constraint
            WHERE conname='chk_cross_ref_jurisdiction'
              AND connamespace='wa_court_rules'::regnamespace
        """)
        defn = cur.fetchone()[0]
    if "'state-wa-court-rule'" not in defn:
        raise HaltError(f"chk_cross_ref_jurisdiction missing state-wa-court-rule: {defn}")
    print(f"  [ok] chk_cross_ref_jurisdiction includes state-wa-court-rule", flush=True)

    # HF endpoint health
    try:
        r = httpx.get(f"{HF_ENDPOINT}/health", timeout=5.0)
        if r.text.strip() != "ok":
            raise HaltError(f"HF .187 /health returned: {r.text[:200]}")
    except Exception as e:
        raise HaltError(f"HF .187 health check failed: {e}")
    print(f"  [ok] HF .187 /health returns 'ok'", flush=True)

    # Test embed
    try:
        v = hf_embed_batch(["preflight test"])
        if len(v[0]) != 1024:
            raise HaltError(f"HF embed dim={len(v[0])}, expected 1024")
    except Exception as e:
        raise HaltError(f"HF .187 test embed failed: {e}")
    print(f"  [ok] HF .187 test embed returned 1024-dim vector", flush=True)

    return already_ingested


def write_cumulative_report(stats: dict[str, Any]) -> None:
    lines: list[str] = []
    lines.append("# WA Court Rules Phase 4 Cumulative Report\n")
    lines.append(f"**Generated:** {dt.datetime.now().isoformat()}\n")
    lines.append(f"**Publication date:** {PUBLICATION_DATE.isoformat()}\n")
    lines.append(f"**Source:** `/opt/wdws/wa_court_rules_research/phase3_parsed.jsonl` "
                 f"(1,145 records)\n")
    lines.append("")

    # Wall time
    if "wall_time_total_s" in stats:
        lines.append(f"**Total wall time:** {stats['wall_time_total_s']:.1f}s\n")

    # Embed stats
    lines.append("## Embedding stats (HF .187)\n")
    lines.append(f"- Calls: {_embed_call_count}")
    lines.append(f"- Total seconds: {_embed_total_seconds:.1f}")
    lines.append(f"- Avg seconds/call: {(_embed_total_seconds / _embed_call_count) if _embed_call_count else 0:.3f}")
    lines.append(f"- Max seconds/call: {_embed_max_seconds:.3f}")
    lines.append("")

    # Per-tier
    for tier_num in (1, 2):
        key = f"tier_{tier_num}"
        if key not in stats:
            continue
        tdata = stats[key]
        lines.append(f"## Tier {tier_num} — Pause-Checkpoint {'A' if tier_num == 1 else 'B'}\n")
        total = sum(s["inserted"] for s in tdata["sets"].values())
        expected = sum(EXPECTED_COUNTS[c] for c in (TIER_1_ORDER if tier_num == 1 else TIER_2_ORDER))
        lines.append(f"**Total inserted:** {total} / {expected} expected\n")

        lines.append("\n| Set | Inserted | Expected | Elapsed (s) | Rate/s | Pat (a) | Pat (b) | Pat (c) |")
        lines.append("|-----|----------|----------|-------------|--------|---------|---------|---------|")
        for code in (TIER_1_ORDER if tier_num == 1 else TIER_2_ORDER):
            s = tdata["sets"].get(code, {})
            pa = s.get("pattern_a", (False, ""))
            pb_all = s.get("pattern_b", [])
            pb_pass = all(ok for _, ok, _ in pb_all)
            pc = s.get("pattern_c", (False, ""))
            lines.append(f"| {code} | {s.get('inserted', 0)} | {EXPECTED_COUNTS[code]} | "
                         f"{s.get('elapsed_s', 0)} | {s.get('rate_per_s', 0)} | "
                         f"{'PASS' if pa[0] else 'log-only'} | "
                         f"{'PASS' if pb_pass else 'FAIL'} | "
                         f"{'PASS' if pc[0] else 'log-only'} |")
        lines.append("")

        # Canary detail per set
        for code in (TIER_1_ORDER if tier_num == 1 else TIER_2_ORDER):
            s = tdata["sets"].get(code, {})
            lines.append(f"### {code} canaries\n")
            lines.append(f"- Pattern (a) semantic: {s.get('pattern_a', ('','N/A'))[1]}")
            for anchor, ok, msg in s.get("pattern_b", []):
                lines.append(f"- Pattern (b) `{anchor}`: {'PASS' if ok else 'FAIL'} — {msg}")
            lines.append(f"- Pattern (c) FTS: {s.get('pattern_c', ('','N/A'))[1]}")
            lines.append("")

    # FK pass 1
    if "range_pass" in stats:
        rp = stats["range_pass"]
        lines.append("## FK Pass 1 — `range_doc_id` resolution\n")
        lines.append(f"- Resolved: {rp['resolved']}")
        lines.append(f"- Unresolved: {len(rp['unresolved'])}")
        if rp["unresolved"]:
            lines.append("- Unresolved children:")
            for child, parent in rp["unresolved"]:
                lines.append(f"  - {child} → {parent}")
        lines.append("")

    # FK pass 2
    if "xref_pass" in stats:
        xp = stats["xref_pass"]
        lines.append("## FK Pass 2 — cross-references\n")
        lines.append(f"- Inserted: {xp['inserted']}")
        lines.append(f"- Skipped self-cites: {xp['skipped_self']}")
        lines.append(f"- In-scope target_rule_id resolved: {xp['resolved_inscope']}")
        lines.append(f"- In-scope target_rule_id unresolved: {xp['unresolved_inscope']}")
        rate = xp["in_scope_resolution_rate"] * 100
        lines.append(f"- In-scope resolution rate: {rate:.1f}%")
        lines.append("")

    # Final corpus check
    if "final" in stats:
        f = stats["final"]
        lines.append("## Final corpus state\n")
        lines.append(f"- wa_court_rules.rules: {f['rules']}")
        lines.append(f"- wa_court_rules.rule_sets: {f['rule_sets']}")
        lines.append(f"- wa_court_rules.cross_references: {f['xrefs']}")
        lines.append(f"- wa_court_rules.rule_history: {f['history']}")
        lines.append(f"- core.document_chunks (court_rule docs): {f['chunks']}")
        lines.append("")

    REPORT_PATH.write_text("\n".join(lines))


def main() -> int:
    print(f"=== WA Court Rules Phase 4 Ingest ===", flush=True)
    print(f"Started: {dt.datetime.now().isoformat()}", flush=True)

    # Load all parsed records
    parsed_records: list[dict] = []
    with PARSED_JSONL.open() as f:
        for line in f:
            line = line.strip()
            if line:
                parsed_records.append(json.loads(line))
    print(f"Loaded {len(parsed_records)} parsed records", flush=True)

    parsed_by_code: dict[str, list[dict]] = {}
    for r in parsed_records:
        parsed_by_code.setdefault(r["rule_set_code"], []).append(r)

    citation_to_uuid: dict[str, str] = {}
    stats: dict[str, Any] = {}
    t_start = time.time()

    conn = psycopg2.connect(DSN)
    conn.autocommit = False
    try:
        already_ingested = preflight(conn)

        # Open ingestion_runs row (skip on resume — prior run already opened one)
        if not already_ingested:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO wa_court_rules.ingestion_runs (run_label, rule_sets_attempted)
                    VALUES (%s, %s) RETURNING id
                """, ("phase-4-full-1145-2026-04-30", TIER_1_ORDER + TIER_2_ORDER))
                run_id = cur.fetchone()[0]
            conn.commit()
        else:
            # Resume run — open a continuation row
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO wa_court_rules.ingestion_runs (run_label, rule_sets_attempted)
                    VALUES (%s, %s) RETURNING id
                """, ("phase-4-resume-2026-05-01", TIER_1_ORDER + TIER_2_ORDER))
                run_id = cur.fetchone()[0]
            conn.commit()
            print(f"  [resume] opened continuation ingestion_runs row", flush=True)

        # Tier 1
        print(f"\n=== Tier 1 ({sum(EXPECTED_COUNTS[c] for c in TIER_1_ORDER)} rules) ===", flush=True)
        stats["tier_1"] = run_tier(conn, 1, TIER_1_ORDER, parsed_by_code, citation_to_uuid, already_ingested)

        # Pause-Checkpoint A (continuing per operator end-to-end auth)
        print(f"\n[Pause-Checkpoint A] Tier 1 complete; proceeding to Tier 2", flush=True)

        # Tier 2
        print(f"\n=== Tier 2 ({sum(EXPECTED_COUNTS[c] for c in TIER_2_ORDER)} rules) ===", flush=True)
        stats["tier_2"] = run_tier(conn, 2, TIER_2_ORDER, parsed_by_code, citation_to_uuid, already_ingested)

        # CRLJ confirmation surface
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM wa_court_rules.rules
                WHERE rule_set_code='CRLJ' AND canonical_citation LIKE 'CRLJ %.04'
            """)
            crlj_04_count = cur.fetchone()[0]
        print(f"  [CRLJ] confirmed {crlj_04_count} .04 children present", flush=True)

        # Wall time check on embeds
        if _embed_total_seconds > 360:  # >6 min
            raise HaltError(f"Embedding wall time {_embed_total_seconds:.1f}s exceeds 6min budget")

        # Pass 1: range_doc_id
        print(f"\n=== FK Pass 1 — range_doc_id ===", flush=True)
        range_result = resolve_range_doc_ids(conn, citation_to_uuid, parsed_records)
        conn.commit()
        stats["range_pass"] = range_result
        print(f"  resolved={range_result['resolved']} unresolved={len(range_result['unresolved'])}",
              flush=True)
        if range_result["unresolved"]:
            raise HaltError(
                f"Pass 1: {len(range_result['unresolved'])} unresolved range children: "
                f"{range_result['unresolved'][:5]}"
            )

        # Pass 2: cross_references
        print(f"\n=== FK Pass 2 — cross_references ===", flush=True)
        xref_result = insert_cross_references(conn, citation_to_uuid, parsed_records)
        conn.commit()
        stats["xref_pass"] = xref_result
        print(f"  inserted={xref_result['inserted']} self-skipped={xref_result['skipped_self']} "
              f"inscope_resolved={xref_result['resolved_inscope']} "
              f"inscope_unresolved={xref_result['unresolved_inscope']} "
              f"rate={xref_result['in_scope_resolution_rate']*100:.1f}%", flush=True)
        if xref_result["in_scope_resolution_rate"] < 0.70:
            raise HaltError(
                f"In-scope cross-ref resolution rate {xref_result['in_scope_resolution_rate']*100:.1f}% "
                f"< 70% threshold"
            )

        # Final corpus snapshot
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM wa_court_rules.rules")
            n_rules = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM wa_court_rules.rule_sets")
            n_sets = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM wa_court_rules.cross_references")
            n_xref = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM wa_court_rules.rule_history")
            n_hist = cur.fetchone()[0]
            cur.execute("""
                SELECT COUNT(*) FROM core.document_chunks c
                JOIN core.documents d ON d.id=c.document_id
                WHERE d.document_type='court_rule'
            """)
            n_chunks = cur.fetchone()[0]
        stats["final"] = {
            "rules": n_rules, "rule_sets": n_sets, "xrefs": n_xref,
            "history": n_hist, "chunks": n_chunks,
        }

        # Close ingestion_runs
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE wa_court_rules.ingestion_runs
                SET completed_at=NOW(), rules_inserted=%s
                WHERE id=%s
            """, (n_rules, run_id))
        conn.commit()

        stats["wall_time_total_s"] = time.time() - t_start
        write_cumulative_report(stats)
        print(f"\n=== Phase 4 complete ===", flush=True)
        print(f"  rules={n_rules} rule_sets={n_sets} xrefs={n_xref} history={n_hist} chunks={n_chunks}",
              flush=True)
        print(f"  total wall time: {stats['wall_time_total_s']:.1f}s", flush=True)
        print(f"  report: {REPORT_PATH}", flush=True)
        return 0

    except HaltError as e:
        conn.rollback()
        write_halt("HaltError", str(e))
        # Still write a partial report
        try:
            stats["wall_time_total_s"] = time.time() - t_start
            write_cumulative_report(stats)
        except Exception:
            pass
        return 2
    except Exception as e:
        conn.rollback()
        write_halt("Unexpected exception", f"{e}\n{traceback.format_exc()}")
        return 3
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
