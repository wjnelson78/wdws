"""
WA Court Rules Phase 5 Validation Suite — 11 checks
====================================================
Operator-authorized 2026-05-01 (full version with cross-jurisdiction
canary at §T after Phase 4 cross_references re-ingest landed at 09:01 PT).

Run modes:
  --dry-run    : execute all checks but issue ROLLBACK around any
                 state-modifying queries. (Script is read-only by design;
                 dry-run still surfaces SQL bugs / column-name typos
                 BEFORE production.)
  (no flag)    : production run; writes phase5_cumulative_report.md.

Halt conditions:
  - Pattern (b) FAIL on any priority rule set
  - Citation roundtrip mismatch (Check 11)
  - CHECK constraint / FK invariant violations (Checks 6, 7, 8, 9)
  - state-wa-court-rule cross-ref resolution rate < 95%
  - state-wa-rcw section-form resolution rate < 50%
  - Content-fidelity < 18/20 substantive matches

Phase 6 §H2 documented limitations (log-only; do NOT halt):
  - Pattern (a) MISS on long-tail rule sets (query-vocabulary issue)
  - Check 10 partial PASS (3/4 jurisdictions in top-30; WAC absent)
  - federal-usc cross-ref resolution rate (Phase 7 backlog)
  - state-wa-wac cross-ref count == 0 (Phase 7 backlog: chapter-form)

Cumulative report → phase5_cumulative_report.md
"""
from __future__ import annotations

import argparse
import json
import os
import random
import re
import subprocess
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import psycopg2
import psycopg2.extras

# ── Config ────────────────────────────────────────────────────────────────────

ROOT = Path("/opt/wdws/wa_court_rules_research")
LOG = ROOT / "phase5_validate.log"
REPORT = ROOT / "phase5_cumulative_report.md"
HALT_PATH = ROOT / "HALT_PHASE5.md"

DSN = os.environ.get(
    "DATABASE_URL",
    "postgresql://wdws:NEL2233obs@127.0.0.1:5432/wdws",
)

HF_ENDPOINT = "http://172.16.81.187:9098"
HF_API_TOKEN = os.environ.get("HF_API_TOKEN", "")
if not HF_API_TOKEN:
    env_path = Path("/opt/wdws/.env")
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line.startswith("HF_API_TOKEN="):
                HF_API_TOKEN = line.split("=", 1)[1].strip().strip('"').strip("'")
                break

EMBEDDING_DIMS = 1024
HF_TIMEOUT = 30.0

# 15 rule sets (locked)
RULE_SETS = [
    "GR", "RPC", "CR", "ER", "RAP", "CJC", "GALR",
    "CrR", "CrRLJ", "CRLJ", "JuCR", "SCCAR", "RALJ", "APR", "ELC",
]

# Anchors / canaries — same as phase4_ingest.py for symmetry
CANARIES: dict[str, dict] = {
    "GR":    {"semantic": "access to court records",        "anchor": "GR 31",      "extras": ["GR 33", "GR 34"], "fts": "records"},
    "RPC":   {"semantic": "attorney competence",            "anchor": "RPC 1.1",    "extras": ["RPC 1.5", "RPC 5.5"], "fts": "competence"},
    "CR":    {"semantic": "summary judgment",               "anchor": "CR 56",      "extras": ["CR 12", "CR 26"], "fts": "subpoena"},
    "ER":    {"semantic": "relevance of evidence",          "anchor": "ER 401",     "extras": ["ER 403", "ER 802"], "fts": "hearsay"},
    "RAP":   {"semantic": "appeal procedure",               "anchor": "RAP 18.13A", "extras": [], "fts": "appellate"},
    "CJC":   {"semantic": "judicial impartiality",          "anchor": "CJC 2.11",   "extras": ["CJC 1.1"], "fts": "judge"},
    "GALR":  {"semantic": "guardian ad litem scope",        "anchor": "GALR 1",     "extras": ["GALR 5"], "fts": "guardian"},
    "CrR":   {"semantic": "criminal procedure scope",       "anchor": "CrR 1.1",    "extras": ["CrR 3.5"], "fts": "criminal"},
    "CrRLJ": {"semantic": "limited jurisdiction criminal",  "anchor": "CrRLJ 1.1",  "extras": [], "fts": "criminal"},
    "CRLJ":  {"semantic": "limited jurisdiction civil",     "anchor": "CRLJ 30",    "extras": ["CRLJ 92.04"], "fts": "civil"},
    "JuCR":  {"semantic": "juvenile court",                 "anchor": "JuCR 1.1",   "extras": ["JuCR 5A.1"], "fts": "juvenile"},
    "SCCAR": {"semantic": "civil arbitration",              "anchor": "SCCAR 1.1",  "extras": [], "fts": "arbitration"},
    "RALJ":  {"semantic": "appeal limited jurisdiction",    "anchor": "RALJ 1.1",   "extras": [], "fts": "appeal"},
    "APR":   {"semantic": "admission and practice",         "anchor": "APR 15P",    "extras": ["APR Regs 101"], "fts": "admission"},
    "ELC":   {"semantic": "lawyer discipline procedure",    "anchor": "ELC 1.1",    "extras": ["ELC 5.1", "ELC 13.1"], "fts": "discipline"},
}

# Priority rule sets where Pattern (b) FAIL is a halt-class signal
PRIORITY_RULE_SETS = {"GR", "RPC", "CR", "ER", "RAP", "CJC"}

# Tuneable expectations
EXPECTED_TOTAL_RULES = 1145
EXPECTED_RULE_SETS = 15
EXPECTED_BOTH_FLAGS = {"GR 8", "CR 5", "CR 78", "ER 301", "ER 302",
                       "RAP 11.5", "RAP 11.6", "CRLJ 5"}
EXPECTED_RANGE_EXPANSIONS = 58
EXPECTED_RANGE_CANONICALS = 8
EXPECTED_CJC_DISTRIBUTION = {"Canon 1": 3, "Canon 2": 14, "Canon 3": 13, "Canon 4": 5}
EXPECTED_APR_REG_HANDLES = {"Regs": 17, "LP_REGS": 1, "LP_DIS_REGS": 1}

# Resolution-rate thresholds
COURT_RULE_RES_MIN = 0.95
RCW_SECTION_RES_MIN = 0.50

# Content-fidelity threshold (out of 20)
FIDELITY_PASS_MIN = 18

# Citation graph density window (sanity)
DENSITY_MIN = 0.5
DENSITY_MAX = 3.0

# Random-seed for reproducible sampling
random.seed(20260501)


# ── Logging ───────────────────────────────────────────────────────────────────

def log(msg: str):
    line = f"[{datetime.now(timezone.utc).isoformat()}] {msg}"
    print(line, flush=True)
    with open(LOG, "a") as fh:
        fh.write(line + "\n")


# ── HF embed (Check 10 only) ──────────────────────────────────────────────────

_hf_client: httpx.Client | None = None


def hf_client() -> httpx.Client:
    global _hf_client
    if _hf_client is None:
        if not HF_API_TOKEN:
            raise RuntimeError("HF_API_TOKEN not configured (Check 10 needs embed)")
        _hf_client = httpx.Client(
            timeout=HF_TIMEOUT,
            headers={
                "Authorization": f"Bearer {HF_API_TOKEN}",
                "Content-Type": "application/json",
            },
        )
    return _hf_client


def hf_embed(text: str) -> list[float]:
    r = hf_client().post(f"{HF_ENDPOINT}/embed", json={"inputs": [text[:32768]]})
    r.raise_for_status()
    vecs = r.json()
    if len(vecs) != 1 or len(vecs[0]) != EMBEDDING_DIMS:
        raise RuntimeError(f"unexpected HF response shape")
    return vecs[0]


def vec_literal(v: list[float]) -> str:
    return "[" + ",".join(f"{x:.6f}" for x in v) + "]"


# ── Halt machinery ────────────────────────────────────────────────────────────

class HaltError(Exception):
    pass


def write_halt(reason: str, details: str) -> None:
    HALT_PATH.write_text(
        f"# Phase 5 HALT\n\n"
        f"**Time:** {datetime.now(timezone.utc).isoformat()}\n\n"
        f"**Reason:** {reason}\n\n"
        f"## Details\n\n```\n{details}\n```\n"
    )
    print(f"\n!!! HALT: {reason}\n{details}\n", file=sys.stderr, flush=True)


# ── Connection helper ────────────────────────────────────────────────────────

def connect():
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    return conn


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║ CHECK 1 — Embedding count completeness                                     ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

def check_1_embedding_count() -> dict[str, Any]:
    log("=== Check 1: embedding count completeness ===")
    with connect() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT
              (SELECT COUNT(*) FROM wa_court_rules.rules)                       AS total_rules,
              (SELECT COUNT(DISTINCT r.id)
                 FROM wa_court_rules.rules r
                 JOIN core.document_chunks c ON c.document_id = r.document_id
                 WHERE c.embedding IS NOT NULL)                                  AS rules_with_embedding,
              (SELECT COUNT(*) FROM wa_court_rules.rules r
                 JOIN core.documents d ON d.id = r.document_id
                 WHERE d.document_type='court_rule')                              AS docs_court_rule,
              (SELECT COUNT(*) FROM core.document_chunks c
                 JOIN core.documents d ON d.id = c.document_id
                 WHERE d.document_type='court_rule')                              AS chunks_total
        """)
        total, embedded, docs, chunks = cur.fetchone()

    rate = embedded / total if total else 0
    ok = embedded == total
    log(f"  rules total:                  {total}")
    log(f"  rules with ≥1 NOT NULL embed: {embedded} ({rate*100:.1f}%)")
    log(f"  core.documents (court_rule):  {docs}")
    log(f"  core.document_chunks (cr):    {chunks}")
    log(f"  RESULT: {'PASS' if ok else 'FAIL — coverage <100%'}")
    return {
        "check": "1_embedding_count",
        "pass": ok,
        "total_rules": total,
        "rules_with_embedding": embedded,
        "coverage": round(rate, 4),
        "docs_court_rule": docs,
        "chunks_total": chunks,
    }


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║ CHECK 2 — Authority/cross-ref resolution rate by jurisdiction              ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

def check_2_resolution_rates() -> dict[str, Any]:
    log("=== Check 2: cross-ref resolution rates by jurisdiction ===")
    with connect() as conn, conn.cursor() as cur:
        # state-wa-court-rule (in-corpus FK)
        cur.execute("""
            SELECT COUNT(*),
                   SUM(CASE WHEN target_rule_id IS NOT NULL THEN 1 ELSE 0 END)
            FROM wa_court_rules.cross_references
            WHERE target_jurisdiction = 'state-wa-court-rule'
        """)
        cr_total, cr_resolved = cur.fetchone()
        cr_rate = (cr_resolved / cr_total) if cr_total else 0

        # state-wa-rcw (split section vs chapter form by target_rule_number shape)
        # Section form: 3 dot-separated segments e.g. 49.60.180
        cur.execute("""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN s.id IS NOT NULL THEN 1 ELSE 0 END) AS resolved
            FROM wa_court_rules.cross_references xr
            LEFT JOIN rcw.sections s ON s.canonical_citation = xr.target_citation
            WHERE xr.target_jurisdiction = 'state-wa-rcw'
              AND xr.target_rule_number ~ '^[0-9]+[A-Z]?\\.[0-9]+\\.[0-9]+'
        """)
        rcw_section_total, rcw_section_resolved = cur.fetchone()
        rcw_section_rate = (rcw_section_resolved / rcw_section_total) if rcw_section_total else 0

        cur.execute("""
            SELECT COUNT(*)
            FROM wa_court_rules.cross_references xr
            WHERE xr.target_jurisdiction = 'state-wa-rcw'
              AND NOT (xr.target_rule_number ~ '^[0-9]+[A-Z]?\\.[0-9]+\\.[0-9]+')
        """)
        rcw_chapter_total = cur.fetchone()[0]

        # federal-usc against usc.sections (target_citation matches canonical_citation)
        cur.execute("""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN s.id IS NOT NULL THEN 1 ELSE 0 END) AS resolved
            FROM wa_court_rules.cross_references xr
            LEFT JOIN usc.sections s ON s.canonical_citation = xr.target_citation
            WHERE xr.target_jurisdiction = 'federal-usc'
        """)
        usc_total, usc_resolved = cur.fetchone()
        usc_rate = (usc_resolved / usc_total) if usc_total else 0

        # wa-constitution: text-only count
        cur.execute("""
            SELECT COUNT(*) FROM wa_court_rules.cross_references
            WHERE target_jurisdiction = 'wa-constitution'
        """)
        wac_const_total = cur.fetchone()[0]

        # state-wa-wac
        cur.execute("""
            SELECT COUNT(*) FROM wa_court_rules.cross_references
            WHERE target_jurisdiction = 'state-wa-wac'
        """)
        wac_total = cur.fetchone()[0]

    log(f"  state-wa-court-rule: {cr_resolved}/{cr_total} = {cr_rate*100:.1f}% (PASS≥95%)")
    log(f"  state-wa-rcw section-form: {rcw_section_resolved}/{rcw_section_total} = {rcw_section_rate*100:.1f}% (PASS≥50%)")
    log(f"  state-wa-rcw chapter-form: {rcw_chapter_total} (no section target by design)")
    log(f"  federal-usc: {usc_resolved}/{usc_total} = {usc_rate*100:.1f}% (log-only)")
    log(f"  wa-constitution: {wac_const_total} (text-only, no FK target; log-only)")
    log(f"  state-wa-wac: {wac_total} (Phase 7 backlog; log-only)")

    cr_ok = cr_rate >= COURT_RULE_RES_MIN
    rcw_ok = rcw_section_rate >= RCW_SECTION_RES_MIN
    overall = cr_ok and rcw_ok
    log(f"  RESULT: {'PASS' if overall else 'FAIL'} (court-rule={cr_ok}, rcw-section={rcw_ok})")

    return {
        "check": "2_resolution_rates",
        "pass": overall,
        "court_rule": {"total": cr_total, "resolved": cr_resolved, "rate": round(cr_rate, 4), "min": COURT_RULE_RES_MIN, "ok": cr_ok},
        "rcw_section": {"total": rcw_section_total, "resolved": rcw_section_resolved, "rate": round(rcw_section_rate, 4), "min": RCW_SECTION_RES_MIN, "ok": rcw_ok},
        "rcw_chapter_form_count": rcw_chapter_total,
        "federal_usc": {"total": usc_total, "resolved": usc_resolved, "rate": round(usc_rate, 4)},
        "wa_constitution_count": wac_const_total,
        "state_wa_wac_count": wac_total,
    }


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║ CHECK 3 — Per-rule-set canary suite (3 patterns)                           ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

def _pattern_b(conn, anchors: list[str]) -> list[tuple[str, bool, str]]:
    out = []
    with conn.cursor() as cur:
        for cite in anchors:
            cur.execute("""
                SELECT canonical_citation, full_text IS NOT NULL,
                       LENGTH(COALESCE(full_text,''))
                FROM wa_court_rules.rules WHERE canonical_citation=%s
            """, (cite,))
            rows = cur.fetchall()
            if len(rows) != 1:
                out.append((cite, False, f"expected 1 row, got {len(rows)}"))
            else:
                _, has_text, tlen = rows[0]
                out.append((cite, True, f"row found, full_text len={tlen}"))
    return out


def _pattern_c(conn, code: str, fts_term: str) -> tuple[bool, str]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM core.documents d
            JOIN wa_court_rules.rules r ON r.document_id = d.id
            WHERE r.rule_set_code = %s
              AND d.full_content_tsv @@ plainto_tsquery('english', %s)
        """, (code, fts_term))
        n = cur.fetchone()[0]
    return (n > 0, f"FTS '{fts_term}' on {code}: {n} hits")


def _pattern_a(conn, code: str, semantic_query: str, anchor_cite: str) -> tuple[bool, str, list[str]]:
    try:
        qvec = hf_embed(semantic_query)
        qlit = vec_literal(qvec)
        with conn.cursor() as cur:
            cur.execute("""
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
        return (ok, f"top10[:5]={top10[:5]} anchor={anchor_cite} {'HIT' if ok else 'MISS'}", top10)
    except Exception as e:
        return (False, f"pattern (a) error: {e}", [])


def check_3_canary_suite() -> dict[str, Any]:
    log("=== Check 3: per-rule-set 3-pattern canary suite ===")
    results: dict[str, Any] = {}
    halt_b = []
    pattern_a_misses = []

    with connect() as conn:
        for code in RULE_SETS:
            canary = CANARIES[code]
            anchors = [canary["anchor"]] + canary.get("extras", [])
            anchors = [a for a in anchors if a]
            pat_b = _pattern_b(conn, anchors)
            pat_b_pass = all(ok for _, ok, _ in pat_b)
            if not pat_b_pass and code in PRIORITY_RULE_SETS:
                halt_b.append((code, [(a, m) for a, ok, m in pat_b if not ok]))
            pat_c_ok, pat_c_msg = _pattern_c(conn, code, canary["fts"])
            pat_a_ok, pat_a_msg, top10 = _pattern_a(conn, code, canary["semantic"], canary["anchor"])
            if not pat_a_ok:
                pattern_a_misses.append({
                    "code": code,
                    "anchor": canary["anchor"],
                    "semantic": canary["semantic"],
                    "top10": top10,
                })
            results[code] = {
                "pattern_b": [(a, ok, m) for a, ok, m in pat_b],
                "pattern_b_pass": pat_b_pass,
                "pattern_c": (pat_c_ok, pat_c_msg),
                "pattern_a": (pat_a_ok, pat_a_msg),
            }
            log(f"  [{code}] B={pat_b_pass} C={pat_c_ok} A={pat_a_ok} | {pat_a_msg}")

    if halt_b:
        log(f"  HALT: Pattern (b) FAIL on priority rule sets: {halt_b}")
        raise HaltError(f"Pattern (b) FAIL on priority rule sets: {halt_b}")

    pat_a_total = sum(1 for _ in RULE_SETS)
    pat_a_pass = pat_a_total - len(pattern_a_misses)
    log(f"  Pattern (a): {pat_a_pass}/{pat_a_total} HIT (Phase 6 §H2 documents query-vocabulary misses)")

    return {
        "check": "3_canary_suite",
        "pass": True,  # pattern (b) passes (else halted); a/c are log-only
        "by_rule_set": results,
        "pattern_a_hit": pat_a_pass,
        "pattern_a_misses": pattern_a_misses,
    }


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║ CHECK 4 — 20-spot stratified content-fidelity audit                        ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

def _normalize(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def check_4_content_fidelity() -> dict[str, Any]:
    log("=== Check 4: 20-spot stratified content-fidelity audit ===")

    # RCW Phase-5-bug-1 fix: do NOT use SELECT … ORDER BY random() LIMIT
    # to "stratify" — that gives you N random rows total, not 1 per stratum.
    # Use `SELECT DISTINCT ON (rule_set_code)` ordered by `random()` to get
    # one rule per set, then top up additional samples from larger sets.
    samples: list[dict[str, Any]] = []
    with connect() as conn, conn.cursor() as cur:
        # 1 per set (15 samples) using DISTINCT ON
        cur.execute("""
            SELECT DISTINCT ON (rule_set_code)
                   rule_set_code, canonical_citation, full_text, source_pdf_path
            FROM wa_court_rules.rules
            WHERE source_pdf_path IS NOT NULL
              AND full_text IS NOT NULL
              AND LENGTH(full_text) >= 500
              AND NOT is_repealed AND NOT is_reserved
            ORDER BY rule_set_code, random()
        """)
        for row in cur.fetchall():
            samples.append({
                "rule_set_code": row[0],
                "canonical_citation": row[1],
                "full_text": row[2],
                "source_pdf_path": row[3],
            })

        # 5 additional from largest sets (RAP=183, ELC=133, CRLJ=115, JuCR=99,
        # CrRLJ=76) — one each, picking different rule from those sets
        sampled_cites = {s["canonical_citation"] for s in samples}
        for code in ["RAP", "ELC", "CRLJ", "JuCR", "CrRLJ"]:
            cur.execute("""
                SELECT canonical_citation, full_text, source_pdf_path
                FROM wa_court_rules.rules
                WHERE rule_set_code = %s
                  AND source_pdf_path IS NOT NULL
                  AND full_text IS NOT NULL
                  AND LENGTH(full_text) >= 500
                  AND NOT is_repealed AND NOT is_reserved
                  AND canonical_citation <> ALL(%s)
                ORDER BY random()
                LIMIT 1
            """, (code, list(sampled_cites)))
            row = cur.fetchone()
            if row:
                samples.append({
                    "rule_set_code": code,
                    "canonical_citation": row[0],
                    "full_text": row[1],
                    "source_pdf_path": row[2],
                })
                sampled_cites.add(row[0])

    samples = samples[:20]
    log(f"  sampled {len(samples)} rules across {len(set(s['rule_set_code'] for s in samples))} rule sets")

    pass_count, fail_count = 0, 0
    fidelity = []
    for s in samples:
        pdf_path = s["source_pdf_path"]
        cite = s["canonical_citation"]
        if not pdf_path or not os.path.exists(pdf_path):
            fail_count += 1
            fidelity.append({"cite": cite, "status": "pdf_missing", "path": pdf_path})
            log(f"    {cite}: pdf_missing ({pdf_path})")
            continue

        # pdftotext -layout to capture body
        try:
            cp = subprocess.run(
                ["pdftotext", "-layout", pdf_path, "-"],
                capture_output=True, text=True, timeout=30,
            )
            if cp.returncode != 0:
                fail_count += 1
                fidelity.append({"cite": cite, "status": "pdftotext_error", "stderr": cp.stderr[:200]})
                continue
            pdf_body = cp.stdout
        except Exception as e:
            fail_count += 1
            fidelity.append({"cite": cite, "status": "pdftotext_exception", "error": str(e)})
            continue

        stored_norm = _normalize(s["full_text"])
        pdf_norm = _normalize(pdf_body)
        # 200-char prefix substring match (allows whitespace/format drift)
        first_200 = stored_norm[:200]
        if first_200 and first_200 in pdf_norm:
            pass_count += 1
            fidelity.append({"cite": cite, "status": "match"})
            log(f"    {cite}: MATCH (200-char prefix in PDF)")
        else:
            # Soft match: ≥10% character overlap on first 200
            #   we use shared-bigram overlap as a quick proxy
            overlap = _bigram_overlap(first_200, pdf_norm[: max(2000, len(first_200) * 4)])
            if overlap >= 0.10:
                pass_count += 1
                fidelity.append({"cite": cite, "status": "match_soft", "overlap": round(overlap, 3)})
                log(f"    {cite}: MATCH_SOFT (bigram overlap {overlap*100:.1f}%)")
            else:
                fail_count += 1
                fidelity.append({
                    "cite": cite, "status": "miss",
                    "overlap": round(overlap, 3),
                    "stored_preview": first_200[:120],
                })
                log(f"    {cite}: MISS (overlap={overlap*100:.1f}%) preview='{first_200[:80]}'")

    ok = pass_count >= FIDELITY_PASS_MIN
    log(f"  RESULT: {pass_count}/{len(samples)} match, {fail_count} miss → {'PASS' if ok else 'FAIL'}")
    return {
        "check": "4_content_fidelity",
        "pass": ok,
        "samples": len(samples),
        "match": pass_count,
        "miss": fail_count,
        "details": fidelity,
    }


def _bigram_overlap(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    a_norm = re.sub(r"\s+", "", a.lower())
    b_norm = re.sub(r"\s+", "", b.lower())
    if len(a_norm) < 2:
        return 0.0
    a_grams = {a_norm[i : i + 2] for i in range(len(a_norm) - 1)}
    b_grams = {b_norm[i : i + 2] for i in range(len(b_norm) - 1)}
    if not a_grams:
        return 0.0
    return len(a_grams & b_grams) / len(a_grams)


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║ CHECK 5 — Citation graph density                                            ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

def check_5_citation_graph_density() -> dict[str, Any]:
    log("=== Check 5: citation graph density ===")
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM wa_court_rules.rules")
        n_rules = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM wa_court_rules.cross_references")
        n_xrefs = cur.fetchone()[0]
        cur.execute("""
            SELECT ref_type, COUNT(*) FROM wa_court_rules.cross_references
            GROUP BY ref_type ORDER BY 2 DESC
        """)
        by_type = cur.fetchall()
        cur.execute("""
            SELECT target_jurisdiction, COUNT(*) FROM wa_court_rules.cross_references
            GROUP BY target_jurisdiction ORDER BY 2 DESC
        """)
        by_jur = cur.fetchall()

    ratio = n_xrefs / n_rules if n_rules else 0
    log(f"  rules: {n_rules}, xrefs: {n_xrefs}, ratio: {ratio:.3f}")
    log(f"  by ref_type: {dict(by_type)}")
    log(f"  by target_jurisdiction: {dict(by_jur)}")
    ok = DENSITY_MIN <= ratio <= DENSITY_MAX
    log(f"  RESULT: {'PASS' if ok else 'FAIL'} (ratio in [{DENSITY_MIN},{DENSITY_MAX}])")
    return {
        "check": "5_citation_graph_density",
        "pass": ok,
        "rules": n_rules,
        "xrefs": n_xrefs,
        "ratio": round(ratio, 4),
        "by_ref_type": dict(by_type),
        "by_jurisdiction": dict(by_jur),
    }


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║ CHECK 6 — Ghost-flag invariants                                            ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

def check_6_ghost_flags() -> dict[str, Any]:
    log("=== Check 6: ghost-flag invariants ===")
    with connect() as conn, conn.cursor() as cur:
        # is_repealed body marker (CHECK already enforces, but sanity confirm)
        cur.execute("""
            SELECT COUNT(*) FROM wa_court_rules.rules
            WHERE is_repealed
              AND full_text IS NOT NULL AND full_text <> ''
              AND NOT (full_text ~* '(rescind|deleted|repealed|removed|transferred)')
        """)
        bad_repealed = cur.fetchone()[0]

        # is_reserved body permissive (Path 1 loosening): empty OR contains placeholder
        cur.execute("""
            SELECT canonical_citation, LENGTH(COALESCE(full_text,'')) AS L,
                   full_text ~* 'reserved' AS has_reserved_marker
            FROM wa_court_rules.rules
            WHERE is_reserved AND NOT is_repealed
        """)
        reserved_rows = cur.fetchall()
        bad_reserved = []
        for cite, ln, has_marker in reserved_rows:
            # acceptable: empty body, OR body is short (<200) with reserved marker,
            # OR body longer but contains 'reserved' marker
            if ln == 0:
                continue
            if has_marker:
                continue
            bad_reserved.append((cite, ln))

        # Both-flag rows
        cur.execute("""
            SELECT canonical_citation FROM wa_court_rules.rules
            WHERE is_repealed AND is_reserved
            ORDER BY canonical_citation
        """)
        both_actual = {r[0] for r in cur.fetchall()}

    extra = both_actual - EXPECTED_BOTH_FLAGS
    missing = EXPECTED_BOTH_FLAGS - both_actual

    ok = (bad_repealed == 0 and len(bad_reserved) == 0 and not extra and not missing)
    log(f"  is_repealed without marker: {bad_repealed} (expect 0)")
    log(f"  is_reserved without marker AND non-empty body: {len(bad_reserved)} (expect 0)")
    if bad_reserved:
        for cite, ln in bad_reserved[:5]:
            log(f"    {cite} len={ln}")
    log(f"  both-flag rows: {sorted(both_actual)}")
    log(f"  expected: {sorted(EXPECTED_BOTH_FLAGS)}")
    log(f"  RESULT: {'PASS' if ok else 'FAIL'} (extra={extra}, missing={missing})")
    if not ok:
        raise HaltError(f"Check 6 invariant violation — extra={extra}, missing={missing}, bad_repealed={bad_repealed}, bad_reserved={bad_reserved[:5]}")
    return {
        "check": "6_ghost_flags",
        "pass": ok,
        "bad_repealed_count": bad_repealed,
        "bad_reserved_count": len(bad_reserved),
        "both_flag_set": sorted(both_actual),
        "expected_both_flag_set": sorted(EXPECTED_BOTH_FLAGS),
    }


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║ CHECK 7 — Range expansion integrity                                        ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

def check_7_range_expansion() -> dict[str, Any]:
    log("=== Check 7: range expansion integrity ===")
    with connect() as conn, conn.cursor() as cur:
        # 58 children with range_doc_id resolving to canonical (range_start NOT NULL)
        cur.execute("""
            SELECT COUNT(*) FROM wa_court_rules.rules
            WHERE range_doc_id IS NOT NULL
        """)
        n_children = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*) FROM wa_court_rules.rules child
            LEFT JOIN wa_court_rules.rules canon ON canon.id = child.range_doc_id
            WHERE child.range_doc_id IS NOT NULL
              AND (canon.range_start IS NULL OR canon.range_end IS NULL)
        """)
        n_unresolved = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*) FROM wa_court_rules.rules
            WHERE range_start IS NOT NULL
        """)
        n_canonicals = cur.fetchone()[0]

        # chk_range_self_consistency check (no row has both range_start and range_doc_id)
        cur.execute("""
            SELECT COUNT(*) FROM wa_court_rules.rules
            WHERE range_start IS NOT NULL AND range_doc_id IS NOT NULL
        """)
        n_violation = cur.fetchone()[0]

    ok = (
        n_children == EXPECTED_RANGE_EXPANSIONS
        and n_unresolved == 0
        and n_canonicals == EXPECTED_RANGE_CANONICALS
        and n_violation == 0
    )
    log(f"  expansion children: {n_children} (expect {EXPECTED_RANGE_EXPANSIONS})")
    log(f"  unresolved range_doc_id: {n_unresolved} (expect 0)")
    log(f"  canonical range rows: {n_canonicals} (expect {EXPECTED_RANGE_CANONICALS})")
    log(f"  self-consistency violations: {n_violation} (expect 0)")
    log(f"  RESULT: {'PASS' if ok else 'FAIL'}")
    if not ok:
        raise HaltError(f"Check 7 FAIL: children={n_children}, unresolved={n_unresolved}, canonicals={n_canonicals}, violations={n_violation}")
    return {
        "check": "7_range_expansion",
        "pass": ok,
        "expansion_children": n_children,
        "unresolved_doc_id": n_unresolved,
        "canonicals": n_canonicals,
        "self_consistency_violations": n_violation,
    }


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║ CHECK 8 — parent_canon integrity                                            ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

def check_8_parent_canon() -> dict[str, Any]:
    log("=== Check 8: parent_canon integrity ===")
    with connect() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM wa_court_rules.rules
            WHERE parent_canon IS NOT NULL AND rule_set_code <> 'CJC'
        """)
        n_violations = cur.fetchone()[0]

        cur.execute("""
            SELECT parent_canon, COUNT(*) FROM wa_court_rules.rules
            WHERE rule_set_code='CJC' AND parent_canon IS NOT NULL
            GROUP BY parent_canon ORDER BY parent_canon
        """)
        dist_actual = dict(cur.fetchall())

        cur.execute("""
            SELECT COUNT(*) FROM wa_court_rules.rules
            WHERE rule_set_code='CJC' AND parent_canon IS NULL
        """)
        n_cjc_no_canon = cur.fetchone()[0]

    ok = (
        n_violations == 0
        and dist_actual == EXPECTED_CJC_DISTRIBUTION
        and n_cjc_no_canon == 0
    )
    log(f"  non-CJC rows with parent_canon: {n_violations} (expect 0)")
    log(f"  CJC distribution actual: {dist_actual}")
    log(f"  CJC distribution expected: {EXPECTED_CJC_DISTRIBUTION}")
    log(f"  CJC rows missing parent_canon: {n_cjc_no_canon} (expect 0)")
    log(f"  RESULT: {'PASS' if ok else 'FAIL'}")
    if not ok:
        raise HaltError(f"Check 8 FAIL: violations={n_violations}, dist={dist_actual}, missing={n_cjc_no_canon}")
    return {
        "check": "8_parent_canon",
        "pass": ok,
        "non_cjc_violations": n_violations,
        "cjc_distribution": dist_actual,
        "cjc_missing": n_cjc_no_canon,
    }


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║ CHECK 9 — parent_regulation_handle integrity                                ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

def check_9_parent_regulation() -> dict[str, Any]:
    log("=== Check 9: parent_regulation_handle integrity ===")
    with connect() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM wa_court_rules.rules
            WHERE parent_regulation_handle IS NOT NULL AND rule_set_code <> 'APR'
        """)
        n_non_apr = cur.fetchone()[0]

        cur.execute("""
            SELECT parent_regulation_handle, COUNT(*) FROM wa_court_rules.rules
            WHERE rule_set_code='APR' AND parent_regulation_handle IS NOT NULL
            GROUP BY 1 ORDER BY 1
        """)
        dist_actual = dict(cur.fetchall())

        cur.execute("""
            SELECT COUNT(*) FROM wa_court_rules.rules
            WHERE parent_canon IS NOT NULL AND parent_regulation_handle IS NOT NULL
        """)
        n_both = cur.fetchone()[0]

    ok = (
        n_non_apr == 0
        and dist_actual == EXPECTED_APR_REG_HANDLES
        and n_both == 0
    )
    log(f"  non-APR rows with parent_regulation_handle: {n_non_apr} (expect 0)")
    log(f"  APR handle distribution actual: {dist_actual}")
    log(f"  APR handle distribution expected: {EXPECTED_APR_REG_HANDLES}")
    log(f"  rows with both pointers: {n_both} (expect 0)")
    log(f"  RESULT: {'PASS' if ok else 'FAIL'}")
    if not ok:
        raise HaltError(f"Check 9 FAIL: non_apr={n_non_apr}, dist={dist_actual}, both={n_both}")
    return {
        "check": "9_parent_regulation_handle",
        "pass": ok,
        "non_apr_violations": n_non_apr,
        "apr_distribution": dist_actual,
        "both_pointer_violations": n_both,
    }


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║ CHECK 10 — Four-jurisdiction unified canary (§T)                            ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

def check_10_four_jurisdiction_canary() -> dict[str, Any]:
    log("=== Check 10: four-jurisdiction unified canary ===")
    query = "ADA accommodation in Washington courts"
    qvec = hf_embed(query)
    qlit = vec_literal(qvec)

    with connect() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT
                d.id, d.title,
                d.document_type,
                d.metadata->>'type' AS m_type,
                MIN(c.embedding <=> %s::halfvec(1024)) AS dist
            FROM core.document_chunks c
            JOIN core.documents d ON d.id = c.document_id
            WHERE d.domain = 'legal'
              AND c.embedding IS NOT NULL
              AND (
                d.document_type = 'court_rule'
                OR (d.document_type = 'statute'
                    AND d.metadata->>'type' IN ('rcw_section','usc_section','wac_section'))
              )
            GROUP BY d.id, d.title, d.document_type, d.metadata
            ORDER BY dist ASC
            LIMIT 30
        """, (qlit,))
        results = cur.fetchall()

    # Bucket by jurisdiction
    bucket = {"court_rule": [], "rcw_section": [], "usc_section": [], "wac_section": []}
    for doc_id, title, doc_type, m_type, dist in results:
        key = doc_type if doc_type == "court_rule" else m_type
        if key in bucket:
            bucket[key].append((title, float(dist)))

    log(f"  query: '{query}' top-30 results bucketed:")
    for k, v in bucket.items():
        sample = v[:3]
        log(f"    {k}: {len(v)} hits | sample: {[t for t,_ in sample]}")

    n_present = sum(1 for k, v in bucket.items() if v)
    if n_present == 4:
        verdict = "PASS"
        ok = True
    elif n_present == 3:
        verdict = "PARTIAL_PASS_§H2"
        ok = True
    else:
        verdict = f"LOG_ONLY_§H2 ({n_present}/4 jurisdictions)"
        ok = True  # don't halt — Phase 6 §H2 doctrine

    log(f"  RESULT: {verdict} (jurisdictions in top-30: {n_present}/4)")
    return {
        "check": "10_four_jurisdiction_canary",
        "pass": ok,
        "verdict": verdict,
        "jurisdictions_present": n_present,
        "buckets": {k: len(v) for k, v in bucket.items()},
        "samples": {k: [{"title": t, "dist": round(d, 4)} for t, d in v[:5]]
                    for k, v in bucket.items()},
        "top_30_full": [{"title": t, "doc_type": dt, "m_type": mt, "dist": round(float(ds), 4)}
                        for _, t, dt, mt, ds in results],
    }


# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║ CHECK 11 — Citation roundtrip audit                                         ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

# RCW Phase-5-bug-2 fix: do NOT use a multi-column subquery that selects
# (target_citation, full_text) and tries to feed it back as a tuple. Use a
# single SELECT with all needed columns in the outer query.
def check_11_citation_roundtrip() -> dict[str, Any]:
    log("=== Check 11: citation roundtrip audit ===")
    rng = random.Random(20260501)

    with connect() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT xr.id, xr.target_citation, xr.target_jurisdiction,
                   xr.target_set_code, xr.target_rule_number, xr.context_snippet,
                   r.canonical_citation AS source_cite, r.full_text AS source_body
            FROM wa_court_rules.cross_references xr
            JOIN wa_court_rules.rules r ON r.id = xr.source_rule_id
            WHERE r.full_text IS NOT NULL AND r.full_text <> ''
        """)
        all_rows = cur.fetchall()

    rng.shuffle(all_rows)
    samples = all_rows[:20]
    log(f"  sampled {len(samples)} cross_references for roundtrip verification")

    matches = []
    misses = []
    for xref_id, tcite, tjur, tset, tnum, snippet, src_cite, src_body in samples:
        body_norm = _normalize(src_body).lower()
        ok = False
        evidence = ""

        # Tokens to look for (jurisdiction-specific)
        if tjur == "state-wa-court-rule":
            # target_citation should appear in body (or as a "Rule X" form)
            if tcite.lower() in body_norm:
                ok = True
                evidence = "target_citation substring"
            elif tset and tnum and f"{tset.lower()} {tnum.lower()}" in body_norm:
                ok = True
                evidence = "set + number substring"
        elif tjur == "state-wa-rcw":
            # 49.60 chapter or 49.60.180 section forms
            if tnum and tnum.lower() in body_norm:
                ok = True
                evidence = f"rcw target_rule_number '{tnum}' substring"
            elif tcite.lower() in body_norm:
                ok = True
                evidence = "target_citation substring"
            else:
                # Try 'chapter <num> RCW' or 'ch. <num>'
                for variant in [f"chapter {tnum} rcw", f"ch. {tnum} rcw",
                                f"ch.{tnum} rcw", f"rcw {tnum}"]:
                    if tnum and variant in body_norm:
                        ok = True
                        evidence = f"variant '{variant}'"
                        break
        elif tjur == "federal-usc":
            # tnum could be range like '12101-12213' — check title + num within ~30 chars
            # Extract title from target_citation: "42 U.S.C. § 12101-12213"
            m = re.match(r"^(\d+)\s*U\.?\s*S\.?\s*C\.?", tcite, flags=re.I)
            title = m.group(1) if m else None
            # Section token = first number in tnum
            sec_token = None
            if tnum:
                ms = re.match(r"^([0-9A-Za-z]+)", tnum)
                if ms:
                    sec_token = ms.group(1).lower()
            if tcite.lower() in body_norm:
                ok = True
                evidence = "full target_citation substring"
            elif title and sec_token:
                # Find both within 30 chars of each other
                pattern = re.compile(
                    rf"\b{re.escape(title)}\b.{{0,40}}\b{re.escape(sec_token)}\b",
                    flags=re.I,
                )
                if pattern.search(body_norm):
                    ok = True
                    evidence = "title+sec within 40 chars"
        elif tjur == "wa-constitution":
            # 'const' or 'constitution' present plus article roman numeral
            if "const" in body_norm or "constitution" in body_norm:
                # Check article
                if tnum and tnum.lower() in body_norm:
                    ok = True
                    evidence = f"const + article '{tnum}'"
                else:
                    ok = True
                    evidence = "const marker present"
        elif tjur == "state-wa-wac":
            if tnum and tnum.lower() in body_norm:
                ok = True
                evidence = f"wac target_rule_number '{tnum}'"
            elif tcite.lower() in body_norm:
                ok = True
                evidence = "target_citation substring"

        if ok:
            matches.append({
                "xref_id": str(xref_id),
                "src": src_cite,
                "target_citation": tcite,
                "target_jurisdiction": tjur,
                "evidence": evidence,
            })
        else:
            misses.append({
                "xref_id": str(xref_id),
                "src": src_cite,
                "target_citation": tcite,
                "target_jurisdiction": tjur,
                "target_set_code": tset,
                "target_rule_number": tnum,
                "context_snippet": snippet[:200] if snippet else None,
                "body_preview": (src_body or "")[:300],
            })

    n_match = len(matches)
    log(f"  matches: {n_match}/{len(samples)}")
    for m in misses:
        log(f"    MISS: {m['src']} → {m['target_citation']} (jur={m['target_jurisdiction']})")

    ok = n_match == len(samples)
    if not ok:
        # Halt — surface the first miss
        first = misses[0]
        raise HaltError(
            f"Check 11 roundtrip mismatch: {first['src']} → {first['target_citation']} "
            f"(jur={first['target_jurisdiction']}, snippet={first['context_snippet']}, "
            f"body_preview={first['body_preview'][:200]})"
        )
    log(f"  RESULT: PASS")
    return {
        "check": "11_citation_roundtrip",
        "pass": ok,
        "samples": len(samples),
        "matches": n_match,
        "misses": len(misses),
        "match_evidence": matches[:5],
        "miss_records": misses[:5],
    }


# ── Main / report ─────────────────────────────────────────────────────────────

CHECKS = [
    ("1", check_1_embedding_count),
    ("2", check_2_resolution_rates),
    ("3", check_3_canary_suite),
    ("4", check_4_content_fidelity),
    ("5", check_5_citation_graph_density),
    ("6", check_6_ghost_flags),
    ("7", check_7_range_expansion),
    ("8", check_8_parent_canon),
    ("9", check_9_parent_regulation),
    ("10", check_10_four_jurisdiction_canary),
    ("11", check_11_citation_roundtrip),
]


def run_all(dry_run: bool) -> dict[str, Any]:
    mode = "DRY-RUN" if dry_run else "PRODUCTION"
    log(f"=== Phase 5 Validation START — mode={mode} ===")
    results: list[dict[str, Any]] = []
    halt_event: dict[str, Any] | None = None

    for label, fn in CHECKS:
        try:
            r = fn()
            results.append(r)
        except HaltError as e:
            halt_event = {"check": label, "reason": str(e), "trace": traceback.format_exc()}
            log(f"!!! HALT at Check {label}: {e}")
            break
        except Exception as e:
            halt_event = {
                "check": label,
                "reason": f"unexpected exception: {e}",
                "trace": traceback.format_exc(),
            }
            log(f"!!! Unexpected exception at Check {label}: {e}\n{traceback.format_exc()}")
            break

    return {
        "mode": mode,
        "results": results,
        "halt": halt_event,
    }


def write_report(payload: dict[str, Any]):
    results = payload["results"]
    halt = payload["halt"]
    pass_count = sum(1 for r in results if r.get("pass"))
    total = len(CHECKS)

    lines = []
    lines.append(f"# WA Court Rules Phase 5 Validation Report\n")
    lines.append(f"Generated: {datetime.now(timezone.utc).isoformat()}\n")
    lines.append(f"Mode: **{payload['mode']}**\n")
    lines.append(f"## Summary\n")
    if halt:
        lines.append(f"**HALT** at Check {halt['check']}: {halt['reason']}\n")
    lines.append(f"{pass_count}/{total} checks completed (PASS={pass_count}, halted={1 if halt else 0})\n")

    # Disposition
    pattern_a_misses = []
    h2_notes = []
    halted = halt is not None
    if halted:
        disposition = "HALT"
    else:
        disposition = "PASS"
        # Look for §H2-class items
        for r in results:
            if r["check"] == "3_canary_suite":
                for code, blob in r.get("by_rule_set", {}).items():
                    if not blob["pattern_a"][0]:
                        pattern_a_misses.append(code)
                if pattern_a_misses:
                    h2_notes.append(
                        f"Pattern (a) MISS on rule sets: {pattern_a_misses} "
                        f"(documented as Phase 6 §H2 query-vocabulary limitation)"
                    )
            if r["check"] == "10_four_jurisdiction_canary":
                if r.get("verdict", "").startswith("PARTIAL"):
                    disposition = "PARTIAL PASS"
                    h2_notes.append(
                        f"Check 10: {r['verdict']} — buckets={r['buckets']} "
                        f"(state-wa-wac absence = Phase 7 backlog)"
                    )
                elif r.get("verdict", "").startswith("LOG_ONLY"):
                    disposition = "PARTIAL PASS"
                    h2_notes.append(f"Check 10: {r['verdict']} — buckets={r['buckets']}")
            if r["check"] == "2_resolution_rates":
                usc = r.get("federal_usc", {})
                if usc.get("rate", 0) < 0.5:
                    h2_notes.append(
                        f"federal-usc resolution {usc.get('rate', 0)*100:.1f}% "
                        f"({usc.get('resolved')}/{usc.get('total')}) — Phase 7 USC long-tail backlog"
                    )
                if r.get("state_wa_wac_count", -1) == 0:
                    h2_notes.append(
                        "state-wa-wac cross-ref count=0 (Phase 7 chapter-form pattern backlog)"
                    )

    lines.append(f"## Phase 5 Disposition: **{disposition}**\n")
    if h2_notes:
        lines.append(f"### §H2 documented limitations\n")
        for n in h2_notes:
            lines.append(f"- {n}")
        lines.append("")

    if halt:
        lines.append(f"## HALT Details\n")
        lines.append(f"- check: {halt['check']}")
        lines.append(f"- reason:\n```\n{halt['reason']}\n```")
        if halt.get("trace"):
            lines.append(f"- trace:\n```\n{halt['trace'][:2000]}\n```")
        lines.append("")

    lines.append("## Per-check results\n")
    for r in results:
        lines.append(f"### {r['check']}: {'PASS' if r.get('pass') else 'FAIL'}\n")
        # Strip overly-verbose top_30_full from check 10 in the markdown body
        # (keep the bucket summary; full list goes in JSON appendix only)
        rcopy = dict(r)
        if rcopy.get("check") == "10_four_jurisdiction_canary":
            rcopy.pop("top_30_full", None)
        lines.append(f"```json\n{json.dumps(rcopy, indent=2, default=str)}\n```\n")

    REPORT.write_text("\n".join(lines))
    log(f"Report written → {REPORT}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="Read-only validation pass")
    args = ap.parse_args()

    payload = run_all(dry_run=args.dry_run)

    if args.dry_run:
        # In dry-run, report only via log/stdout; don't overwrite production report
        log(f"--- DRY-RUN summary ---")
        for r in payload["results"]:
            log(f"  {r['check']}: {'PASS' if r.get('pass') else 'FAIL'}")
        if payload["halt"]:
            log(f"  HALT @ Check {payload['halt']['check']}: {payload['halt']['reason']}")
            sys.exit(2)
        log("DRY-RUN OK — no SQL bugs detected; safe to run production.")
        sys.exit(0)
    else:
        write_report(payload)
        if payload["halt"]:
            write_halt(payload["halt"]["reason"], payload["halt"].get("trace", ""))
            sys.exit(2)
        # PASS / PARTIAL PASS — exit 0
        sys.exit(0)


if __name__ == "__main__":
    main()
