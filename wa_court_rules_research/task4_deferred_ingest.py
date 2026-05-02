"""
Refinement Sweep — Task 4: WA Court Rules Phase 8 deferred-set ingest.

Self-contained ingest for the 17 Phase-8-deferred sets:
  * 9 substantive sets (135 enumerated rules): CCR, IRLJ, AR, ARLJ, BJAR, DRJ,
    JISCR, SPR, SPRC
  * 6 mono-PDF document-level sets (1 row each, 6 rows total): ELLLTC, ELPOC,
    LPORPC, ATJ, BJA, SCAR
  * 2 skip-with-justification (no PDF body): BBP, LLLT RPC

Total ingestable: 141 new rules.

Mirrors phase4_ingest.py per-rule contract:
  1. Fetch PDF (1 req/sec to courts.wa.gov; defensive URL-quoting)
  2. Extract text via pdftotext
  3. INSERT core.documents
  4. BGE-M3 embedding via HF .187; INSERT core.document_chunks
  5. INSERT wa_court_rules.rules (rule_set_code is the key; FK to document)
  6. Extract cross-references (RCW/USC/WAC/court-rule patterns)
  7. After all sets ingested: INSERT wa_court_rules.cross_references with
     target_rule_id resolved (in-corpus) and target_rcw/usc_section_id resolved
     for cross-corpus refs

Halt-on-error: any CHECK violation, fetch failure beyond 3 retries, or
embedding endpoint degradation aborts the run with TX rollback.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import traceback
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

import httpx
import psycopg2
import psycopg2.extras

sys.path.insert(0, str(Path(__file__).parent))
from phase1_regex_test import (  # type: ignore
    WACR_CITE_RE, RCW_CITE_RE, RCW_CHAPTER_CITE_RE,
    USC_CITE_RE, WAC_CITE_RE, WACONST_CITE_RE,
    parse_cite, normalize_usc_section,
)

DSN = os.environ.get("DATABASE_URL", "postgresql://wdws:NEL2233obs@127.0.0.1:5432/wdws")
RESEARCH = Path("/opt/wdws/wa_court_rules_research")
INVENTORY = RESEARCH / "cached_html" / "inventory.json"
PDF_ROOT = Path("/opt/wdws/data/wa_court_rules/pdf")
REPORT_PATH = RESEARCH / "task4_cumulative_report.md"
HALT_PATH = RESEARCH / "TASK4_HALT.md"
LOG_PATH = RESEARCH / "task4_ingest.log"

UA = "Athena Cognitive Engine / WA Court Rules ingest / wjnelson78@gmail.com"
BASE_URL = "https://www.courts.wa.gov/court_rules/"
RATE_LIMIT_SEC = 1.0
FETCH_TIMEOUT = 30
MAX_RETRIES = 3

HF_ENDPOINT = "http://172.16.81.187:9098"
HF_API_TOKEN = ""
for line in Path("/opt/wdws/.env").read_text().splitlines():
    if line.strip().startswith("HF_API_TOKEN="):
        HF_API_TOKEN = line.split("=", 1)[1].strip().strip('"').strip("'")
        break
assert HF_API_TOKEN, "HF_API_TOKEN missing from /opt/wdws/.env"
EMBEDDING_MODEL_ID = 2
EMBEDDING_DIMS = 1024
CHUNK_TARGET_CHARS = 6000
HF_BATCH_SIZE = 32
HF_TIMEOUT = 30.0

PUBLICATION_DATE = dt.date(2026, 5, 1)

DEFERRED_SET_NAMES = {
    "CCR":     ("Superior Court Civil Commitment Rules",                      "substantive"),
    "IRLJ":    ("Infraction Rules for Courts of Limited Jurisdiction",        "substantive"),
    "AR":      ("Superior Court Administrative Rules",                        "substantive"),
    "ARLJ":    ("Administrative Rules for Courts of Limited Jurisdiction",    "substantive"),
    "BJAR":    ("Board for Judicial Administration Rules",                    "substantive"),
    "DRJ":     ("Discipline Rules for Judges",                                "substantive"),
    "JISCR":   ("Judicial Information System Committee Rules",                "substantive"),
    "SPR":     ("Superior Court Special Proceedings",                         "substantive"),
    "SPRC":    ("Superior Court Special Proceedings Rules — Criminal",        "substantive"),
    "ELLLTC":  ("Rules for Enforcement of Limited License Legal Technician Conduct", "mono"),
    "ELPOC":   ("Rules for Enforcement of Limited Practice Officer Conduct",  "mono"),
    "LPORPC":  ("Limited Practice Officer Rules of Professional Conduct",     "mono"),
    "ATJ":     ("Access to Justice Technology Principles",                    "mono"),
    "BJA":     ("Advisory Case Processing Time Standards",                    "mono"),
    "SCAR":    ("Supplemental Court of Appeals Administrative Rule",          "mono"),
}

SKIP_SETS = {
    "BBP":     ("Bench-Bar-Press Committee Statement of Principles",
                "narrative landing page only; zero PDF links; not an enumerated rule set"),
    "LLLT RPC":("Limited License Legal Technician Rules of Professional Conduct",
                "LLLT program sunset 2020; landing page has zero rule body and zero PDF links"),
}

EXPECTED_PER_SET = {
    "CCR": 34, "IRLJ": 26, "AR": 6, "ARLJ": 16, "BJAR": 5,
    "DRJ": 14, "JISCR": 18, "SPR": 9, "SPRC": 7,
    "ELLLTC": 1, "ELPOC": 1, "LPORPC": 1, "ATJ": 1, "BJA": 1, "SCAR": 1,
}
EXPECTED_TOTAL = sum(EXPECTED_PER_SET.values())  # 141

# Mono-PDF inventory entries (set -> (pdf_relative_path, title))
MONO_PDF_OVERRIDES = {
    "ATJ":  ("../court_rules/pdf/AM/AM_ATJ_ATJTechnologyPrinciples.pdf", "Access to Justice Technology Principles"),
    "BJA":  ("../court_rules/pdf/BJA/AM_BJA_TIME.pdf",                    "Advisory Case Processing Time Standards"),
    "SCAR": ("../court_rules/pdf/SCAR/ACA_SCAR_00_00_00.pdf",             "Supplemental Court of Appeals Administrative Rule"),
}


def log(msg: str) -> None:
    ts = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with LOG_PATH.open("a") as f:
        f.write(line + "\n")


class HaltError(Exception):
    pass


def write_halt(reason: str, details: str) -> None:
    HALT_PATH.write_text(
        f"# Task 4 HALT\n\n**Time:** {dt.datetime.now().isoformat()}\n\n"
        f"**Reason:** {reason}\n\n## Details\n\n```\n{details}\n```\n"
    )
    log(f"HALT: {reason}")


def resolve_pdf_url(rel_path: str) -> str:
    rel = rel_path.lstrip("./")
    if rel.startswith("court_rules/"):
        rel = rel[len("court_rules/"):]
    rel = urllib.parse.quote(rel, safe="/")
    return BASE_URL + rel


def cache_pdf_path(set_code: str, pdf_rel: str) -> Path:
    fname = pdf_rel.split("/")[-1]
    return PDF_ROOT / set_code / fname


def fetch_pdf(url: str, dest: Path) -> bytes:
    if dest.exists() and dest.stat().st_size > 100:
        return dest.read_bytes()
    dest.parent.mkdir(parents=True, exist_ok=True)
    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT) as r:
                if r.status != 200:
                    raise RuntimeError(f"HTTP {r.status}")
                ctype = r.headers.get("Content-Type", "")
                body = r.read()
                if "pdf" not in ctype.lower() and not body.startswith(b"%PDF"):
                    raise RuntimeError(f"non-PDF content-type: {ctype}")
                if len(body) < 100:
                    raise RuntimeError("body <100 bytes (likely error page)")
                dest.write_bytes(body)
                time.sleep(RATE_LIMIT_SEC)
                return body
        except Exception as e:
            last_err = e
            log(f"  fetch attempt {attempt+1}/{MAX_RETRIES} for {url} failed: {e}")
            time.sleep(2 * (attempt + 1))
    raise HaltError(f"PDF fetch failed after {MAX_RETRIES}: {url} -- {last_err}")


def extract_text(pdf_path: Path) -> str:
    if not pdf_path.exists():
        raise HaltError(f"PDF missing: {pdf_path}")
    try:
        out = subprocess.check_output(
            ["pdftotext", "-layout", str(pdf_path), "-"],
            stderr=subprocess.DEVNULL, timeout=30,
        )
        text = out.decode("utf-8", errors="replace")
        return text.strip()
    except subprocess.CalledProcessError as e:
        raise HaltError(f"pdftotext failed for {pdf_path}: {e}")


_hf_client: httpx.Client | None = None


def hf_client() -> httpx.Client:
    global _hf_client
    if _hf_client is None:
        _hf_client = httpx.Client(
            timeout=HF_TIMEOUT,
            headers={"Authorization": f"Bearer {HF_API_TOKEN}", "Content-Type": "application/json"},
        )
    return _hf_client


_embed_calls = 0
_embed_total_s = 0.0


def hf_embed_batch(texts: list[str]) -> list[list[float]]:
    global _embed_calls, _embed_total_s
    if not texts:
        return []
    out: list[list[float]] = []
    client = hf_client()
    for i in range(0, len(texts), HF_BATCH_SIZE):
        chunk = [(t or "")[:32768] for t in texts[i:i+HF_BATCH_SIZE]]
        t0 = time.time()
        r = client.post(f"{HF_ENDPOINT}/embed", json={"inputs": chunk})
        elapsed = time.time() - t0
        _embed_calls += 1
        _embed_total_s += elapsed
        if elapsed > 5.0:
            raise HaltError(f"HF endpoint degradation: batch of {len(chunk)} took {elapsed:.2f}s")
        if r.status_code >= 500:
            raise HaltError(f"HF endpoint 5xx: {r.status_code} {r.text[:200]}")
        r.raise_for_status()
        vecs = r.json()
        if len(vecs) != len(chunk):
            raise HaltError(f"HF returned {len(vecs)} vectors for {len(chunk)} inputs")
        for v in vecs:
            if len(v) != EMBEDDING_DIMS:
                raise HaltError(f"HF returned {len(v)}-dim vector, expected {EMBEDDING_DIMS}")
        out.extend(vecs)
    return out


def vec_literal(vec: list[float]) -> str:
    return "[" + ",".join(f"{v:.6f}" for v in vec) + "]"


def chunk_text(full_text: str, breadcrumb: str) -> list[str]:
    if not full_text or not full_text.strip():
        return []
    full = f"{breadcrumb}\n\n{full_text}"
    if len(full) <= CHUNK_TARGET_CHARS:
        return [full]
    out = []
    paras = full_text.split("\n\n")
    buf = ""
    target = CHUNK_TARGET_CHARS - len(breadcrumb) - 4
    for p in paras:
        if len(buf) + len(p) + 2 <= target:
            buf = (buf + "\n\n" + p) if buf else p
        else:
            if buf:
                out.append(f"{breadcrumb}\n\n{buf}")
                buf = ""
            if len(p) > target:
                for i in range(0, len(p), target):
                    out.append(f"{breadcrumb}\n\n{p[i:i+target]}")
            else:
                buf = p
    if buf:
        out.append(f"{breadcrumb}\n\n{buf}")
    return out


def detect_status_flags(title: str, body: str) -> tuple[bool, bool]:
    """Return (is_repealed, is_reserved). Mirrors phase4_ingest.refine_ghost_flags."""
    t_up = (title or "").upper()
    b_up = (body or "").upper()
    has_reserved = "RESERVED" in t_up or "[RESERVED]" in b_up or "RESERVED" in b_up[:200]
    has_repealed = any(kw in t_up for kw in ("REPEALED", "RESCINDED", "REMOVED", "TRANSFERRED")) or \
                   any(kw in b_up[:300] for kw in ("RESCINDED", "REPEALED"))
    if has_repealed and has_reserved:
        return (True, True)
    if has_reserved and not has_repealed:
        return (False, True)
    if has_repealed and not has_reserved:
        return (True, False)
    return (False, False)


def extract_cross_refs(body: str) -> list[dict]:
    """Pull RCW/USC/WAC/court-rule citations from a rule body."""
    if not body:
        return []
    refs: list[dict] = []
    seen = set()

    for m in WACR_CITE_RE.finditer(body):
        info = parse_cite(m.group(0))
        if not info:
            continue
        set_c = info.get("set")
        num = info.get("num")
        subs_list = info.get("subs") or []
        cmt = info.get("cmt")
        if not set_c or not num:
            continue
        key = ("court", set_c, num)
        if key in seen:
            continue
        seen.add(key)
        ctx_start = max(0, m.start() - 60)
        ctx_end = min(len(body), m.end() + 60)
        refs.append({
            "target_citation": f"{set_c} {num}",
            "target_set_code": set_c,
            "target_rule_number": num,
            "target_subsection": subs_list[0] if subs_list else None,
            "target_cmt_index": cmt,
            "target_jurisdiction": "state-wa-court-rule",
            "context_snippet": body[ctx_start:ctx_end].replace("\n", " "),
            "ref_type": "cross-rule",
        })

    for m in RCW_CITE_RE.finditer(body):
        cite = m.group(0).strip()
        section = m.group(1) if m.lastindex else None
        key = ("rcw", cite)
        if key in seen:
            continue
        seen.add(key)
        ctx_start = max(0, m.start() - 60)
        ctx_end = min(len(body), m.end() + 60)
        refs.append({
            "target_citation": f"RCW {section}" if section else cite,
            "target_set_code": "RCW",
            "target_rule_number": section,
            "target_jurisdiction": "state-wa-rcw",
            "context_snippet": body[ctx_start:ctx_end].replace("\n", " "),
            "ref_type": "cross-statute",
        })

    for m in RCW_CHAPTER_CITE_RE.finditer(body):
        cite = m.group(0).strip()
        chap = m.group(1) if m.lastindex else None
        key = ("rcw", cite)
        if key in seen:
            continue
        seen.add(key)
        ctx_start = max(0, m.start() - 60)
        ctx_end = min(len(body), m.end() + 60)
        refs.append({
            "target_citation": f"RCW {chap}" if chap else cite,
            "target_set_code": "RCW",
            "target_rule_number": chap,
            "target_jurisdiction": "state-wa-rcw",
            "context_snippet": body[ctx_start:ctx_end].replace("\n", " "),
            "ref_type": "cross-statute-chapter",
        })

    for m in USC_CITE_RE.finditer(body):
        cite = m.group(0).strip()
        section = normalize_usc_section(m.group(0)) if m.group(0) else None
        key = ("usc", cite)
        if key in seen:
            continue
        seen.add(key)
        ctx_start = max(0, m.start() - 60)
        ctx_end = min(len(body), m.end() + 60)
        refs.append({
            "target_citation": cite,
            "target_set_code": "USC",
            "target_rule_number": section,
            "target_jurisdiction": "federal-usc",
            "context_snippet": body[ctx_start:ctx_end].replace("\n", " "),
            "ref_type": "cross-statute-federal",
        })

    for m in WAC_CITE_RE.finditer(body):
        cite = m.group(0).strip()
        key = ("wac", cite)
        if key in seen:
            continue
        seen.add(key)
        ctx_start = max(0, m.start() - 60)
        ctx_end = min(len(body), m.end() + 60)
        refs.append({
            "target_citation": cite,
            "target_set_code": "WAC",
            "target_rule_number": cite.replace("WAC ", "").strip() if cite.startswith("WAC ") else None,
            "target_jurisdiction": "state-wa-wac",
            "context_snippet": body[ctx_start:ctx_end].replace("\n", " "),
            "ref_type": "cross-rule-wac",
        })

    for m in WACONST_CITE_RE.finditer(body):
        cite = m.group(0).strip()
        key = ("waconst", cite)
        if key in seen:
            continue
        seen.add(key)
        ctx_start = max(0, m.start() - 60)
        ctx_end = min(len(body), m.end() + 60)
        refs.append({
            "target_citation": cite,
            "target_set_code": "WA-CONST",
            "target_rule_number": None,
            "target_jurisdiction": "wa-constitution",
            "context_snippet": body[ctx_start:ctx_end].replace("\n", " "),
            "ref_type": "cross-constitution",
        })

    return refs


def ensure_rule_set(conn, code: str) -> None:
    name = DEFERRED_SET_NAMES[code][0]
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO wa_court_rules.rule_sets
                (rule_set_code, rule_set_name, promulgating_court, statutory_authority,
                 publication_date, tier, is_in_scope)
            VALUES (%s, %s, 'WA Supreme Court', 'RCW 2.04.190', %s, NULL, TRUE)
            ON CONFLICT (rule_set_code) DO NOTHING
        """, (code, name, PUBLICATION_DATE))


def ingest_one_rule(conn, rec: dict) -> str:
    code = rec["rule_set_code"]
    citation = rec["canonical_citation"]
    full_text = rec.get("full_text") or ""

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
        "source_pdf_path": rec.get("source_pdf_path"),
        "source_pdf_url":  rec.get("source_pdf_url"),
        "tier": 8,
        "phase": "Task 4 deferred-set ingest 2026-05-01",
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
        """, (title_str, source_path, filename, full_text, content_hash, json.dumps(metadata)))
        document_id = cur.fetchone()[0]

        chunks: list[str] = []
        if full_text and full_text.strip():
            breadcrumb = f"{DEFERRED_SET_NAMES[code][0]} ({code}) — {citation}: {rec['rule_title']}"
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

        cur.execute("""
            INSERT INTO wa_court_rules.rules
                (document_id, rule_set_code, rule_number, rule_title, canonical_citation,
                 is_repealed, is_reserved,
                 range_doc_id, range_start, range_end,
                 parent_canon, parent_regulation_handle,
                 full_text, source_pdf_path,
                 has_comment, comment_text,
                 effective_date, last_amended_date, publication_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s,
                    NULL, NULL, NULL, NULL, NULL,
                    %s, %s, FALSE, NULL,
                    NULL, NULL, %s)
            RETURNING id
        """, (
            document_id, code, rec["rule_number"], rec["rule_title"], citation,
            rec.get("is_repealed", False), rec.get("is_reserved", False),
            (full_text if full_text else None),
            rec.get("source_pdf_path"),
            PUBLICATION_DATE,
        ))
        rule_id = cur.fetchone()[0]
    return rule_id


def insert_xrefs(conn, citation_to_uuid: dict[str, str], records: list[dict]) -> dict:
    """Insert cross_references with target_rule_id (in-corpus) +
    target_rcw_section_id / target_usc_section_id (cross-corpus) resolved."""
    rows = []
    skipped_self = 0
    for rec in records:
        src_uuid = citation_to_uuid[rec["canonical_citation"]]
        src_code = rec["rule_set_code"]
        src_num = rec["rule_number"]
        for x in rec.get("cross_references", []):
            t_set = x.get("target_set_code")
            t_num = x.get("target_rule_number")
            t_jur = x.get("target_jurisdiction") or "state-wa-court-rule"
            if t_set == src_code and t_num == src_num:
                skipped_self += 1
                continue
            target_uuid = None
            if t_jur == "state-wa-court-rule" and t_set and t_num:
                target_cite = f"{t_set} {t_num}"
                target_uuid = citation_to_uuid.get(target_cite)
            rows.append((
                src_uuid,
                x.get("target_citation") or f"{t_set} {t_num}".strip(),
                target_uuid,
                t_jur,
                t_set,
                t_num,
                x.get("target_subsection"),
                x.get("target_cmt_index"),
                (x.get("context_snippet") or "")[:500],
                x.get("ref_type") or "reference",
            ))
    if not rows:
        return {"inserted": 0, "skipped_self": skipped_self}

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, """
            INSERT INTO wa_court_rules.cross_references
                (source_rule_id, target_citation, target_rule_id,
                 target_jurisdiction, target_set_code, target_rule_number,
                 target_subsection, target_cmt_index, context_snippet, ref_type)
            VALUES %s
        """, rows)

        # Resolve cross-corpus FKs by direct join
        cur.execute("""
            UPDATE wa_court_rules.cross_references c
            SET target_rcw_section_id = s.id
            FROM rcw.sections s
            WHERE c.target_jurisdiction='state-wa-rcw'
              AND c.target_rcw_section_id IS NULL
              AND s.canonical_citation = c.target_citation
              AND c.source_rule_id = ANY(%s::uuid[])
        """, (list(citation_to_uuid.values()),))
        cur.execute("""
            UPDATE wa_court_rules.cross_references c
            SET target_usc_section_id = s.id
            FROM usc.sections s
            WHERE c.target_jurisdiction='federal-usc'
              AND c.target_usc_section_id IS NULL
              AND s.canonical_citation = c.target_citation
              AND c.source_rule_id = ANY(%s::uuid[])
        """, (list(citation_to_uuid.values()),))
    return {"inserted": len(rows), "skipped_self": skipped_self}


def build_substantive_records(set_code: str, set_entry: dict, conn) -> list[dict]:
    """For the 9 substantive sets: per inventory rule, fetch PDF, extract, build record."""
    records = []
    for r in set_entry["rules"]:
        num = r["num"]
        title = (r.get("title") or "").strip()
        pdf_rel = r.get("pdf")
        if not pdf_rel:
            raise HaltError(f"{set_code} {num}: no pdf in inventory")
        url = resolve_pdf_url(pdf_rel)
        dest = cache_pdf_path(set_code, pdf_rel)
        try:
            fetch_pdf(url, dest)
        except HaltError:
            raise
        body = extract_text(dest)
        is_rep, is_res = detect_status_flags(title, body)
        # Reserved/repealed body stripping (per chk_repealed_text_meaningful + chk_rule_text_status_invariant)
        if is_res and not body.strip():
            body_for_db = None
        elif is_res:
            # Reserved rules sometimes have body explaining the reservation; keep
            body_for_db = body
        elif is_rep:
            # If body lacks rescind/repealed/etc keyword, force it to NULL (constraint).
            body_lower = body.lower()
            if not any(k in body_lower for k in ("rescind", "deleted", "repealed", "removed", "transferred")):
                body_for_db = None
            else:
                body_for_db = body
        else:
            body_for_db = body if body.strip() else None

        if body_for_db is None and not is_rep and not is_res:
            # chk_rule_text_status_invariant requires text or repealed/reserved.
            # Mark reserved if title contains placeholder-like "Reserved"
            if "reserved" in (title or "").lower():
                is_res = True
            else:
                # Empty-PDF anomaly — surface to halt
                raise HaltError(f"{set_code} {num}: empty body and no repealed/reserved flag; PDF may be corrupted ({dest})")

        records.append({
            "rule_set_code": set_code,
            "rule_number": num,
            "rule_title": title or "(untitled)",
            "canonical_citation": f"{set_code} {num}",
            "is_repealed": is_rep,
            "is_reserved": is_res,
            "full_text": body_for_db,
            "source_pdf_path": str(dest),
            "source_pdf_url": url,
            "cross_references": extract_cross_refs(body_for_db or ""),
        })
    return records


def build_mono_records(set_code: str, set_entry: dict) -> list[dict]:
    """For mono-PDF document-level sets: 1 row per set."""
    if set_entry.get("rules"):
        # Inventory has a single rule entry (e.g., ELLLTC has 1 rule)
        r = set_entry["rules"][0]
        num = r["num"]
        title = (r.get("title") or DEFERRED_SET_NAMES[set_code][0]).strip()
        pdf_rel = r.get("pdf")
    else:
        # Inventory empty but cached HTML has a PDF link (ATJ/BJA/SCAR override)
        pdf_rel, title = MONO_PDF_OVERRIDES[set_code]
        num = set_code
    url = resolve_pdf_url(pdf_rel)
    dest = cache_pdf_path(set_code, pdf_rel)
    fetch_pdf(url, dest)
    body = extract_text(dest)
    is_rep, is_res = detect_status_flags(title, body)
    if not body.strip() and not is_rep and not is_res:
        raise HaltError(f"{set_code} mono-PDF empty body and no flag ({dest})")

    return [{
        "rule_set_code": set_code,
        "rule_number": num,
        "rule_title": title,
        "canonical_citation": f"{set_code} {num}",
        "is_repealed": is_rep,
        "is_reserved": is_res,
        "full_text": body if body.strip() else None,
        "source_pdf_path": str(dest),
        "source_pdf_url": url,
        "cross_references": extract_cross_refs(body),
    }]


def main() -> int:
    log("=== Task 4 Deferred-Set Ingest ===")
    log(f"Started: {dt.datetime.now().isoformat()}")
    log(f"Target: {EXPECTED_TOTAL} rules across 15 sets (9 substantive + 6 mono-PDF), 2 skip-with-justification")

    inventory = json.loads(INVENTORY.read_text())
    inv_by_code = {e["set_code"]: e for e in inventory}

    t_start = time.time()
    conn = psycopg2.connect(DSN)
    conn.autocommit = False
    stats: dict[str, Any] = {"sets": {}, "skipped": list(SKIP_SETS.keys())}
    citation_to_uuid: dict[str, str] = {}
    all_records: list[dict] = []

    try:
        # Pre-flight: HF endpoint
        h = httpx.get(f"{HF_ENDPOINT}/health", timeout=5.0)
        if h.text.strip() != "ok":
            raise HaltError(f"HF health: {h.text[:100]}")
        log("[ok] HF .187 healthy")

        # Pre-flight: confirm none of the 15 deferred sets already have rows
        with conn.cursor() as cur:
            cur.execute("""
                SELECT rule_set_code, COUNT(*) FROM wa_court_rules.rules
                WHERE rule_set_code = ANY(%s) GROUP BY rule_set_code
            """, (list(EXPECTED_PER_SET.keys()),))
            preexisting = dict(cur.fetchall())
        if preexisting:
            raise HaltError(f"Pre-existing deferred-set rows detected: {preexisting}")
        log("[ok] No pre-existing deferred-set rules; clean state")

        # Open ingestion run
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO wa_court_rules.ingestion_runs (run_label, rule_sets_attempted)
                VALUES (%s, %s) RETURNING id
            """, ("task-4-deferred-ingest-2026-05-01", list(EXPECTED_PER_SET.keys())))
            run_id = cur.fetchone()[0]
        conn.commit()

        # Process each set
        for code, (name, kind) in DEFERRED_SET_NAMES.items():
            inv = inv_by_code.get(code)
            if inv is None:
                raise HaltError(f"Set {code} not in inventory")
            log(f"\n--- {code} ({kind}, expect {EXPECTED_PER_SET[code]}) ---")
            ensure_rule_set(conn, code)

            if kind == "substantive":
                records = build_substantive_records(code, inv, conn)
            else:
                records = build_mono_records(code, inv)

            actual = len(records)
            expected = EXPECTED_PER_SET[code]
            if actual > 2 * expected or actual < expected / 2:
                raise HaltError(f"{code}: built {actual} records, expected {expected} (>2x deviation)")
            if actual != expected:
                log(f"  [warn] {code}: built {actual}, expected {expected} (within tolerance, surface)")

            t0 = time.time()
            inserted = 0
            for rec in records:
                try:
                    rid = ingest_one_rule(conn, rec)
                    citation_to_uuid[rec["canonical_citation"]] = rid
                    all_records.append(rec)
                    inserted += 1
                except psycopg2.errors.CheckViolation as e:
                    conn.rollback()
                    raise HaltError(
                        f"CHECK violation on {rec['canonical_citation']}: {e}\n"
                        f"Row: rule_number={rec['rule_number']} title={rec['rule_title'][:80]} "
                        f"is_repealed={rec.get('is_repealed')} is_reserved={rec.get('is_reserved')} "
                        f"body_len={len(rec.get('full_text') or '')}"
                    )
                except Exception as e:
                    conn.rollback()
                    raise HaltError(f"INSERT failed on {rec['canonical_citation']}: {e}\n{traceback.format_exc()}")
            conn.commit()
            elapsed = time.time() - t0
            log(f"  [{code}] inserted={inserted}/{expected} elapsed={elapsed:.1f}s")

            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE wa_court_rules.rule_sets SET rule_count=%s, last_ingested_at=NOW()
                    WHERE rule_set_code=%s
                """, (inserted, code))
            conn.commit()
            stats["sets"][code] = {"inserted": inserted, "expected": expected, "elapsed_s": round(elapsed, 1)}

        # Cross-references
        log("\n=== Cross-references INSERT ===")
        xref_result = insert_xrefs(conn, citation_to_uuid, all_records)
        conn.commit()
        log(f"  inserted={xref_result['inserted']} skipped_self={xref_result['skipped_self']}")
        stats["xrefs"] = xref_result

        # Close ingestion run
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE wa_court_rules.ingestion_runs
                SET completed_at=NOW(), rules_inserted=(
                    SELECT COUNT(*) FROM wa_court_rules.rules WHERE rule_set_code=ANY(%s)
                )
                WHERE id=%s
            """, (list(EXPECTED_PER_SET.keys()), run_id))
        conn.commit()

        wall = time.time() - t_start
        stats["wall_time_s"] = round(wall, 1)
        stats["embed_calls"] = _embed_calls
        stats["embed_total_s"] = round(_embed_total_s, 1)
        log(f"\n=== Task 4 ingest complete ===")
        log(f"Total rules: {sum(s['inserted'] for s in stats['sets'].values())} / {EXPECTED_TOTAL}")
        log(f"Wall time: {wall:.1f}s")
        log(f"Embed calls: {_embed_calls}; total {_embed_total_s:.1f}s")

        REPORT_PATH.write_text(json.dumps(stats, indent=2))
        return 0

    except HaltError as e:
        conn.rollback()
        write_halt("HaltError", str(e))
        return 2
    except Exception as e:
        conn.rollback()
        write_halt("Unexpected", f"{e}\n{traceback.format_exc()}")
        return 3
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
