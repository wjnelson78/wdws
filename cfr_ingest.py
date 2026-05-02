#!/usr/bin/env python3
"""Federal CFR ingestion for ACE.

Ingests eCFR title XML into `core.documents` as citation-addressable
`cfr_section` / `cfr_appendix` documents with BGE-M3 chunks and authority
metadata. The script is deliberately resumable and idempotent: the canonical
CFR citation in `metadata.canonical_citation` is the logical key.

Important date behavior:
    eCFR's API rejects dates later than its current `titles.meta.date`.
    The project kickoff requested 2026-05-01, but the live API currently
    reports 2026-04-30. By default this script halts on that mismatch; pass
    `--use-available-date` to ingest the API-available date and store that
    actual value in metadata.ecfr_as_of.
"""
from __future__ import annotations

import argparse
import collections
import hashlib
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Optional

try:
    from dotenv import load_dotenv
    load_dotenv("/opt/wdws/.env")
except Exception:
    pass

import psycopg2
import psycopg2.extras
import requests
from lxml import etree

sys.path.insert(0, "/opt/wdws")
from embedding_service import embed_query_sync, embed_texts_sync, EMBEDDING_DIMENSIONS

assert EMBEDDING_DIMENSIONS == 1024, f"unexpected BGE-M3 dimension {EMBEDDING_DIMENSIONS}"


ROOT = Path("/opt/wdws")
DATA_DIR = ROOT / "data" / "cfr"
RAW_DIR = DATA_DIR / "raw"
DEBUG_DIR = DATA_DIR / "debug"
STATE_PATH = DATA_DIR / "ingestion_state.json"
LOG_PATH = DATA_DIR / "cfr_ingest.log"

API_BASE = "https://www.ecfr.gov/api/versioner/v1"
DEFAULT_REQUESTED_DATE = "2026-05-01"
USER_AGENT = "wdws-athena-cfr-ingest/1.0 (legal-research; contact: local-operator)"
DSN = os.environ.get("DATABASE_URL")
if not DSN:
    raise RuntimeError("DATABASE_URL is required; set it in /opt/wdws/.env or the environment")

EMBEDDING_MODEL_ID = int(os.environ.get("BGE_M3_MODEL_ID", "2"))
EMBED_BATCH_SIZE = int(os.environ.get("CFR_EMBED_BATCH_SIZE", "64"))

PHASES: dict[int, list[int]] = {
    0: [28, 29, 45],
    1: [20, 34, 42, 5, 2],
    2: [6, 24, 38, 41, 16, 26, 31, 44],
    3: [3, 1, 4, 11, 39, 37, 23, 13, 8, 27, 36, 22, 25, 43, 47, 10,
        30, 50, 14, 17, 18, 32, 33, 15, 19, 9, 46, 12, 21, 49, 48, 7, 40],
}

PHASE0_SPOT_CHECKS = [
    "28 C.F.R. § 35.101",
    "28 C.F.R. § 35.102",
    "28 C.F.R. § 35.104",
    "28 C.F.R. § 35.130",
    "28 C.F.R. § 35.136",
    "28 C.F.R. § 35.160",
    "28 C.F.R. § 35.164",
    "28 C.F.R. § 35.190",
    "29 C.F.R. § 1630.2",
    "29 C.F.R. § 1630.9",
    "45 C.F.R. § 84.3",
    "45 C.F.R. § 84.52",
]

RAG_SMOKE_TESTS = [
    ("reasonable modification disability public entity", "28 C.F.R. § 35.130"),
    ("effective communication auxiliary aids services", "28 C.F.R. § 35.160"),
    ("undue burden ADA determination factors", "28 C.F.R. § 35.164"),
    ("ADA major life activities definition", "29 C.F.R. § 1630.2"),
    ("interactive process reasonable accommodation", "29 C.F.R. § 1630.9"),
    ("section 504 qualified handicapped person", "45 C.F.R. § 84.3"),
]


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def log(msg: str) -> None:
    line = f"[{now_iso()}] {msg}"
    print(line, flush=True)
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def clean_text(text: str | None) -> str:
    if not text:
        return ""
    text = text.replace("\xa0", " ").replace("\u200b", "")
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
    text = re.sub(r"\s+([,.;:])", r"\1", text)
    return text.strip()


def local_name(elem: etree._Element) -> str:
    return etree.QName(elem).localname


def text_of(elem: etree._Element | None) -> str:
    if elem is None:
        return ""
    return clean_text("".join(elem.itertext()))


def strip_prefixed_heading(heading: str, prefixes: tuple[str, ...]) -> str:
    heading = clean_text(heading)
    for prefix in prefixes:
        if heading.upper().startswith(prefix):
            heading = heading[len(prefix):].strip()
            break
    heading = re.sub(r"^§\s*[\w.\-]+\s+", "", heading).strip()
    heading = re.sub(r"^(?:PART|Subpart|CHAPTER|SUBCHAPTER)\s+[\w.\-]+[—-]", "", heading, flags=re.I).strip()
    return clean_text(heading).rstrip(".")


def strip_auth_source_label(text: str, label: str) -> str:
    text = clean_text(text)
    return re.sub(rf"^{label}:\s*", "", text, flags=re.I).strip().rstrip(".")


@dataclass
class DivContext:
    tag: str
    n: Optional[str]
    type: Optional[str]
    elem: etree._Element
    heading: str = ""
    authority: str = ""
    source: str = ""


@dataclass
class CFRUnit:
    document_type: str
    title_number: int
    title_name: str
    chapter: Optional[str]
    chapter_heading: Optional[str]
    part_number: Optional[str]
    part_heading: Optional[str]
    subpart: Optional[str]
    subpart_heading: Optional[str]
    subject_group: Optional[str]
    section_number: str
    section_heading: str
    canonical_citation: str
    content: str
    authority: str
    authority_usc_citations: list[str]
    source_fr: str
    is_reserved: bool
    ecfr_as_of: str
    raw_xml: str

    @property
    def ace_title(self) -> str:
        suffix = self.section_heading.upper() if self.section_heading else "[RESERVED]"
        return f"{self.canonical_citation} — {suffix}"

    @property
    def metadata(self) -> dict[str, Any]:
        return {
            "type": self.document_type,
            "corpus": "federal-cfr",
            "ecfr_as_of": self.ecfr_as_of,
            "title_number": self.title_number,
            "title_name": self.title_name,
            "chapter": self.chapter,
            "chapter_heading": self.chapter_heading,
            "part_number": self.part_number,
            "part_heading": self.part_heading,
            "subpart": self.subpart,
            "subpart_heading": self.subpart_heading,
            "subject_group": self.subject_group,
            "section_number": self.section_number,
            "section_heading": self.section_heading,
            "canonical_citation": self.canonical_citation,
            "is_reserved": self.is_reserved,
            "authority": self.authority,
            "authority_usc_citations": self.authority_usc_citations,
            "source_fr": self.source_fr,
            "ingestion_date": now_iso()[:10],
        }


def load_state(path: Path = STATE_PATH) -> dict[str, Any]:
    if not path.exists():
        return {
            "started_at": now_iso(),
            "ecfr_snapshot_date": None,
            "phase": None,
            "completed_titles": [],
            "in_progress_title": None,
            "in_progress_part": None,
            "failed_titles": [],
            "completed_parts": {},
            "counts": {},
            "unresolved_authority_citations": [],
            "warnings": [],
        }
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        raise RuntimeError(f"state file is corrupted/unreadable: {path}: {e}")


def save_state(state: dict[str, Any], path: Path = STATE_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def request_json(url: str, *, timeout: int = 120) -> dict[str, Any]:
    r = requests.get(url, timeout=timeout, headers={"User-Agent": USER_AGENT})
    r.raise_for_status()
    return r.json()


def titles_payload() -> dict[str, Any]:
    return request_json(f"{API_BASE}/titles")


def resolve_snapshot_date(requested: str, *, use_available_date: bool) -> tuple[str, dict[str, Any]]:
    payload = titles_payload()
    available = payload.get("meta", {}).get("date")
    if not available:
        raise RuntimeError("could not determine eCFR available date from /titles metadata")
    if requested > available:
        msg = (
            f"Requested eCFR snapshot {requested} is not available; API meta.date is {available}. "
            "Re-run with --use-available-date to ingest the available date explicitly."
        )
        if not use_available_date:
            raise RuntimeError(msg)
        log("WARNING: " + msg)
        return available, payload
    return requested, payload


def active_titles(payload: dict[str, Any]) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    for item in payload.get("titles", []):
        if item.get("reserved"):
            continue
        out[int(item["number"])] = item
    return out


def expected_counts(snapshot_date: str, title: int) -> dict[str, int]:
    data = request_json(f"{API_BASE}/structure/{snapshot_date}/title-{title}.json", timeout=180)
    counts: collections.Counter[str] = collections.Counter()
    def walk(node: dict[str, Any]) -> None:
        counts[node.get("type") or "unknown"] += 1
        for child in node.get("children") or []:
            walk(child)
    walk(data)
    return {"sections": counts.get("section", 0), "appendices": counts.get("appendix", 0)}


def raw_xml_path(snapshot_date: str, title: int) -> Path:
    return RAW_DIR / snapshot_date / f"title-{title}.xml"


def download_title_xml(snapshot_date: str, title: int, *, force: bool = False) -> Path:
    path = raw_xml_path(snapshot_date, title)
    if path.exists() and path.stat().st_size > 1000 and not force:
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    url = f"{API_BASE}/full/{snapshot_date}/title-{title}.xml"
    last_err = None
    for attempt in range(1, 4):
        try:
            log(f"download title {title}: {url} (attempt {attempt}/3)")
            with requests.get(url, stream=True, timeout=300, headers={"User-Agent": USER_AGENT}) as r:
                r.raise_for_status()
                tmp = path.with_suffix(".xml.tmp")
                with tmp.open("wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
                if tmp.stat().st_size < 1000:
                    raise RuntimeError(f"downloaded XML is too small: {tmp.stat().st_size} bytes")
                tmp.replace(path)
            log(f"downloaded title {title}: {path} ({path.stat().st_size:,} bytes)")
            return path
        except Exception as e:
            last_err = e
            log(f"download title {title} failed: {e}")
            if attempt < 3:
                time.sleep(30)
    raise RuntimeError(f"failed to download title {title} after 3 attempts: {last_err}")


USC_GROUP_RE = re.compile(r"(\d+)\s+U\.\s*S\.\s*C\.?(?:\s*§+)?\s+([^;\n]+)", re.I)


def parse_usc_authority_citations(authority: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for m in USC_GROUP_RE.finditer(authority or ""):
        title = m.group(1)
        tail = m.group(2)
        tail = re.split(r"(?:;|\bunless\b|\bsubpart\b|\bpart\b)", tail, maxsplit=1, flags=re.I)[0]
        tail = tail.replace("and", ",")
        for raw in tail.split(","):
            sec = raw.strip().strip(" .")
            sec = re.sub(r"\bet\s+seq\.?", "", sec, flags=re.I).strip(" .")
            sec = re.sub(r"^§+\s*", "", sec)
            if not sec or not re.match(r"^[0-9][0-9A-Za-z.\-()]*$", sec):
                continue
            cit = f"{title} U.S.C. § {sec}"
            if cit not in seen:
                out.append(cit)
                seen.add(cit)
    return out


def render_element_body(elem: etree._Element) -> str:
    lines: list[str] = []

    def walk(node: etree._Element, *, depth: int = 0) -> None:
        for child in node:
            local = local_name(child)
            if local in {"HEAD", "AUTH", "SOURCE"}:
                continue
            txt = text_of(child)
            if not txt:
                walk(child, depth=depth + 1)
                continue
            if local.startswith("HD"):
                lines.append(f"## {txt}")
                lines.append("")
            elif local in {"P", "FP", "CITA", "NOTE", "EXTRACT", "PSPACE"}:
                lines.append(txt)
                lines.append("")
            elif local in {"GPH", "GPOTABLE", "TABLE"}:
                lines.append(txt)
                lines.append("")
            else:
                # Unknown block-ish nodes still carry official text.
                if len(txt) > 0 and txt not in lines[-2:]:
                    lines.append(txt)
                    lines.append("")
    walk(elem)
    return clean_text("\n".join(lines))


def appendix_citation(title: int, n: str, part_number: Optional[str]) -> str:
    n = clean_text(n)
    m = re.match(r"Appendix\s+(.+?)\s+to\s+Part\s+([\w.\-]+)$", n, re.I)
    if m:
        return f"{title} C.F.R. pt. {m.group(2)}, App. {m.group(1)}"
    m = re.match(r"Appendix\s+to\s+Subpart\s+(.+?)\s+of\s+Part\s+([\w.\-]+)$", n, re.I)
    if m:
        return f"{title} C.F.R. pt. {m.group(2)}, App. to Subpart {m.group(1)}"
    if part_number:
        return f"{title} C.F.R. pt. {part_number}, {n}"
    return f"{title} C.F.R. {n}"


def parse_cfr_xml(xml_path: Path, *, title_number: int, title_name: str, ecfr_as_of: str) -> Iterator[CFRUnit]:
    div_stack: list[DivContext] = []
    target_divs = {"DIV1", "DIV2", "DIV3", "DIV4", "DIV5", "DIV6", "DIV7", "DIV8", "DIV9"}
    context = etree.iterparse(str(xml_path), events=("start", "end"), recover=True, huge_tree=True)
    for event, elem in context:
        local = local_name(elem)
        if event == "start" and local in target_divs:
            div_stack.append(DivContext(local, elem.get("N"), elem.get("TYPE"), elem))
            continue

        if event != "end":
            continue

        if local == "HEAD":
            parent = elem.getparent()
            if parent is not None:
                for ctx in reversed(div_stack):
                    if ctx.elem is parent:
                        ctx.heading = text_of(elem)
                        break
            continue

        if local in {"AUTH", "SOURCE"}:
            raw = strip_auth_source_label(text_of(elem), "Authority" if local == "AUTH" else "Source")
            for ctx in reversed(div_stack):
                if ctx.tag == "DIV5":
                    if local == "AUTH":
                        ctx.authority = raw
                    else:
                        ctx.source = raw
                    break
            continue

        if local in {"DIV8", "DIV9"}:
            by_tag = {ctx.tag: ctx for ctx in div_stack}
            # Nearest contexts by type.
            chapter = next((c for c in reversed(div_stack) if c.tag == "DIV3"), None)
            part = next((c for c in reversed(div_stack) if c.tag == "DIV5"), None)
            subpart = next((c for c in reversed(div_stack) if c.tag == "DIV6"), None)
            subject = next((c for c in reversed(div_stack) if c.tag == "DIV7"), None)
            section_number = elem.get("N") or ""
            raw_heading = text_of(elem.find("HEAD"))
            body = render_element_body(elem)
            is_appendix = local == "DIV9"
            doc_type = "cfr_appendix" if is_appendix else "cfr_section"
            if is_appendix:
                canonical = appendix_citation(title_number, section_number, part.n if part else None)
                section_heading = strip_prefixed_heading(raw_heading, ("APPENDIX",)) or raw_heading
            else:
                canonical = f"{title_number} C.F.R. § {section_number}"
                section_heading = strip_prefixed_heading(raw_heading, ("§",)) or raw_heading
            reserved = bool(re.search(r"\[\s*Reserved\s*\]\s*$", raw_heading, re.I)) or not body.strip()
            display_heading = "[RESERVED]" if reserved and not section_heading else section_heading
            content_heading = f"{canonical} — {display_heading.upper() if display_heading else '[RESERVED]'}"
            content = f"[{content_heading}]\n\n{raw_heading}\n\n{body}".strip()
            authority = part.authority if part else ""
            yield CFRUnit(
                document_type=doc_type,
                title_number=title_number,
                title_name=title_name,
                chapter=chapter.n if chapter else None,
                chapter_heading=strip_prefixed_heading(chapter.heading, ("CHAPTER",)) if chapter else None,
                part_number=part.n if part else None,
                part_heading=strip_prefixed_heading(part.heading, ("PART",)) if part else None,
                subpart=subpart.n if subpart else None,
                subpart_heading=strip_prefixed_heading(subpart.heading, ("SUBPART",)) if subpart else None,
                subject_group=strip_prefixed_heading(subject.heading, ("SUBJECT GROUP",)) if subject else None,
                section_number=section_number,
                section_heading=display_heading or "[RESERVED]",
                canonical_citation=canonical,
                content=content,
                authority=authority,
                authority_usc_citations=parse_usc_authority_citations(authority),
                source_fr=part.source if part else "",
                is_reserved=reserved,
                ecfr_as_of=ecfr_as_of,
                raw_xml=etree.tostring(elem, encoding="unicode", pretty_print=False),
            )

        if local in target_divs:
            if div_stack and div_stack[-1].elem is elem:
                div_stack.pop()
            elem.clear()
            parent = elem.getparent()
            if parent is not None:
                while elem.getprevious() is not None:
                    del parent[0]


def chunk_content(unit: CFRUnit) -> list[str]:
    words = unit.content.split()
    if len(words) <= 400:
        return [unit.content]
    paras = [p.strip() for p in unit.content.split("\n\n") if p.strip()]
    target = 600 if len(words) <= 1200 else 400
    overlap = 100
    chunks: list[str] = []
    buf: list[str] = []
    buf_words = 0
    prefix = f"[{unit.canonical_citation} — {unit.section_heading.upper()}]"
    for para in paras:
        n = len(para.split())
        if buf and buf_words + n > target:
            chunks.append(prefix + "\n\n" + "\n\n".join(buf))
            overlap_words = " ".join("\n\n".join(buf).split()[-overlap:])
            buf = [overlap_words] if overlap_words else []
            buf_words = len(overlap_words.split())
        if n > target * 2:
            tokens = para.split()
            start = 0
            while start < len(tokens):
                part = " ".join(tokens[start:start + target])
                chunks.append(prefix + "\n\n" + part)
                start += max(1, target - overlap)
            buf = []
            buf_words = 0
        else:
            buf.append(para)
            buf_words += n
    if buf:
        chunks.append(prefix + "\n\n" + "\n\n".join(buf))
    return [c.strip() for c in chunks if c.strip()]


def vec_literal(vec: list[float]) -> str:
    return "[" + ",".join(f"{x:.7f}" for x in vec) + "]"


def db_conn():
    conn = psycopg2.connect(DSN)
    conn.autocommit = False
    return conn


def ensure_db_support(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS uniq_documents_cfr_citation
              ON core.documents ((metadata->>'canonical_citation'))
              WHERE document_type IN ('cfr_section', 'cfr_appendix')
                AND metadata->>'corpus' = 'federal-cfr'
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_documents_cfr_title
              ON core.documents (((metadata->>'title_number')::int))
              WHERE document_type IN ('cfr_section', 'cfr_appendix')
                AND metadata->>'corpus' = 'federal-cfr'
        """)
    conn.commit()


def find_existing_doc(conn, canonical: str) -> Optional[tuple[str, Optional[str], Optional[str]]]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id::text, metadata->>'ecfr_as_of', content_hash
            FROM core.documents
            WHERE document_type IN ('cfr_section', 'cfr_appendix')
              AND metadata @> %s::jsonb
            LIMIT 1
        """, (json.dumps({"corpus": "federal-cfr", "canonical_citation": canonical}),))
        row = cur.fetchone()
    return row if row else None


def upsert_unit(conn, unit: CFRUnit, *, force: bool = False, skip_embeddings: bool = False) -> tuple[str, str, int]:
    content_hash = hashlib.sha256(unit.content.encode("utf-8", errors="ignore")).hexdigest()
    existing = find_existing_doc(conn, unit.canonical_citation)
    if existing and existing[1] == unit.ecfr_as_of and existing[2] == content_hash and not force:
        return existing[0], "skipped", 0
    chunks = chunk_content(unit)
    source_key = hashlib.sha1(unit.canonical_citation.encode("utf-8")).hexdigest()[:16]
    source_path = f"cfr:{unit.ecfr_as_of}:t{unit.title_number}:{source_key}"
    filename = f"cfr-title-{unit.title_number}-{source_key}.xml"
    with conn.cursor() as cur:
        if existing:
            doc_id = existing[0]
            cur.execute("""
                UPDATE core.documents
                   SET domain='legal', document_type=%s, title=%s, source_path=%s,
                       filename=%s, full_content=%s, raw_content=%s, content_hash=%s,
                       metadata=%s::jsonb, classification='public', confidentiality='public',
                       legal_hold=FALSE, privilege='none', privilege_classified_at=NOW(),
                       privilege_classified_by='rule:cfr_primary_source_public',
                       total_chunks=%s, updated_at=NOW()
                 WHERE id=%s
            """, (unit.document_type, unit.ace_title, source_path, filename, unit.content,
                  unit.raw_xml, content_hash, json.dumps(unit.metadata), len(chunks), doc_id))
            status = "updated"
        else:
            cur.execute("""
                INSERT INTO core.documents
                    (domain, document_type, title, source_path, filename,
                     full_content, raw_content, content_hash, metadata,
                     classification, confidentiality, legal_hold,
                     privilege, privilege_classified_at, privilege_classified_by,
                     total_chunks)
                VALUES
                    ('legal', %s, %s, %s, %s,
                     %s, %s, %s, %s::jsonb,
                     'public', 'public', FALSE,
                     'none', NOW(), 'rule:cfr_primary_source_public',
                     %s)
                RETURNING id::text
            """, (unit.document_type, unit.ace_title, source_path, filename,
                  unit.content, unit.raw_xml, content_hash, json.dumps(unit.metadata), len(chunks)))
            doc_id = cur.fetchone()[0]
            status = "inserted"
        cur.execute("DELETE FROM core.document_chunks WHERE document_id=%s", (doc_id,))
        rows = []
        for i, text in enumerate(chunks):
            rows.append((f"{doc_id}:{i}", doc_id, i, len(chunks), text,
                         EMBEDDING_MODEL_ID, len(text.split()), text,
                         json.dumps({"corpus": "federal-cfr", "canonical_citation": unit.canonical_citation})))
        if rows:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO core.document_chunks
                    (id, document_id, chunk_index, total_chunks, content,
                     embedding_model_id, token_count, embedded_content, metadata)
                VALUES %s
            """, rows)
    if not skip_embeddings and chunks:
        embed_doc_chunks(conn, doc_id)
    return doc_id, status, len(chunks)


def embed_doc_chunks(conn, doc_id: str) -> int:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, content FROM core.document_chunks
            WHERE document_id=%s AND embedding IS NULL
            ORDER BY chunk_index
        """, (doc_id,))
        rows = cur.fetchall()
    if not rows:
        return 0
    embedded = 0
    for start in range(0, len(rows), EMBED_BATCH_SIZE):
        batch = rows[start:start + EMBED_BATCH_SIZE]
        ids = [r[0] for r in batch]
        texts = [r[1] for r in batch]
        vecs = embed_texts_sync(texts)
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, """
                UPDATE core.document_chunks
                   SET embedding=%s::halfvec(1024), embedded_at=NOW()
                 WHERE id=%s
            """, [(vec_literal(v), cid) for cid, v in zip(ids, vecs)], page_size=64)
        conn.commit()
        embedded += len(batch)
    return embedded


def embed_pending_for_title(conn, title: int, *, limit: Optional[int] = None) -> int:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c.id, c.content
            FROM core.document_chunks c
            JOIN core.documents d ON d.id = c.document_id
            WHERE d.document_type IN ('cfr_section', 'cfr_appendix')
              AND d.metadata @> %s::jsonb
              AND c.embedding IS NULL
            ORDER BY d.title, c.chunk_index
            LIMIT %s
        """, (json.dumps({"corpus": "federal-cfr", "title_number": title}), limit))
        rows = cur.fetchall()
    conn.commit()
    done = 0
    for start in range(0, len(rows), EMB_BATCH_SIZE := EMBED_BATCH_SIZE):
        batch = rows[start:start + EMB_BATCH_SIZE]
        vecs = embed_texts_sync([r[1] for r in batch])
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, """
                UPDATE core.document_chunks
                   SET embedding=%s::halfvec(1024), embedded_at=NOW()
                 WHERE id=%s
            """, [(vec_literal(v), r[0]) for r, v in zip(batch, vecs)], page_size=64)
        conn.commit()
        done += len(batch)
        log(f"embedded title {title}: {done}/{len(rows)} chunks")
    return done


def register_authority_relationships(conn, *, title: int) -> dict[str, Any]:
    status = {"registered": 0, "unresolved": []}
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id::text, metadata->>'authority', metadata->'authority_usc_citations'
            FROM core.documents
            WHERE document_type = 'cfr_section'
              AND metadata @> %s::jsonb
              AND jsonb_array_length(COALESCE(metadata->'authority_usc_citations', '[]'::jsonb)) > 0
        """, (json.dumps({"corpus": "federal-cfr", "title_number": title}),))
        rows = cur.fetchall()
        for cfr_doc_id, authority, citations_json in rows:
            citations = citations_json or []
            for usc_cit in citations:
                cur.execute("""
                    SELECT d.id::text
                    FROM usc.sections s
                    JOIN core.documents d ON d.id = s.document_id
                    WHERE s.canonical_citation = %s
                    LIMIT 1
                """, (usc_cit,))
                found = cur.fetchone()
                if not found:
                    status["unresolved"].append({"cfr_doc_id": cfr_doc_id, "usc_citation": usc_cit})
                    continue
                target_id = found[0]
                cur.execute("""
                    INSERT INTO core.document_relationships
                        (source_document_id, target_document_id, relationship_type, confidence, metadata)
                    VALUES (%s, %s, 'authorized_by', 1.0, %s::jsonb)
                    ON CONFLICT (source_document_id, target_document_id, relationship_type)
                    DO UPDATE SET confidence=EXCLUDED.confidence, metadata=EXCLUDED.metadata
                """, (cfr_doc_id, target_id, json.dumps({
                    "authority_text": authority,
                    "authority_citation": usc_cit,
                    "registered_by": "cfr_ingest.py",
                    "registered_at": now_iso(),
                })))
                status["registered"] += 1
    conn.commit()
    return status


def ingest_title(title: int, *, snapshot_date: str, title_name: str, force_download: bool,
                 force: bool, skip_embeddings: bool, limit: Optional[int], resume: bool,
                 state: dict[str, Any]) -> dict[str, Any]:
    expected = expected_counts(snapshot_date, title)
    xml_path = download_title_xml(snapshot_date, title, force=force_download)
    summary = {"inserted": 0, "updated": 0, "skipped": 0, "chunks": 0, "appendices": 0}
    completed_parts = set((state.get("completed_parts") or {}).get(str(title), [])) if resume else set()
    current_part = None
    units_seen = 0
    conn = db_conn()
    try:
        ensure_db_support(conn)
        for unit in parse_cfr_xml(xml_path, title_number=title, title_name=title_name, ecfr_as_of=snapshot_date):
            if limit and units_seen >= limit:
                break
            units_seen += 1
            if current_part is not None and unit.part_number != current_part:
                state.setdefault("completed_parts", {}).setdefault(str(title), [])
                if current_part not in state["completed_parts"][str(title)]:
                    state["completed_parts"][str(title)].append(current_part)
                state["in_progress_part"] = unit.part_number
                save_state(state)
                conn.commit()
            current_part = unit.part_number
            if resume and unit.part_number in completed_parts:
                summary["skipped"] += 1
                continue
            _, status, n_chunks = upsert_unit(conn, unit, force=force, skip_embeddings=skip_embeddings)
            summary[status] = summary.get(status, 0) + 1
            summary["chunks"] += n_chunks
            if unit.document_type == "cfr_appendix":
                summary["appendices"] += 1
            if units_seen % 100 == 0:
                conn.commit()
                log(f"title {title}: processed {units_seen}; summary={summary}")
        conn.commit()
        if current_part:
            state.setdefault("completed_parts", {}).setdefault(str(title), [])
            if current_part not in state["completed_parts"][str(title)]:
                state["completed_parts"][str(title)].append(current_part)
        if not skip_embeddings:
            embed_pending_for_title(conn, title)
        xref_status = register_authority_relationships(conn, title=title)
        actual = verify_title(conn, title, expected_sections=None if limit else expected["sections"])
        summary.update({"expected": expected, "actual": actual, "authority_xrefs": xref_status})
        if not limit and actual["sections"] < expected["sections"] * 0.95:
            raise RuntimeError(f"title {title} more than 5% below structure count: {actual} vs {expected}")
        return summary
    finally:
        conn.close()


def verify_title(conn, title: int, *, expected_sections: Optional[int] = None) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FILTER (WHERE document_type='cfr_section') AS sections,
                   COUNT(*) FILTER (WHERE document_type='cfr_appendix') AS appendices,
                   COUNT(*) FILTER (WHERE document_type='cfr_section' AND (metadata->>'is_reserved')::boolean) AS reserved,
                   COUNT(DISTINCT metadata->>'part_number') FILTER (WHERE document_type='cfr_section') AS parts
            FROM core.documents
            WHERE document_type IN ('cfr_section', 'cfr_appendix')
              AND metadata @> %s::jsonb
        """, (json.dumps({"corpus": "federal-cfr", "title_number": title}),))
        row = cur.fetchone()
    out = {"sections": row[0] or 0, "appendices": row[1] or 0, "reserved": row[2] or 0, "parts": row[3] or 0}
    if expected_sections is not None:
        out["expected_sections"] = expected_sections
        out["variance"] = out["sections"] - expected_sections
    return out


def validate_phase0(conn) -> dict[str, Any]:
    report: dict[str, Any] = {"spot_checks": {}, "rag": []}
    with conn.cursor() as cur:
        cur.execute("""
            SELECT metadata->>'canonical_citation', COUNT(*)
            FROM core.documents
            WHERE document_type='cfr_section'
              AND metadata->>'canonical_citation' = ANY(%s)
            GROUP BY metadata->>'canonical_citation'
        """, (PHASE0_SPOT_CHECKS,))
        counts = dict(cur.fetchall())
    failures = []
    for citation in PHASE0_SPOT_CHECKS:
        n = counts.get(citation, 0)
        report["spot_checks"][citation] = n
        if n != 1:
            failures.append((citation, n))
    if failures:
        raise RuntimeError(f"Phase 0 spot-check failures: {failures}")
    for question, expected in RAG_SMOKE_TESTS:
        citations = rag_search_cfr(conn, question, top_k=8)
        ok = expected in citations
        report["rag"].append({"question": question, "expected": expected, "citations": citations, "ok": ok})
        if not ok:
            raise RuntimeError(f"Phase 0 RAG smoke failed: {question!r} expected {expected}, got {citations}")
    return report


def rag_search_cfr(conn, question: str, *, top_k: int = 8) -> list[str]:
    qvec = embed_query_sync(question)
    qlit = vec_literal(qvec)
    fetch_count = top_k * 4
    with conn.cursor() as cur:
        cur.execute("SET hnsw.iterative_scan = strict_order")
        cur.execute("""
            SELECT c.id, d.id::text, d.metadata->>'canonical_citation',
                   (c.embedding::halfvec(1024) <=> %s::halfvec(1024)) AS distance
            FROM core.document_chunks c
            JOIN core.documents d ON d.id = c.document_id
            WHERE c.embedding IS NOT NULL
              AND d.document_type='cfr_section'
              AND d.metadata->>'corpus'='federal-cfr'
            ORDER BY c.embedding::halfvec(1024) <=> %s::halfvec(1024)
            LIMIT %s
        """, (qlit, qlit, fetch_count))
        sem = cur.fetchall()
        cur.execute("""
            SELECT c.id, d.id::text, d.metadata->>'canonical_citation',
                   ts_rank(c.content_tsv, websearch_to_tsquery('english', %s)) AS rank
            FROM core.document_chunks c
            JOIN core.documents d ON d.id = c.document_id
            WHERE c.content_tsv @@ websearch_to_tsquery('english', %s)
              AND d.document_type='cfr_section'
              AND d.metadata->>'corpus'='federal-cfr'
            ORDER BY rank DESC
            LIMIT %s
        """, (question, question, fetch_count))
        ft = cur.fetchall()
    scores: dict[str, float] = {}
    data: dict[str, tuple[str, str]] = {}
    for rank, row in enumerate(sem):
        scores[row[0]] = scores.get(row[0], 0.0) + 0.7 / (60 + rank + 1)
        data.setdefault(row[0], (row[1], row[2]))
    for rank, row in enumerate(ft):
        scores[row[0]] = scores.get(row[0], 0.0) + 0.3 / (60 + rank + 1)
        data.setdefault(row[0], (row[1], row[2]))
    seen_docs: set[str] = set()
    out: list[str] = []
    for chunk_id, _ in sorted(scores.items(), key=lambda kv: kv[1], reverse=True):
        doc_id, citation = data[chunk_id]
        if doc_id in seen_docs:
            continue
        seen_docs.add(doc_id)
        out.append(citation)
        if len(out) >= top_k:
            break
    return out


def write_completion_note(conn, *, snapshot_date: str, state: dict[str, Any]) -> int:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT (metadata->>'title_number')::int AS title,
                   metadata->>'title_name' AS name,
                   COUNT(*) FILTER (WHERE document_type='cfr_section') AS sections,
                   COUNT(*) FILTER (WHERE document_type='cfr_section' AND (metadata->>'is_reserved')::boolean) AS reserved,
                   COUNT(*) FILTER (WHERE document_type='cfr_appendix') AS appendices
            FROM core.documents
            WHERE document_type IN ('cfr_section', 'cfr_appendix')
              AND metadata->>'corpus'='federal-cfr'
            GROUP BY 1, 2
            ORDER BY 1
        """)
        counts = [dict(title=r[0], name=r[1], sections=r[2], reserved=r[3], appendices=r[4]) for r in cur.fetchall()]
        cur.execute("""
            SELECT COUNT(*) FROM core.document_relationships
            WHERE relationship_type='authorized_by'
              AND metadata->>'registered_by'='cfr_ingest.py'
        """)
        xrefs = cur.fetchone()[0]
        content = json.dumps({
            "snapshot_date": snapshot_date,
            "counts_by_title": counts,
            "total_sections": sum(r["sections"] for r in counts),
            "total_appendices": sum(r["appendices"] for r in counts),
            "cross_references_registered": xrefs,
            "failed_titles": state.get("failed_titles", []),
            "unresolved_authority_citations": state.get("unresolved_authority_citations", []),
            "started_at": state.get("started_at"),
            "completed_at": now_iso(),
        }, indent=2)
        cur.execute("""
            INSERT INTO ops.agent_notes
                (agent_id, note_type, title, content, tags, priority, status, metadata)
            VALUES ('athena', 'finding', %s, %s, %s, 3, 'closed', %s::jsonb)
            RETURNING id
        """, (
            "CFR ingestion complete",
            content,
            ["cfr-ingestion-complete", "federal-cfr"],
            json.dumps({"tag": "cfr-ingestion-complete", "snapshot_date": snapshot_date}),
        ))
        note_id = cur.fetchone()[0]
    conn.commit()
    return int(note_id)


def titles_for_args(args, active: dict[int, dict[str, Any]]) -> list[int]:
    if args.title:
        return args.title
    if args.phase is not None:
        return [t for t in PHASES[args.phase] if t in active]
    if args.all_titles:
        return [t for t in sorted(active) if t != 35]
    return []


def main() -> int:
    ap = argparse.ArgumentParser(description="Ingest eCFR CFR titles into ACE")
    ap.add_argument("--snapshot-date", default=DEFAULT_REQUESTED_DATE)
    ap.add_argument("--use-available-date", action="store_true",
                    help="if requested date is newer than eCFR meta.date, ingest meta.date explicitly")
    ap.add_argument("--title", type=int, action="append", help="title number to ingest; repeatable")
    ap.add_argument("--phase", type=int, choices=sorted(PHASES), help="phase number to ingest")
    ap.add_argument("--all-titles", action="store_true")
    ap.add_argument("--force-download", action="store_true")
    ap.add_argument("--force", action="store_true", help="rewrite same-date documents/chunks")
    ap.add_argument("--skip-embeddings", action="store_true", help="write docs/chunks only; embed later")
    ap.add_argument("--embed-title", type=int, help="embed pending CFR chunks for one title")
    ap.add_argument("--verify-title", type=int, help="verify one title count against DB")
    ap.add_argument("--validate-phase0", action="store_true")
    ap.add_argument("--register-xrefs-title", type=int)
    ap.add_argument("--completion-note", action="store_true")
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--limit", type=int, help="smoke-test limit of units per title")
    args = ap.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)

    snapshot_date, payload = resolve_snapshot_date(args.snapshot_date, use_available_date=args.use_available_date)
    active = active_titles(payload)
    state = load_state()
    state["ecfr_snapshot_date"] = snapshot_date
    save_state(state)

    if args.verify_title:
        exp = expected_counts(snapshot_date, args.verify_title)
        conn = db_conn(); conn.autocommit = True
        try:
            print(json.dumps(verify_title(conn, args.verify_title, expected_sections=exp["sections"]), indent=2))
        finally:
            conn.close()
        return 0

    if args.embed_title:
        conn = db_conn()
        try:
            ensure_db_support(conn)
            embed_pending_for_title(conn, args.embed_title)
        finally:
            conn.close()
        return 0

    if args.register_xrefs_title:
        conn = db_conn()
        try:
            status = register_authority_relationships(conn, title=args.register_xrefs_title)
            print(json.dumps(status, indent=2))
        finally:
            conn.close()
        return 0

    if args.validate_phase0:
        conn = db_conn(); conn.autocommit = True
        try:
            print(json.dumps(validate_phase0(conn), indent=2))
        finally:
            conn.close()
        return 0

    if args.completion_note:
        conn = db_conn()
        try:
            note_id = write_completion_note(conn, snapshot_date=snapshot_date, state=state)
            print(f"completion note id={note_id}")
        finally:
            conn.close()
        return 0

    titles = titles_for_args(args, active)
    if not titles:
        ap.print_help()
        return 0

    for i, title in enumerate(titles):
        title_meta = active.get(title)
        if not title_meta:
            log(f"skip title {title}: not active or reserved")
            continue
        if i > 0:
            time.sleep(5)  # politeness throttle between full-title downloads
        state["phase"] = args.phase
        state["in_progress_title"] = title
        state["in_progress_part"] = None
        save_state(state)
        try:
            summary = ingest_title(
                title,
                snapshot_date=snapshot_date,
                title_name=title_meta["name"],
                force_download=args.force_download,
                force=args.force,
                skip_embeddings=args.skip_embeddings,
                limit=args.limit,
                resume=args.resume,
                state=state,
            )
            state.setdefault("counts", {})[str(title)] = {
                "expected": summary["expected"],
                "actual": summary["actual"],
                "completed_at": now_iso(),
                "summary": {k: v for k, v in summary.items() if k not in {"expected", "actual", "authority_xrefs"}},
            }
            if title not in state.setdefault("completed_titles", []):
                state["completed_titles"].append(title)
            unresolved = summary.get("authority_xrefs", {}).get("unresolved", [])
            state.setdefault("unresolved_authority_citations", []).extend(unresolved[:500])
            log(f"title {title} complete: {json.dumps(summary, default=str)[:2000]}")
        except Exception as e:
            state.setdefault("failed_titles", []).append({"title": title, "error": str(e), "at": now_iso()})
            save_state(state)
            raise
        finally:
            state["in_progress_title"] = None
            state["in_progress_part"] = None
            save_state(state)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())