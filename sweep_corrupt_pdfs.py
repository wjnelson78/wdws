#!/usr/bin/env python3
"""sweep_corrupt_pdfs.py — fleet-wide detection of corrupt PDF attachments.

For every PDF attachment row in legal.email_attachments, check the on-disk
binary at metadata.local_attachment_path. Two signals:

  (a) pdfinfo FAILS on the binary → physically corrupt
  (b) the binary's filename has a `_<digit>_<hex>.pdf` dedup suffix AND a
      sibling without the suffix exists in the archive AND is meaningfully
      larger → likely Outlook-reply truncation (current extraction may be
      incomplete even if non-empty)

For each suspect, search the mailbox archive for sibling clean copies
(same base filename, valid pdfinfo, larger size). Report scope + match rate.
With --apply, run backfill_unextracted_attachments.py logic to re-extract
from the clean copy.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import asyncpg

sys.path.insert(0, "/opt/wdws")

from email_sync import extract_text_from_binary  # noqa: E402
from ingest import TextChunker  # noqa: E402
from embedding_service import (  # noqa: E402
    embed_texts_sync,
    get_or_create_embedding_model_id,
    _vec_literal as _embedding_vec_literal,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("sweep")

DATABASE_URL = os.environ["DATABASE_URL"]

ATTACHMENTS_ROOT = Path("/opt/wdws/data/emails/attachments")

# Dedup suffix pattern: filename ends with "_<digit>_<hex>.pdf"
DEDUP_SUFFIX_RE = re.compile(r"_\d+_[0-9a-f]+(\.pdf)$", re.IGNORECASE)


@dataclass
class Suspect:
    att_id: uuid.UUID
    child_doc_id: Optional[uuid.UUID]
    mailbox: str
    filename: str
    local_path: str
    db_size: int
    on_disk_size: int
    pdfinfo_ok: bool
    has_dedup_suffix: bool
    extracted_text_len: int
    clean_sibling_path: Optional[str] = None
    clean_sibling_size: int = 0


def pdfinfo_ok(path: Path) -> bool:
    """Returns True if pdfinfo succeeds (binary has valid xref/trailer)."""
    try:
        r = subprocess.run(
            ["pdfinfo", str(path)],
            capture_output=True, timeout=10,
        )
        if r.returncode != 0:
            return False
        # Some malformed PDFs return 0 but emit Syntax Error on stderr
        stderr = r.stderr.decode("utf-8", errors="ignore")
        if "Syntax Error" in stderr and "trailer dictionary" in stderr:
            return False
        return True
    except Exception:
        return False


def base_filename_for_search(filename: str) -> str:
    """Strip dedup `_N_<hex>` suffix. 'foo_1_2fad4f9dd9.pdf' → 'foo.pdf'."""
    m = DEDUP_SUFFIX_RE.search(filename)
    if m:
        return DEDUP_SUFFIX_RE.sub(r"\1", filename)
    return filename


def find_clean_siblings(mailbox_dir: Path, target_basename: str) -> list[tuple[Path, int]]:
    """Search the mailbox archive for files matching target_basename. Return
    (path, size) tuples ranked by size desc, valid pdfinfo only.

    First scans the same mailbox; if no VALID candidates found after
    filtering for pdfinfo health, falls back to cross-mailbox search.
    """
    def collect(root: Path) -> list[tuple[Path, int]]:
        out: list[tuple[Path, int]] = []
        if not root.exists():
            return out
        for p in root.rglob(target_basename):
            try:
                size = p.stat().st_size
            except OSError:
                continue
            out.append((p, size))
        return out

    primary = [(p, s) for (p, s) in collect(mailbox_dir) if pdfinfo_ok(p)]
    if primary:
        primary.sort(key=lambda x: -x[1])
        return primary

    # No clean copy in the same mailbox — search siblings (e.g., athena@ ↔ william@)
    cross: list[tuple[Path, int]] = []
    if mailbox_dir.parent.exists():
        for sibling_mbox_dir in mailbox_dir.parent.iterdir():
            if sibling_mbox_dir == mailbox_dir or not sibling_mbox_dir.is_dir():
                continue
            cross.extend(collect(sibling_mbox_dir))
    cross = [(p, s) for (p, s) in cross if pdfinfo_ok(p)]
    cross.sort(key=lambda x: -x[1])
    return cross


def mailbox_archive_dir(mailbox: str) -> Path:
    """Map mailbox 'foo@bar.com' → /opt/wdws/data/emails/attachments/foo_bar.com"""
    safe = mailbox.replace("@", "_").replace(":", "_")
    return ATTACHMENTS_ROOT / safe


async def sweep(conn, sample_limit: Optional[int] = None) -> list[Suspect]:
    """Walk every PDF attachment, classify suspects."""
    rows = await conn.fetch(
        """
        SELECT ea.id AS att_id,
               ea.child_doc_id,
               em.mailbox,
               ea.filename,
               ea.file_size AS db_size,
               LENGTH(COALESCE(ea.extracted_text,'')) AS extracted_text_len,
               cd.metadata->>'local_attachment_path' AS local_path
        FROM legal.email_attachments ea
        JOIN legal.email_metadata em ON em.document_id = ea.email_doc_id
        JOIN core.documents cd ON cd.id = ea.child_doc_id
        WHERE ea.content_type = 'application/pdf'
        ORDER BY ea.created_at
        """ + (f"\n        LIMIT {int(sample_limit)}" if sample_limit else "")
    )

    suspects: list[Suspect] = []
    checked = 0
    for r in rows:
        checked += 1
        if checked % 200 == 0:
            log.info("  ...checked %d/%d", checked, len(rows))
        local_path = r["local_path"]
        if not local_path:
            continue
        p = Path(local_path)
        if not p.exists():
            continue
        try:
            on_disk = p.stat().st_size
        except OSError:
            continue
        ok = pdfinfo_ok(p)
        has_suffix = bool(DEDUP_SUFFIX_RE.search(p.name))
        is_corrupt = not ok
        # Suspect criteria: physically corrupt OR (dedup suffix AND extracted_text is short)
        # We focus on physical corruption first; the suffix-pattern alone is too noisy.
        if not is_corrupt:
            continue
        s = Suspect(
            att_id=r["att_id"],
            child_doc_id=r["child_doc_id"],
            mailbox=r["mailbox"],
            filename=r["filename"],
            local_path=local_path,
            db_size=r["db_size"] or 0,
            on_disk_size=on_disk,
            pdfinfo_ok=ok,
            has_dedup_suffix=has_suffix,
            extracted_text_len=r["extracted_text_len"] or 0,
        )
        # Search for a clean sibling
        target = base_filename_for_search(p.name)
        siblings = find_clean_siblings(mailbox_archive_dir(r["mailbox"]), target)
        if siblings:
            s.clean_sibling_path = str(siblings[0][0])
            s.clean_sibling_size = siblings[0][1]
        suspects.append(s)

    log.info("Sweep complete: scanned %d PDF attachments, %d corrupt", checked, len(suspects))
    return suspects


async def reprocess_suspect(
    conn,
    s: Suspect,
    chunker: TextChunker,
    model_id: int,
) -> dict:
    """Backfill a suspect from its clean sibling. Returns result dict."""
    if not s.clean_sibling_path:
        return {"att_id": str(s.att_id), "status": "no_clean_sibling",
                "chars": 0, "method": "", "chunks": 0}

    src = Path(s.clean_sibling_path)
    data = src.read_bytes()
    text, method = extract_text_from_binary(s.filename, "application/pdf", data)
    if not (text or "").strip():
        return {"att_id": str(s.att_id), "status": "extract_empty",
                "chars": 0, "method": method, "chunks": 0}

    truncated = text[:100000]
    async with conn.transaction():
        await conn.execute(
            """
            UPDATE legal.email_attachments
               SET extracted_text = $2,
                   extraction_method = $3,
                   is_processed = TRUE
             WHERE id = $1
            """,
            s.att_id, truncated, method,
        )
        await conn.execute(
            """
            UPDATE core.documents
               SET full_content = $2,
                   content_hash = $3,
                   metadata = COALESCE(metadata, '{}'::jsonb) ||
                              jsonb_build_object(
                                'extraction_method', $4::text,
                                'reextracted_at', now()::text,
                                'reextract_source_path', $5::text,
                                'reextract_reason', 'corrupt_binary_sweep_2026_04_28'::text
                              ),
                   updated_at = now()
             WHERE id = $1
            """,
            s.child_doc_id, text,
            hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest(),
            method, str(src),
        )
        await conn.execute(
            "DELETE FROM core.document_chunks WHERE document_id = $1", s.child_doc_id
        )

        chunks = chunker.split(text)
        embeddings = embed_texts_sync(chunks)
        row = await conn.fetchrow(
            "SELECT source_path FROM core.documents WHERE id = $1", s.child_doc_id
        )
        source_path = row["source_path"] if row else str(s.child_doc_id)
        total = len(chunks)
        for i, (t, emb) in enumerate(zip(chunks, embeddings)):
            chunk_id = hashlib.md5(f"{source_path}:{i}".encode()).hexdigest()
            emb_param = _embedding_vec_literal(emb) if emb is not None else None
            meta = {
                "source_part": "attachment_body",
                "is_embedded": True,
                "case_numbers": [],
                "backfill": "corrupt_pdf_sweep_2026_04_28",
            }
            await conn.execute(
                """
                INSERT INTO core.document_chunks
                    (id, document_id, chunk_index, total_chunks,
                     content, embedded_content, embedding, embedding_model_id, metadata)
                VALUES ($1, $2, $3, $4, $5, $6, $7::halfvec, $8, $9::jsonb)
                ON CONFLICT (id) DO UPDATE SET
                    content = EXCLUDED.content,
                    embedding = EXCLUDED.embedding,
                    metadata = EXCLUDED.metadata
                """,
                chunk_id, s.child_doc_id, i, total, t, None,
                emb_param, model_id, json.dumps(meta),
            )
        await conn.execute(
            "UPDATE core.documents SET total_chunks = $2 WHERE id = $1",
            s.child_doc_id, total,
        )
    return {"att_id": str(s.att_id), "status": "ok",
            "chars": len(text), "method": method, "chunks": total}


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true",
                        help="Re-extract suspects from clean siblings (writes DB)")
    parser.add_argument("--sample-limit", type=int, default=None,
                        help="Scan only first N PDF rows (for quick sanity checks)")
    args = parser.parse_args()

    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=2)
    chunker = TextChunker()

    async with pool.acquire() as conn:
        suspects = await sweep(conn, args.sample_limit)

        log.info("\n=== SUSPECT REPORT ===")
        log.info("Total suspects (corrupt on-disk): %d", len(suspects))
        with_sibling = [s for s in suspects if s.clean_sibling_path]
        without = [s for s in suspects if not s.clean_sibling_path]
        log.info("  with clean sibling: %d", len(with_sibling))
        log.info("  without clean sibling: %d", len(without))

        for s in suspects:
            log.info(
                "  %s  %s | %s | db_size=%d on_disk=%d  text_len=%d  sibling=%s (%d B)",
                s.att_id, s.mailbox[:32], s.filename[:50],
                s.db_size, s.on_disk_size, s.extracted_text_len,
                "YES" if s.clean_sibling_path else "no",
                s.clean_sibling_size,
            )

        if args.apply and with_sibling:
            log.info("\n=== APPLYING ===")
            model_id = await get_or_create_embedding_model_id(conn)
            results = []
            for s in with_sibling:
                try:
                    r = await reprocess_suspect(conn, s, chunker, model_id)
                except Exception as e:
                    log.exception("reprocess failed: %s", s.att_id)
                    r = {"att_id": str(s.att_id), "status": "error",
                         "chars": 0, "method": "", "chunks": 0, "error": str(e)[:200]}
                log.info("  %s  status=%s chars=%d chunks=%d  %s",
                         r["att_id"], r["status"], r["chars"], r["chunks"], s.filename[:50])
                results.append(r)
            by_status = {}
            for r in results:
                by_status[r["status"]] = by_status.get(r["status"], 0) + 1
            log.info("Apply status counts: %s", by_status)

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
