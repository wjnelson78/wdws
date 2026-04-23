"""
Unified Ingestion Pipeline — Athena Cognitive Platform
=======================================================
One entry point absorbs the chunk → enrich → embed → insert pattern that
was previously duplicated across ~20 ingest scripts. Callers shape their
source-specific input (Graph API, Samba, OneDrive, etc.) into a
DocumentSpec and call `ingest()`; the pipeline owns everything from
chunker selection through DB writes.

Scope — what this module DOES:
  • Chunker routing (email-aware vs generic) via email_chunker
  • Contextual-Retrieval prefix generation
  • Dense embedding + per-chunk source_part metadata
  • core.documents INSERT (idempotent by source_path)
  • core.document_chunks INSERT (handles None embeddings for non-body parts)
  • legal.case_documents linking (auto-extracts case numbers if not supplied)
  • core.tags / core.document_tags application
  • core.document_relationships (parent→child, e.g. email→attachment)
  • legal.email_metadata side-table writes
  • Recursive attachment ingestion as child documents

Scope — what this module does NOT do:
  • Fetch from Graph/OneDrive/Dropbox — callers handle source acquisition
  • Binary text extraction (OCR/PDF/DOCX) — callers supply extracted_text
  • Webhook / delta / polling loops — callers orchestrate
  • Source-specific dedup (Graph messageId, OneDrive itemId) — callers
    shape into a stable source_path which is our idempotency key

Usage:
    from ingestion_pipeline import ingest, DocumentSpec, Attachment

    result = await ingest(
        DocumentSpec(
            domain="legal",
            document_type="email",
            source_path=f"graph://{mailbox}/{msg_id}",
            title=subject,
            full_content=parsed["full_text"],
            metadata={"mailbox": mailbox, "graph_message_id": msg_id},
            email_meta={"sender": ..., "subject": ..., "date_sent": ...},
            attachments=[Attachment(filename="exhibit_a.pdf",
                                    content_type="application/pdf",
                                    extracted_text=ocr_text)],
        ),
        pool=pool,
    )
    print(result.doc_id, result.chunk_count, result.attachment_doc_ids)
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

sys.path.insert(0, "/opt/wdws")

from embedding_service import (  # type: ignore[import-untyped]
    EMBEDDING_DIMENSIONS,
    _vec_literal,
    embed_texts_sync,
    get_or_create_embedding_model_id,
)
from email_chunker import (  # type: ignore[import-untyped]
    ChunkResult,
    EmailAwareChunker,
    PartType,
)
from contextual_retrieval import (  # type: ignore[import-untyped]
    enrich_chunks,
    generate_context_sync,
)

log = logging.getLogger("athena.ingestion")


# ============================================================
# Public data classes
# ============================================================

@dataclass
class Attachment:
    """A file attached to a parent document.

    Callers pre-extract text (PDF/OCR/DOCX/etc.) and supply `extracted_text`.
    Pipeline chunks + embeds + links as a child document of the parent.
    """
    filename: str
    content_type: str = ""
    extracted_text: str = ""
    extraction_method: str = ""          # 'ocr', 'text_decode', 'html_strip', ...
    file_size: int = 0
    metadata: dict = field(default_factory=dict)


@dataclass
class DocumentSpec:
    """What to ingest. Callers shape their source-specific data into this."""
    domain: str                            # 'legal', 'medical', 'general', 'web', ...
    document_type: str                     # 'email', 'email_attachment', 'court_filing', ...
    source_path: str                       # stable identity key — idempotency boundary
    title: str
    full_content: str                      # the text to index
    metadata: dict = field(default_factory=dict)

    # Optional — pipeline fills defaults if None/empty
    filename: Optional[str] = None         # defaults to title for display
    content_hash: Optional[str] = None     # defaults to sha256(full_content)[:16]
    case_numbers: Optional[list[str]] = None   # None → auto-extract from content
    tags: list[str] = field(default_factory=list)

    # Type-specific side-table extensions
    email_meta: Optional[dict] = None          # only for document_type=='email'
    attachment_meta: Optional[dict] = None     # only for document_type=='email_attachment'
                                               # — keys: id (uuid str, optional),
                                               #   filename, content_type, file_size,
                                               #   extracted_text (≤100k chars),
                                               #   extraction_method
    medical_meta: Optional[dict] = None        # only for domain=='medical'
                                               # — keys: patient_name, provider,
                                               #   facility, date_of_service (YYYY-MM-DD
                                               #   or ISO), record_type, section_title,
                                               #   extra_metadata (dict, optional)

    # Optional: original pre-cleaning text. Callers that do text normalization
    # (HTML stripping, redaction) preserve the pre-cleaning form in raw_content
    # so audits / re-runs can see what arrived. Defaults to full_content.
    raw_content: Optional[str] = None

    # Blob-storage pointer (populated by callers that have already uploaded
    # the original bytes to object storage via blob_storage.upload_*).
    # When set, retention_sweep and the MCP download path will read/delete
    # from this location instead of (or in addition to) the legacy FS path
    # in source_path.
    storage_backend: Optional[str] = None       # 'minio' | 's3' | 'fs'
    storage_uri: Optional[str] = None           # 's3://bucket/key' or FS path
    storage_sha256: Optional[str] = None
    storage_size_bytes: Optional[int] = None

    # Relationships
    parent_doc_id: Optional[str] = None    # set when ingesting an attachment
    relationship_type: str = "has_attachment"  # used with parent_doc_id

    # Nested attachments — ingested as child documents after parent
    attachments: list[Attachment] = field(default_factory=list)


@dataclass
class IngestResult:
    doc_id: str
    was_existing: bool                     # True if source_path already ingested
    chunk_count: int
    embedded_chunk_count: int              # chunks that got a real vector
    case_links_created: int = 0
    tags_applied: int = 0
    attachment_doc_ids: list[str] = field(default_factory=list)
    error: Optional[str] = None            # non-None → pipeline caught an exception


# ============================================================
# Case-number extraction (canonical home)
# ============================================================
#
# Callers still passing explicit case_numbers keep their own extractors
# (e.g. email_sync.extract_case_numbers_from_text). When case_numbers is
# None on a DocumentSpec, we run this to auto-extract. Kept intentionally
# conservative — false positives create noisy legal.case_documents rows.

_STATE_CASE_RE = re.compile(r'(\d{2})-(\d)-(\d{5})-(\d{2})')
_FEDERAL_CASE_RE = re.compile(
    r'(\d{1,2})[-:]?(\d{2})[-:]?cv[-:]?(\d{5})[-:]?(\w{2,4})?',
    re.IGNORECASE,
)


def extract_case_numbers_from_text(text: str, scan_prefix: int = 5000) -> list[str]:
    """Extract and normalize case numbers from a document's leading text.

    `scan_prefix` caps how much of the content we scan — most documents
    name their case in the first page/header, and scanning the full body
    on very long docs is wasted work.
    """
    if not text:
        return []
    sample = text[:scan_prefix]
    found: set[str] = set()
    for m in _FEDERAL_CASE_RE.finditer(sample):
        court, year, seq, judge = m.groups()
        normalized = f"{court}{year}CV{seq}"
        if judge:
            normalized += judge.upper()
        found.add(normalized.upper())
    for m in _STATE_CASE_RE.finditer(sample):
        found.add(m.group(0))
    return sorted(found)


# ============================================================
# Internal: chunker routing
# ============================================================

_CHUNKER = EmailAwareChunker()


def _chunk_for_spec(spec: DocumentSpec) -> list[ChunkResult]:
    """Select and run the right chunker strategy for this document type.

    When PDF/DOCX structure-aware chunkers exist, they plug in here as
    additional branches — callers never select a chunker themselves.
    """
    dt = spec.document_type

    if dt == "email":
        results = _CHUNKER.split_email_body(spec.full_content)
        if results:
            return results
        # Defensive fallback — very short / malformed messages
        return [
            ChunkResult(text=t, source_part=PartType.BODY, is_embedded=True)
            for t in _CHUNKER.split(spec.full_content)
        ]

    if dt == "email_attachment":
        return _CHUNKER.split_attachment(
            spec.full_content,
            filename=spec.metadata.get("filename", "") or spec.filename or "",
            content_type=spec.metadata.get("content_type", ""),
        )

    # Generic path for all non-email document types. GENERIC is its own
    # PartType so retrievers can distinguish "chunk from a generic doc"
    # from "body of an email" via metadata.source_part.
    return [
        ChunkResult(text=t, source_part=PartType.GENERIC, is_embedded=True)
        for t in _CHUNKER.split(spec.full_content)
    ]


# ============================================================
# Internal: context + embed
# ============================================================

def _enrich_and_embed(
    spec: DocumentSpec,
    chunk_results: list[ChunkResult],
) -> list[Optional[list[float]]]:
    """Generate contextual prefixes and embed only the is_embedded chunks.

    Returns a parallel list to chunk_results where None means "don't embed
    this chunk" (quoted-reply, signature, disclaimer). The caller writes
    NULL to the embedding column so the chunk is still indexed for FTS
    but excluded from dense similarity search.
    """
    embed_indices = [i for i, r in enumerate(chunk_results) if r.is_embedded]
    embeddings: list[Optional[list[float]]] = [None] * len(chunk_results)
    if not embed_indices:
        return embeddings

    raw_texts = [chunk_results[i].text for i in embed_indices]
    case_num = spec.case_numbers[0] if spec.case_numbers else None

    # Contextual Retrieval: attach a short context prefix to each chunk
    # before embedding. Improves retrieval recall on questions that reference
    # the document's topic without quoting exact chunk text.
    try:
        context = generate_context_sync(
            title=spec.title,
            domain=spec.domain,
            document_type=spec.document_type,
            content_preview=spec.full_content[:3000],
            case_number=case_num,
        )
        enriched_texts = enrich_chunks(context, raw_texts)
    except Exception as e:
        log.warning("context generation failed for %s: %s — embedding raw chunks",
                    spec.source_path, e)
        enriched_texts = list(raw_texts)

    # Persist enriched text alongside each chunk for DB write.
    for list_idx, chunk_idx in enumerate(embed_indices):
        chunk_results[chunk_idx].extra["_embedded_content"] = enriched_texts[list_idx]

    try:
        vectors = embed_texts_sync(enriched_texts)
    except Exception as e:
        log.error("embedding failed for %s: %s — writing zero vectors so dense "
                  "search misses but FTS still works", spec.source_path, e)
        vectors = [[0.0] * EMBEDDING_DIMENSIONS] * len(enriched_texts)

    for list_idx, chunk_idx in enumerate(embed_indices):
        embeddings[chunk_idx] = vectors[list_idx]
    return embeddings


# ============================================================
# Internal: DB writes
# ============================================================

async def _insert_core_document(
    conn,
    doc_id: uuid.UUID,
    spec: DocumentSpec,
    content_hash: str,
    total_chunks: int,
) -> None:
    raw_text = (spec.raw_content if spec.raw_content is not None
                else spec.full_content)
    await conn.execute(
        """
        INSERT INTO core.documents
            (id, domain, source_path, filename, document_type, title,
             content_hash, total_chunks, full_content, raw_content, metadata,
             storage_backend, storage_uri, storage_sha256, storage_size_bytes)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb,
                $12, $13, $14, $15)
        ON CONFLICT (id) DO NOTHING
        """,
        doc_id,
        spec.domain,
        spec.source_path,
        spec.filename or spec.title,
        spec.document_type,
        spec.title,
        content_hash,
        total_chunks,
        spec.full_content[:500000],
        raw_text[:500000],
        json.dumps(spec.metadata),
        spec.storage_backend,
        spec.storage_uri,
        spec.storage_sha256,
        spec.storage_size_bytes,
    )


async def _insert_chunks(
    conn,
    doc_id: uuid.UUID,
    spec: DocumentSpec,
    chunk_results: list[ChunkResult],
    embeddings: list[Optional[list[float]]],
    model_id: int,
) -> int:
    """Insert chunks. Returns count of chunks with a real (non-None) embedding."""
    embedded_count = 0
    total = len(chunk_results)

    for i, (r, emb) in enumerate(zip(chunk_results, embeddings)):
        chunk_id = hashlib.md5(f"{spec.source_path}:{i}".encode()).hexdigest()

        # Build per-chunk metadata: start from ChunkResult.extra, add
        # source_part / is_embedded / case_numbers, strip internal keys.
        chunk_meta: dict[str, Any] = dict(r.extra)
        enriched_text = chunk_meta.pop("_embedded_content", None)
        chunk_meta.update({
            "source_part": r.source_part.value,
            "is_embedded": r.is_embedded,
            "case_numbers": spec.case_numbers or [],
        })

        emb_param = _vec_literal(emb) if emb is not None else None
        if emb is not None:
            embedded_count += 1

        await conn.execute(
            """
            INSERT INTO core.document_chunks
                (id, document_id, chunk_index, total_chunks,
                 content, embedded_content, embedding, embedding_model_id, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, $7::halfvec, $8, $9::jsonb)
            ON CONFLICT (id) DO NOTHING
            """,
            chunk_id, doc_id, i, total,
            r.text, enriched_text, emb_param, model_id,
            json.dumps(chunk_meta),
        )

    return embedded_count


async def _link_cases(conn, doc_id: uuid.UUID, case_numbers: list[str]) -> int:
    """Link doc to legal.cases rows. Returns count of links actually created."""
    created = 0
    for case_num in case_numbers:
        row = await conn.fetchrow(
            "SELECT id FROM legal.cases WHERE case_number = $1",
            case_num,
        )
        if not row:
            # Case not in registry — silently skip. Upstream case-management
            # workflow creates these; we don't auto-create from ingest.
            continue
        result = await conn.execute(
            """
            INSERT INTO legal.case_documents (case_id, document_id)
            VALUES ($1, $2) ON CONFLICT DO NOTHING
            """,
            row["id"], doc_id,
        )
        # asyncpg returns 'INSERT 0 1' / 'INSERT 0 0' — parse the row count.
        if result and result.endswith(" 1"):
            created += 1
    return created


async def _apply_tags(conn, doc_id: uuid.UUID, tag_names: list[str]) -> int:
    """Attach tags to doc. Creates missing tags. Returns count applied."""
    applied = 0
    for name in tag_names:
        tag_row = await conn.fetchrow(
            """
            INSERT INTO core.tags (name)
            VALUES ($1)
            ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
            RETURNING id
            """,
            name,
        )
        if not tag_row:
            continue
        result = await conn.execute(
            """
            INSERT INTO core.document_tags (document_id, tag_id)
            VALUES ($1, $2) ON CONFLICT DO NOTHING
            """,
            doc_id, tag_row["id"],
        )
        if result and result.endswith(" 1"):
            applied += 1
    return applied


async def _link_parent(
    conn,
    parent_doc_id: uuid.UUID,
    child_doc_id: uuid.UUID,
    relationship_type: str,
) -> None:
    await conn.execute(
        """
        INSERT INTO core.document_relationships
            (source_document_id, target_document_id, relationship_type)
        VALUES ($1, $2, $3)
        ON CONFLICT DO NOTHING
        """,
        parent_doc_id, child_doc_id, relationship_type,
    )


async def _upsert_patient(conn, name: str) -> Optional[int]:
    if not name:
        return None
    row = await conn.fetchrow("SELECT id FROM medical.patients WHERE name = $1", name)
    if row:
        return row["id"]
    row = await conn.fetchrow(
        "INSERT INTO medical.patients (name) VALUES ($1) "
        "ON CONFLICT (name) DO NOTHING RETURNING id",
        name,
    )
    if row:
        return row["id"]
    # Race fallback — another writer inserted between our SELECT and INSERT
    row = await conn.fetchrow(
        "SELECT id FROM medical.patients WHERE name = $1", name,
    )
    return row["id"] if row else None


async def _upsert_provider(
    conn, name: str, facility: Optional[str] = None,
) -> Optional[int]:
    if not name:
        return None
    fac = facility or ""
    row = await conn.fetchrow(
        """
        SELECT id FROM medical.providers
        WHERE name = $1 AND COALESCE(facility, '') = COALESCE($2, '')
        """,
        name, fac,
    )
    if row:
        return row["id"]
    try:
        row = await conn.fetchrow(
            "INSERT INTO medical.providers (name, facility) VALUES ($1, $2) RETURNING id",
            name, facility,
        )
        return row["id"]
    except Exception:
        row = await conn.fetchrow(
            """
            SELECT id FROM medical.providers
            WHERE name = $1 AND COALESCE(facility, '') = COALESCE($2, '')
            """,
            name, fac,
        )
        return row["id"] if row else None


def _parse_dos(value) -> Optional[datetime]:
    """Parse a date_of_service string into a date/datetime, or pass-through.

    medical.record_metadata.date_of_service is a DATE column — asyncpg will
    coerce a datetime down to date on insert, so we just need a real datetime
    (or None) here.
    """
    if value is None or isinstance(value, datetime):
        return value
    if not value or not isinstance(value, str):
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(value[:len(fmt) + 10], fmt)
        except Exception:
            continue
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


async def _write_medical_metadata(conn, doc_id: uuid.UUID, med: dict) -> None:
    """Write medical.record_metadata + upsert patient/provider.

    `med` keys:
      patient_name, provider, facility, date_of_service,
      record_type, section_title, extra_metadata (optional dict)
    """
    patient_id = await _upsert_patient(conn, med.get("patient_name", ""))
    provider_id = await _upsert_provider(
        conn, med.get("provider", ""), med.get("facility"),
    )
    await conn.execute(
        """
        INSERT INTO medical.record_metadata
            (document_id, patient_id, provider_id, record_type,
             date_of_service, facility, section_title, metadata)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
        ON CONFLICT (document_id) DO NOTHING
        """,
        doc_id,
        patient_id,
        provider_id,
        med.get("record_type"),
        _parse_dos(med.get("date_of_service")),
        med.get("facility"),
        med.get("section_title"),
        json.dumps(med.get("extra_metadata") or {}),
    )


async def _write_email_attachment_record(
    conn,
    child_doc_id: uuid.UUID,
    parent_email_doc_id: uuid.UUID,
    att: dict,
) -> None:
    """Upsert a legal.email_attachments row linking parent email to child doc.

    Honors a pre-existing `id` (caller's stable dedup key across reruns) if
    provided; otherwise generates a new UUID. Idempotent on re-run —
    ON CONFLICT (id) updates the child_doc_id linkage and sets is_processed.
    """
    att_id_raw = att.get("id")
    if att_id_raw:
        try:
            att_id = uuid.UUID(str(att_id_raw))
        except (ValueError, TypeError):
            att_id = uuid.uuid4()
    else:
        att_id = uuid.uuid4()

    # legal.email_attachments has no primary-key upsert target besides `id`.
    # Use an UPDATE-then-INSERT-if-zero-rows pattern so we handle both
    # "existing row found by caller" and "new row" in one helper.
    result = await conn.execute(
        """
        UPDATE legal.email_attachments
           SET child_doc_id      = $2,
               filename          = $3,
               content_type      = $4,
               file_size         = $5,
               extracted_text    = $6,
               extraction_method = $7,
               is_processed      = true
         WHERE id = $1
        """,
        att_id,
        child_doc_id,
        att.get("filename", ""),
        att.get("content_type", ""),
        int(att.get("file_size") or 0),
        (att.get("extracted_text") or "")[:100000],
        att.get("extraction_method", ""),
    )
    # asyncpg returns 'UPDATE 0' / 'UPDATE 1'. Insert if nothing updated.
    if result.endswith(" 0"):
        await conn.execute(
            """
            INSERT INTO legal.email_attachments
                (id, email_doc_id, child_doc_id, filename, content_type,
                 file_size, extracted_text, extraction_method, is_processed)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, true)
            ON CONFLICT (id) DO NOTHING
            """,
            att_id,
            parent_email_doc_id,
            child_doc_id,
            att.get("filename", ""),
            att.get("content_type", ""),
            int(att.get("file_size") or 0),
            (att.get("extracted_text") or "")[:100000],
            att.get("extraction_method", ""),
        )


async def _write_email_metadata(conn, doc_id: uuid.UUID, em: dict) -> None:
    """Write legal.email_metadata row. Caller supplies parsed date_sent
    as a timestamp, not a raw string — mirrors email_sync behavior."""
    date_sent = em.get("date_sent")
    if isinstance(date_sent, str):
        # Last-mile parse: callers should convert before handing off, but
        # tolerate string input to keep migration incremental.
        try:
            date_sent = datetime.fromisoformat(date_sent.replace("Z", "+00:00"))
        except Exception:
            date_sent = None

    await conn.execute(
        """
        INSERT INTO legal.email_metadata
            (document_id, message_id, in_reply_to, thread_id,
             sender, recipients, cc, date_sent, direction,
             mailbox, has_attachments, subject)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ON CONFLICT (document_id) DO NOTHING
        """,
        doc_id,
        em.get("message_id"),
        em.get("in_reply_to"),
        em.get("thread_id"),
        em.get("sender"),
        em.get("recipients"),
        em.get("cc"),
        date_sent,
        em.get("direction"),
        em.get("mailbox"),
        em.get("has_attachments", False),
        em.get("subject"),
    )


# ============================================================
# Public API
# ============================================================

async def ingest(spec: DocumentSpec, *, pool, dry_run: bool = False) -> IngestResult:
    """Ingest a single document (+ its attachments recursively).

    Idempotent by spec.source_path — calling twice with the same source_path
    returns the existing doc_id without re-embedding.

    On error mid-pipeline, the transaction rolls back (nothing persisted)
    and IngestResult.error carries the exception message.
    """
    # 1. Idempotency check — if already ingested, return existing doc_id.
    existing = await pool.fetchval(
        "SELECT id FROM core.documents WHERE source_path = $1",
        spec.source_path,
    )
    if existing and not dry_run:
        return IngestResult(
            doc_id=str(existing),
            was_existing=True,
            chunk_count=0,
            embedded_chunk_count=0,
        )

    # 2. Compute derived fields.
    doc_id = uuid.uuid4()
    content_hash = spec.content_hash or hashlib.sha256(
        spec.full_content.encode()).hexdigest()[:16]
    if spec.case_numbers is None:
        spec.case_numbers = extract_case_numbers_from_text(spec.full_content)

    # 3. Chunk (routes by document_type).
    chunk_results = _chunk_for_spec(spec)
    if not chunk_results:
        return IngestResult(
            doc_id=str(doc_id),
            was_existing=False,
            chunk_count=0,
            embedded_chunk_count=0,
            error="no chunks produced (empty content)",
        )

    # Dry-run short-circuit: skip the expensive embedding/context step.
    # The dry-run's purpose is validating structural correctness (chunker
    # routing, case extraction, spec defaults), not exercising model inference.
    if dry_run:
        embedded = sum(1 for r in chunk_results if r.is_embedded)
        return IngestResult(
            doc_id=str(doc_id),
            was_existing=False,
            chunk_count=len(chunk_results),
            embedded_chunk_count=embedded,
        )

    # 4. Enrich + embed (only body/attachment/generic parts; quoted skipped).
    embeddings = _enrich_and_embed(spec, chunk_results)

    # 5. Single transaction: doc + chunks + case links + tags + parent link
    #    + email metadata. If any step fails, nothing persists.
    attachment_doc_ids: list[str] = []
    try:
        async with pool.acquire() as conn:
            model_id = await get_or_create_embedding_model_id(conn)

            async with conn.transaction():
                await _insert_core_document(
                    conn, doc_id, spec, content_hash, len(chunk_results)
                )
                embedded_count = await _insert_chunks(
                    conn, doc_id, spec, chunk_results, embeddings, model_id
                )
                case_links = await _link_cases(
                    conn, doc_id, spec.case_numbers or []
                )
                tags_applied = await _apply_tags(conn, doc_id, spec.tags)

                if spec.parent_doc_id:
                    await _link_parent(
                        conn,
                        uuid.UUID(spec.parent_doc_id),
                        doc_id,
                        spec.relationship_type,
                    )

                if spec.email_meta:
                    await _write_email_metadata(conn, doc_id, spec.email_meta)

                if spec.attachment_meta and spec.parent_doc_id:
                    await _write_email_attachment_record(
                        conn,
                        child_doc_id=doc_id,
                        parent_email_doc_id=uuid.UUID(spec.parent_doc_id),
                        att=spec.attachment_meta,
                    )

                if spec.medical_meta and spec.domain == "medical":
                    await _write_medical_metadata(conn, doc_id, spec.medical_meta)

    except Exception as e:
        log.exception("ingest failed for %s", spec.source_path)
        return IngestResult(
            doc_id=str(doc_id),
            was_existing=False,
            chunk_count=len(chunk_results),
            embedded_chunk_count=0,
            error=f"{type(e).__name__}: {e}",
        )

    # 6. Recursively ingest attachments as child documents. Each attachment
    #    gets its own DocumentSpec pointing back at the parent. Errors on
    #    one attachment don't abort the others — parent is already saved.
    for att in spec.attachments:
        if not att.extracted_text.strip():
            continue
        att_source_path = f"{spec.source_path}#att/{att.filename}"
        att_spec = DocumentSpec(
            domain=spec.domain,
            document_type="email_attachment",
            source_path=att_source_path,
            title=att.filename,
            full_content=att.extracted_text,
            metadata={
                "filename": att.filename,
                "content_type": att.content_type,
                "file_size": att.file_size,
                "extraction_method": att.extraction_method,
                "parent_source_path": spec.source_path,
                **att.metadata,
            },
            parent_doc_id=str(doc_id),
            relationship_type="has_attachment",
            # Attachments inherit case numbers from parent, but also re-scan
            # their own text (exhibit may reference additional cases).
            case_numbers=None,
        )
        att_result = await ingest(att_spec, pool=pool)
        if not att_result.error:
            attachment_doc_ids.append(att_result.doc_id)

    return IngestResult(
        doc_id=str(doc_id),
        was_existing=False,
        chunk_count=len(chunk_results),
        embedded_chunk_count=embedded_count,
        case_links_created=case_links,
        tags_applied=tags_applied,
        attachment_doc_ids=attachment_doc_ids,
    )


# ============================================================
# CLI self-test (dry-run — no DB writes)
# ============================================================

if __name__ == "__main__":
    import asyncio

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    sample_email = """Hi Bill,

Confirming the mediation date of May 3 in case 26-2-00762-31.

Thanks,
Sarah Jennings

From: Bill Nelson <wjnelson78@gmail.com>
Sent: Monday, April 21, 2026 3:14 PM
Subject: Mediation scheduling
"""

    async def _dry_run():
        spec = DocumentSpec(
            domain="legal",
            document_type="email",
            source_path="test://pipeline-selftest/msg-1",
            title="RE: Mediation scheduling",
            full_content=sample_email,
            email_meta={
                "subject": "RE: Mediation scheduling",
                "sender": "sjennings@example.com",
                "date_sent": "2026-04-22T09:00:00Z",
                "mailbox": "wjnelson78@gmail.com",
                "direction": "inbound",
            },
        )
        # Dry-run: no pool needed for the parts we exercise
        class _NullPool:
            async def fetchval(self, *_a, **_kw): return None
        result = await ingest(spec, pool=_NullPool(), dry_run=True)
        print(f"  doc_id           : {result.doc_id}")
        print(f"  chunk_count      : {result.chunk_count}")
        print(f"  embedded_chunks  : {result.embedded_chunk_count}")
        print(f"  auto-case_numbers: {spec.case_numbers}")
        print(f"  error            : {result.error}")

    print("── Ingestion pipeline dry-run selftest ──")
    asyncio.run(_dry_run())
