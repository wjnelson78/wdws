"""
📊 Data Quality Agent — Document & Embedding Integrity
═══════════════════════════════════════════════════════
Scans the document corpus for quality issues:
  - Missing embeddings
  - Failed/low-quality OCR
  - Duplicate documents
  - Orphaned records (chunks without documents, etc.)
  - Empty content
  - Email threading integrity
"""
import json
from framework import BaseAgent, RunContext


class DataQualityAgent(BaseAgent):
    agent_id = "data-quality"
    agent_name = "Data Quality"
    description = "Scans for missing embeddings, bad OCR, duplicates, orphaned records"
    version = "1.0.0"
    schedule = "0 */6 * * *"  # Every 6 hours
    priority = 3
    capabilities = ["embedding-audit", "ocr-quality", "dedup", "integrity-check"]

    instructions = """You are the Data Quality Agent for the Athena Cognitive Platform.

DATABASE SCHEMA:
- core.documents: id, domain, document_type, title, source_path, content_hash, full_content_preview, created_at
- core.document_chunks: id, document_id, chunk_index, content, embedding (vector), has_embedding
- core.document_relationships: links between documents
- legal.email_metadata: sender, recipients, subject, email_date, direction, mailbox
- legal.email_attachments: email_id, filename, content_type, ocr_text
- legal.cases: case_number, case_title, court, status
- medical.record_metadata: patient, provider, date_of_service, record_type

YOUR RESPONSIBILITIES:
1. Find chunks with missing embeddings (has_embedding = false or embedding IS NULL)
2. Detect documents with empty/very short content (likely OCR failures)
3. Find duplicate documents (same content_hash)
4. Check for orphaned records (chunks without parent documents)
5. Verify email attachment integrity (attachments without OCR text)
6. Monitor embedding model consistency
7. Report all findings with actionable details

QUALITY THRESHOLDS:
- Chunk with < 20 characters of content → likely garbage
- Document with 0 chunks → incomplete ingestion
- More than 5 documents with same content_hash → dedup needed
- Attachments > 1MB with no OCR text → OCR failure

Always quantify issues: "42 chunks missing embeddings across 12 documents" not just "some chunks need embeddings"."""

    async def run(self, ctx: RunContext) -> dict:
        metrics = {}

        # ── Missing embeddings ───────────────────────────────
        missing_embed = await ctx.query("""
            SELECT COUNT(*) as count,
                   COUNT(DISTINCT document_id) as doc_count
            FROM core.document_chunks
            WHERE has_embedding = false OR embedding IS NULL
        """)
        me = missing_embed[0] if missing_embed else {"count": 0, "doc_count": 0}
        metrics["missing_embeddings"] = me["count"]
        metrics["docs_missing_embeddings"] = me["doc_count"]

        if me["count"] > 0:
            await ctx.finding("warning", "data-quality",
                f"{me['count']} chunks missing embeddings across {me['doc_count']} documents",
                "These documents won't appear in semantic search results",
                dict(me))

        # ── Empty/short content chunks ───────────────────────
        short_chunks = await ctx.query("""
            SELECT COUNT(*) as count,
                   COUNT(DISTINCT document_id) as doc_count
            FROM core.document_chunks
            WHERE LENGTH(content) < 20
        """)
        sc = short_chunks[0] if short_chunks else {"count": 0, "doc_count": 0}
        metrics["short_chunks"] = sc["count"]

        if sc["count"] > 10:
            await ctx.finding("warning", "data-quality",
                f"{sc['count']} chunks with < 20 chars content (likely garbage)",
                evidence=dict(sc))

        # ── Documents with zero chunks ───────────────────────
        zero_chunks = await ctx.query("""
            SELECT d.id, d.title, d.domain, d.source_path
            FROM core.documents d
            LEFT JOIN core.document_chunks c ON d.id = c.document_id
            WHERE c.id IS NULL
            LIMIT 50
        """)
        metrics["docs_zero_chunks"] = len(zero_chunks)

        if zero_chunks:
            await ctx.finding("warning", "data-quality",
                f"{len(zero_chunks)} documents have zero chunks (incomplete ingestion)",
                "These documents exist in core.documents but have no content chunks",
                {"sample_ids": [z["id"] for z in zero_chunks[:10]]})

        # ── Duplicate documents (same content_hash) ──────────
        dupes = await ctx.query("""
            SELECT content_hash, COUNT(*) as copies,
                   array_agg(id) as doc_ids,
                   array_agg(DISTINCT domain) as domains
            FROM core.documents
            WHERE content_hash IS NOT NULL AND content_hash != ''
            GROUP BY content_hash
            HAVING COUNT(*) > 1
            ORDER BY COUNT(*) DESC
            LIMIT 20
        """)
        total_dupes = sum(d["copies"] - 1 for d in dupes)
        metrics["duplicate_groups"] = len(dupes)
        metrics["duplicate_docs"] = total_dupes

        if total_dupes > 0:
            await ctx.finding("info", "data-quality",
                f"{total_dupes} duplicate documents in {len(dupes)} groups",
                "Documents with identical content_hash",
                {"top_groups": [{"hash": d["content_hash"][:16], "copies": d["copies"]}
                               for d in dupes[:5]]})

        # ── Orphaned chunks ──────────────────────────────────
        orphans = await ctx.query("""
            SELECT COUNT(*) as count
            FROM core.document_chunks c
            LEFT JOIN core.documents d ON c.document_id = d.id
            WHERE d.id IS NULL
        """)
        metrics["orphaned_chunks"] = orphans[0]["count"] if orphans else 0

        if metrics["orphaned_chunks"] > 0:
            await ctx.finding("warning", "data-quality",
                f"{metrics['orphaned_chunks']} orphaned chunks (no parent document)",
                "These chunks reference documents that no longer exist")

        # ── Email attachment OCR quality ─────────────────────
        bad_ocr = await ctx.query("""
            SELECT COUNT(*) as count
            FROM legal.email_attachments
            WHERE (ocr_text IS NULL OR LENGTH(ocr_text) < 10)
              AND content_type LIKE 'application/pdf%'
        """)
        metrics["attachments_no_ocr"] = bad_ocr[0]["count"] if bad_ocr else 0

        if metrics["attachments_no_ocr"] > 0:
            await ctx.finding("info", "data-quality",
                f"{metrics['attachments_no_ocr']} PDF attachments without OCR text",
                "May need re-processing with OCR pipeline")

        # ── Overall corpus stats ─────────────────────────────
        corpus = await ctx.query("""
            SELECT 
                (SELECT COUNT(*) FROM core.documents) as total_docs,
                (SELECT COUNT(*) FROM core.document_chunks) as total_chunks,
                (SELECT COUNT(*) FROM core.document_chunks WHERE has_embedding = true) as embedded_chunks,
                (SELECT COUNT(DISTINCT domain) FROM core.documents) as domains,
                (SELECT COUNT(*) FROM legal.cases) as cases,
                (SELECT COUNT(*) FROM legal.email_metadata) as emails,
                (SELECT COUNT(*) FROM legal.email_attachments) as attachments
        """)
        if corpus:
            c = corpus[0]
            metrics["total_docs"] = c["total_docs"]
            metrics["total_chunks"] = c["total_chunks"]
            metrics["embedded_chunks"] = c["embedded_chunks"]
            metrics["embedding_coverage"] = round(
                c["embedded_chunks"] / max(c["total_chunks"], 1) * 100, 1)
            metrics["total_cases"] = c["cases"]
            metrics["total_emails"] = c["emails"]

        return {
            "summary": f"Quality scan: {metrics.get('total_docs', 0)} docs, "
                      f"{metrics.get('missing_embeddings', 0)} missing embeddings, "
                      f"{metrics.get('duplicate_docs', 0)} dupes, "
                      f"{metrics.get('orphaned_chunks', 0)} orphans",
            "metrics": metrics,
        }
