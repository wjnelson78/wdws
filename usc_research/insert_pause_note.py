"""Direct DB insert of the USC pause-reminder note (MCP bridge is unreachable).

Replicates the work that mcp__claude_ai_Athena_Cognitive_Engine__create_note
performs server-side: insert into core.documents (domain='legal',
document_type='note'), chunk + embed + insert into core.document_chunks,
attach tags via core.document_tags.
"""
from __future__ import annotations
import sys, hashlib, json, os
sys.path.insert(0, "/opt/wdws"); sys.path.insert(0, "/opt/wdws/mcp-server")

import psycopg2, psycopg2.extras
from embedding_service import embed_texts_sync, EMBEDDING_DIMENSIONS

assert EMBEDDING_DIMENSIONS == 1024
DSN = "postgresql://wdws:NEL2233obs@127.0.0.1:5432/wdws"
EMBEDDING_MODEL_ID = 2

TITLE = "USC Ingestion — PAUSED for RCW priority pivot (resume when authorized)"
SOURCE_PATH = "ace-note:usc-paused-2026-04-28"
TAGS = ["usc", "federal-statute", "project-paused", "backlog", "ace-meta"]

with open("/opt/wdws/usc_research/usc_pause_note.md", "r", encoding="utf-8") as f:
    CONTENT = f.read()
content_hash = hashlib.sha256(CONTENT.encode("utf-8")).hexdigest()

def chunk_markdown(md: str, target: int = 4000) -> list[str]:
    """Split on H2 (## ) so each section becomes a chunk; merge tiny adjacent."""
    parts = md.split("\n## ")
    if len(parts) == 1:
        return [md]
    chunks = [parts[0]]
    for p in parts[1:]:
        chunks.append("## " + p)
    # Merge tiny consecutive ones
    merged = []
    buf = ""
    for c in chunks:
        if len(buf) + len(c) + 2 <= target:
            buf = (buf + "\n\n" + c) if buf else c
        else:
            if buf:
                merged.append(buf)
            buf = c
    if buf:
        merged.append(buf)
    return merged

chunks = chunk_markdown(CONTENT, target=4000)
print(f"[note] split into {len(chunks)} chunks (sizes: {[len(c) for c in chunks]})")

print(f"[note] embedding {len(chunks)} chunks via BGE-M3...")
vecs = embed_texts_sync(chunks)
assert all(len(v) == 1024 for v in vecs)

conn = psycopg2.connect(DSN)
conn.autocommit = False
try:
    with conn.cursor() as cur:
        # Insert document
        cur.execute("""
            INSERT INTO core.documents
                (domain, document_type, title, source_path, filename,
                 full_content, content_hash,
                 classification, confidentiality, legal_hold,
                 metadata)
            VALUES
                ('legal', 'note', %s, %s, 'usc_pause_note.md',
                 %s, %s,
                 'public', 'public', FALSE,
                 %s::jsonb)
            RETURNING id
        """, (TITLE, SOURCE_PATH, CONTENT, content_hash,
              json.dumps({
                  "type": "ace_meta_note",
                  "project": "usc_full_ingestion",
                  "status": "paused",
                  "paused_date": "2026-04-28",
                  "reason": "priority pivot to RCW (Revised Code of Washington) full ingestion",
                  "resume_when": "explicit operator authorization",
                  "source_file": "/opt/wdws/usc_research/usc_pause_note.md",
              })))
        document_id = cur.fetchone()[0]
        print(f"[note] core.documents.id = {document_id}")

        # Insert chunks with embeddings (canonical halfvec cast in SQL)
        chunk_rows = []
        for i, (text, vec) in enumerate(zip(chunks, vecs)):
            chunk_id = f"{document_id}:{i}"
            lit = "[" + ",".join(f"{x:.7f}" for x in vec) + "]"
            chunk_rows.append((chunk_id, document_id, i, len(chunks), text,
                               lit, EMBEDDING_MODEL_ID, len(text.split()), text))
        # execute_values doesn't handle the per-row halfvec cast cleanly; loop
        for cid, doc_id, idx, total, text, lit, model_id, tok, embedded in chunk_rows:
            cur.execute("""
                INSERT INTO core.document_chunks
                    (id, document_id, chunk_index, total_chunks, content,
                     embedding, embedding_model_id, token_count, embedded_content, embedded_at)
                VALUES (%s, %s, %s, %s, %s,
                        %s::halfvec(1024), %s, %s, %s, NOW())
            """, (cid, doc_id, idx, total, text, lit, model_id, tok, embedded))
        cur.execute("UPDATE core.documents SET total_chunks=%s WHERE id=%s",
                    (len(chunks), document_id))

        # Attach tags
        cur.execute("SELECT slug, id FROM core.tags WHERE slug = ANY(%s)", (TAGS,))
        slug_to_id = dict(cur.fetchall())
        missing = [s for s in TAGS if s not in slug_to_id]
        if missing:
            print(f"[note] WARNING — tags not found: {missing}")
        attached = []
        for slug in TAGS:
            if slug in slug_to_id:
                cur.execute("""
                    INSERT INTO core.document_tags (document_id, tag_id)
                    VALUES (%s, %s) ON CONFLICT DO NOTHING
                """, (document_id, slug_to_id[slug]))
                attached.append(slug)
        print(f"[note] attached tags: {attached}")

    conn.commit()
    print(f"[note] COMMIT — document_id={document_id}, "
          f"chunks={len(chunks)} (all embedded), tags={len(attached)}")
finally:
    conn.close()
