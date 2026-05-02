"""
§9.2 acceptance — replicates rag_query hybrid_rrf retrieval (mcp_server_v2.py:5440-5630)
to verify the canary 42 USC § 12101 row is retrieved end-to-end.
"""
import sys, json
sys.path.insert(0, "/opt/wdws"); sys.path.insert(0, "/opt/wdws/mcp-server")
import psycopg2
from embedding_service import embed_query_sync

DSN = "postgresql://wdws:NEL2233obs@127.0.0.1:5432/wdws"
QUESTION = "discrimination against individuals with disabilities findings"
TOP_K = 5
SEM_W, FT_W, RRF_K = 0.7, 0.3, 60
CANARY_DOC_ID = "284ea0b8-4a09-4995-836d-520a38478944"

print(f"[verify] Question: {QUESTION!r}")
print(f"[verify] Embedding question with BAAI/bge-m3 …")
qvec = embed_query_sync(QUESTION)
qlit = "[" + ",".join(str(v) for v in qvec) + "]"

conn = psycopg2.connect(DSN); conn.autocommit = True
fetch_count = TOP_K * 3

# --- Semantic ---
with conn.cursor() as cur:
    cur.execute("""
        SELECT c.id, c.document_id, d.title,
               (c.embedding::halfvec(1024) <=> %s::halfvec(1024)) AS distance
        FROM core.document_chunks c
        JOIN core.documents d ON c.document_id = d.id
        WHERE c.embedding IS NOT NULL AND d.domain = 'legal'
        ORDER BY c.embedding::halfvec(1024) <=> %s::halfvec(1024)
        LIMIT %s
    """, (qlit, qlit, fetch_count))
    sem_rows = cur.fetchall()
print(f"[verify] Semantic top-3:")
for i, r in enumerate(sem_rows[:3]):
    is_canary = (str(r[1]) == CANARY_DOC_ID)
    mark = "  ★ CANARY" if is_canary else ""
    print(f"  rank={i:>2}  dist={r[3]:.4f}  doc={str(r[1])[:8]}  title={r[2][:60]}{mark}")

sem_canary_rank = next((i for i, r in enumerate(sem_rows) if str(r[1]) == CANARY_DOC_ID), None)

# --- Fulltext ---
with conn.cursor() as cur:
    cur.execute("""
        SELECT c.id, c.document_id, d.title,
               ts_rank(c.content_tsv, websearch_to_tsquery('english', %s)) AS ft_rank
        FROM core.document_chunks c
        JOIN core.documents d ON c.document_id = d.id
        WHERE c.content_tsv @@ websearch_to_tsquery('english', %s) AND d.domain = 'legal'
        ORDER BY ft_rank DESC
        LIMIT %s
    """, (QUESTION, QUESTION, fetch_count))
    ft_rows = cur.fetchall()
print(f"[verify] Fulltext top-3:")
for i, r in enumerate(ft_rows[:3]):
    is_canary = (str(r[1]) == CANARY_DOC_ID)
    mark = "  ★ CANARY" if is_canary else ""
    print(f"  rank={i:>2}  rank={r[3]:.4f}  doc={str(r[1])[:8]}  title={r[2][:60]}{mark}")

ft_canary_rank = next((i for i, r in enumerate(ft_rows) if str(r[1]) == CANARY_DOC_ID), None)

# --- RRF fusion (matches rag_query lines 5517-5538) ---
scores = {}
for rank, r in enumerate(sem_rows):
    cid = r[0]; scores[cid] = scores.get(cid, 0) + SEM_W / (RRF_K + rank + 1)
for rank, r in enumerate(ft_rows):
    cid = r[0]; scores[cid] = scores.get(cid, 0) + FT_W  / (RRF_K + rank + 1)

# Build chunk_data lookup
chunk_data = {r[0]: (r[1], r[2]) for r in sem_rows}
for r in ft_rows:
    chunk_data.setdefault(r[0], (r[1], r[2]))

ordered = sorted(scores.items(), key=lambda x: x[1], reverse=True)
print(f"[verify] RRF fused top-{TOP_K}:")
canary_in_topk = False
for i, (cid, sc) in enumerate(ordered[:TOP_K]):
    doc_id, title = chunk_data[cid]
    is_canary = (str(doc_id) == CANARY_DOC_ID)
    if is_canary: canary_in_topk = True
    mark = "  ★ CANARY" if is_canary else ""
    print(f"  rank={i:>2}  rrf={sc:.6f}  doc={str(doc_id)[:8]}  title={title[:60]}{mark}")

# --- Acceptance ---
print()
print(f"[verify] sem_canary_rank = {sem_canary_rank}  (PASS if 0)")
print(f"[verify] ft_canary_rank  = {ft_canary_rank}   (PASS if not None)")
print(f"[verify] in top-{TOP_K}    = {canary_in_topk}  (PASS if True)")
print(f"[verify] search_method    = hybrid_rrf (semantic={SEM_W:.2f}, fulltext={FT_W:.2f}, neighbors=1)")

# Also confirm jsonb_path_ops query pattern hits the metadata index
with conn.cursor() as cur:
    cur.execute("EXPLAIN SELECT id FROM core.documents WHERE metadata @> %s::jsonb",
                (json.dumps({"title_number": 42}),))
    plan = "\n".join(r[0] for r in cur.fetchall())
print(f"[verify] EXPLAIN metadata@>'title_number':42 :")
print("  " + plan.replace("\n", "\n  "))

assert sem_canary_rank == 0, f"semantic miss: rank={sem_canary_rank}"
assert ft_canary_rank is not None, "fulltext miss"
assert canary_in_topk, f"canary not in fused top-{TOP_K}"
print()
print("[verify] ALL ACCEPTANCE CHECKS PASS")
