"""
Shared canary helpers — methodology v3.

Three retrieval patterns mirroring real legal-research workflows:
  (a) statutory phrasing  — hybrid_rrf with doc_type_filter='statute'
  (b) citation lookup     — sql_citation_lookup (deterministic SQL exact match)
  (c) popular name + tag  — hybrid_rrf with tag_filter + hnsw.iterative_scan

Pattern (c) requires `hnsw.iterative_scan = strict_order` (pgvector 0.8) to
fix the HNSW + EXISTS interaction where the index returns nearest neighbors
*before* applying the EXISTS clause — at high tag-filter selectivity this
silently returns 0 rows.

`session_preload_libraries = 'vector'` is enabled in postgresql.conf
(/etc/postgresql/17/main/conf.d/10-pgvector-session-preload.conf) so the
GUCs register at session start and `SET hnsw.iterative_scan = strict_order`
works on the first SQL of any new connection — no warm-up needed.
"""
from __future__ import annotations
import re, sys
sys.path.insert(0, "/opt/wdws"); sys.path.insert(0, "/opt/wdws/mcp-server")
import psycopg2
from embedding_service import embed_query_sync

DSN = "postgresql://wdws:NEL2233obs@127.0.0.1:5432/wdws"
SEM_W, FT_W, RRF_K = 0.7, 0.3, 60
TOP_K_DEFAULT = 5

# Connections that have applied iterative_scan = strict_order. Keyed by
# id(conn). With session_preload_libraries the GUCs are pre-registered;
# this just avoids re-running the SET on every call.
_SCAN_SET: set[int] = set()


def _ensure_iterative_scan(conn) -> None:
    """SET hnsw.iterative_scan = strict_order once per connection."""
    key = id(conn)
    if key in _SCAN_SET:
        return
    with conn.cursor() as cur:
        cur.execute("SET hnsw.iterative_scan = strict_order")
    _SCAN_SET.add(key)


def _resolve_tag_to_slug(conn, tag_input: str) -> str | None:
    """Resolve a slug-or-name tag input to its canonical slug.
    Returns None if the tag isn't found.

    Why this two-step pattern instead of `LOWER(t.slug) = LOWER(%s)
    OR LOWER(t.name) = LOWER(%s)` directly in the retrieval SQL:

    PG's planner cannot fold a parameter inside a `LOWER(%s)` call into a
    constant predicate. With the LOWER wrappers in the retrieval SQL, the
    planner picks an HNSW-Index-Scan-then-Semi-Join plan that probes the
    HNSW for top-N nearest chunks BEFORE applying the tag filter — and at
    high tag selectivity (small candidate pool relative to the index), the
    HNSW's top-N silently misses chunks belonging to the tagged docs.
    `hnsw.iterative_scan=strict_order` does not rescue this query shape.

    With slug pre-resolved, the retrieval SQL uses `t.slug = %s` (single
    equality on indexed column, no function wrapping). The planner folds
    the parameter into a constant predicate, sees the small candidate set,
    and chooses a filter-first plan: HashAggregate over tags → JOIN docs →
    JOIN chunks → Sort by distance → LIMIT. Correct results.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT slug FROM core.tags
            WHERE LOWER(slug) = LOWER(%s) OR LOWER(name) = LOWER(%s)
            LIMIT 1
        """, [tag_input, tag_input])
        row = cur.fetchone()
    return row[0] if row else None


def hybrid_rrf(conn, *, question: str, top_k: int = TOP_K_DEFAULT,
               domain: str = "legal", fetch_count_mult: int = 3,
               tag_filter: str | None = None,
               doc_type_filter: str | None = None):
    """Hybrid RRF retrieval mirroring mcp_server_v2.py:5440-5630.

    tag_filter:        slug or human name (case-insensitive) — restrict
                       candidate pool to docs carrying this tag. Triggers
                       hnsw.iterative_scan = strict_order on the session.
    doc_type_filter:   exact value of core.documents.document_type (e.g.,
                       'statute'). Useful when the researcher's intent is
                       "search legal authority, not case law".
    """
    _ensure_iterative_scan(conn)
    qvec = embed_query_sync(question)
    qlit = "[" + ",".join(str(v) for v in qvec) + "]"
    fetch_count = top_k * fetch_count_mult

    extra_clauses: list[str] = []
    extra_params: list = []
    if doc_type_filter:
        extra_clauses.append("d.document_type = %s")
        extra_params.append(doc_type_filter)
    if tag_filter:
        # Pre-resolve to canonical slug so the retrieval SQL is a clean
        # single-equality predicate the planner can fold to a constant.
        # See _resolve_tag_to_slug docstring for the planner-opacity story.
        slug = _resolve_tag_to_slug(conn, tag_filter)
        if slug is None:
            raise ValueError(f"unknown tag filter {tag_filter!r}")
        extra_clauses.append(
            "EXISTS (SELECT 1 FROM core.document_tags dt "
            "JOIN core.tags t ON dt.tag_id = t.id "
            "WHERE dt.document_id = d.id AND t.slug = %s)"
        )
        extra_params.append(slug)
    extra_sql = (" AND " + " AND ".join(extra_clauses)) if extra_clauses else ""

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT c.id, c.document_id, d.title,
                   (c.embedding::halfvec(1024) <=> %s::halfvec(1024)) AS distance
            FROM core.document_chunks c
            JOIN core.documents d ON c.document_id = d.id
            WHERE c.embedding IS NOT NULL AND d.domain = %s {extra_sql}
            ORDER BY c.embedding::halfvec(1024) <=> %s::halfvec(1024)
            LIMIT %s
        """, [qlit, domain, *extra_params, qlit, fetch_count])
        sem = cur.fetchall()
        cur.execute(f"""
            SELECT c.id, c.document_id, d.title,
                   ts_rank(c.content_tsv, websearch_to_tsquery('english', %s)) AS r
            FROM core.document_chunks c
            JOIN core.documents d ON c.document_id = d.id
            WHERE c.content_tsv @@ websearch_to_tsquery('english', %s)
              AND d.domain = %s {extra_sql}
            ORDER BY r DESC
            LIMIT %s
        """, [question, question, domain, *extra_params, fetch_count])
        ft = cur.fetchall()

    scores: dict = {}
    chunk_data: dict = {}
    for rank, r in enumerate(sem):
        cid = r[0]
        scores[cid] = scores.get(cid, 0) + SEM_W / (RRF_K + rank + 1)
        chunk_data.setdefault(cid, (r[1], r[2]))
    for rank, r in enumerate(ft):
        cid = r[0]
        scores[cid] = scores.get(cid, 0) + FT_W / (RRF_K + rank + 1)
        chunk_data.setdefault(cid, (r[1], r[2]))
    ordered = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    seen_docs = set()
    out = []
    for cid, sc in ordered:
        doc_id, title = chunk_data[cid]
        if doc_id in seen_docs:
            continue
        seen_docs.add(doc_id)
        out.append({"doc_id": str(doc_id), "title": title, "rrf_score": sc})
        if len(out) >= top_k:
            break
    return out


def sql_citation_lookup(conn, *, citation: str) -> list[dict]:
    """Deterministic citation lookup. Returns at most one row (the section
    matching the canonical_citation literally). Used for pattern (b) where
    the researcher types the citation verbatim."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT d.id, d.title
            FROM usc.sections s
            JOIN core.documents d ON s.document_id = d.id
            WHERE s.canonical_citation = %s
            LIMIT 1
        """, [citation])
        row = cur.fetchone()
    if not row:
        return []
    return [{"doc_id": str(row[0]), "title": row[1], "rrf_score": 1.0}]


def cite_match(citation: str, title: str) -> bool:
    """Delimiter-aware citation match. Guards against e.g. '§ 12132' matching '§ 12132A'."""
    pattern = re.escape(citation) + r"(\s+—|\s*$)"
    return re.match(pattern, title) is not None


def _print_check_header(chk: dict) -> None:
    name = chk["name"]
    method = chk.get("method", "hybrid_rrf")
    print(f"\n=== {name}  [method={method}] ===")
    if chk.get("question"):
        print(f"Q: {chk['question']!r}")
    if chk.get("tag_filter"):
        print(f"Tag filter: {chk['tag_filter']!r}")
    if chk.get("doc_type_filter"):
        print(f"Doc-type filter: {chk['doc_type_filter']!r}")
    print(f"Expected: {chk['expected']!r} in top-{chk['max_rank']}")


def run_checks(checks: list[dict], *, top_k: int = TOP_K_DEFAULT) -> bool:
    """Run a heterogeneous list of checks. Each check declares its own method.

    Method values:
      'hybrid_rrf'    (default) — uses tag_filter / doc_type_filter
      'sql_citation'           — exact-match against usc.sections.canonical_citation
    """
    conn = psycopg2.connect(DSN); conn.autocommit = True
    overall = True
    for chk in checks:
        _print_check_header(chk)
        method = chk.get("method", "hybrid_rrf")
        eff_top_k = max(top_k, chk["max_rank"])
        if method == "sql_citation":
            results = sql_citation_lookup(conn, citation=chk["expected"])
        elif method == "hybrid_rrf":
            results = hybrid_rrf(
                conn, question=chk["question"], top_k=eff_top_k,
                domain=chk.get("domain", "legal"),
                tag_filter=chk.get("tag_filter"),
                doc_type_filter=chk.get("doc_type_filter"),
            )
        else:
            print(f"  RESULT: FAIL — unknown method {method!r}")
            overall = False
            continue

        found = None
        for i, r in enumerate(results):
            ok = cite_match(chk["expected"], r["title"])
            if ok and found is None:
                found = i
            mark = "  ★ MATCH" if ok else ""
            score_label = "rrf" if method == "hybrid_rrf" else "score"
            print(f"  rank={i+1} {score_label}={r['rrf_score']:.6f}  {r['title'][:90]}{mark}")
        if not results:
            print(f"  (no results)")
        if found is None:
            print(f"  RESULT: FAIL — {chk['expected']!r} not in top {eff_top_k}")
            overall = False
        elif found + 1 > chk["max_rank"]:
            print(f"  RESULT: FAIL — at rank {found+1}, max {chk['max_rank']}")
            overall = False
        else:
            print(f"  RESULT: PASS — rank {found+1}")
    conn.close()
    return overall


def run_set_check(check: dict, *, top_k: int = 10) -> bool:
    """For 'must include all of X, Y, Z' set checks (cross-domain canary)."""
    conn = psycopg2.connect(DSN); conn.autocommit = True
    print(f"\n=== {check['name']} ===")
    print(f"Q: {check['question']!r}")
    print(f"Required (any rank in top-{top_k}): {check['required']}")
    results = hybrid_rrf(conn, question=check["question"], top_k=top_k,
                         tag_filter=check.get("tag_filter"),
                         doc_type_filter=check.get("doc_type_filter"))
    found_set = set()
    for i, r in enumerate(results):
        marks = []
        for cite in check["required"]:
            if cite_match(cite, r["title"]):
                found_set.add(cite); marks.append("★ " + cite)
        suffix = "  " + " | ".join(marks) if marks else ""
        print(f"  rank={i+1} rrf={r['rrf_score']:.6f}  {r['title'][:90]}{suffix}")
    missing = [c for c in check["required"] if c not in found_set]
    conn.close()
    if missing:
        print(f"  RESULT: FAIL — missing {missing}")
        return False
    print(f"  RESULT: PASS — all required citations surfaced")
    return True
