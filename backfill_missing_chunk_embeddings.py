#!/usr/bin/env python3
"""Batch-fill missing core.document_chunks embeddings using the active BGE-M3 stack."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values


def load_embedding_runtime():
    sys.path.insert(0, "/opt/wdws")
    from embedding_service import (  # noqa: E402
        EMBEDDING_DIMENSIONS,
        EMBEDDING_MODEL_NAME,
        HF_ENABLED,
        _vec_literal,
        embed_texts_sync,
    )

    return {
        "dimensions": EMBEDDING_DIMENSIONS,
        "model_name": EMBEDDING_MODEL_NAME,
        "hf_enabled": HF_ENABLED,
        "vec_literal": _vec_literal,
        "embed_texts_sync": embed_texts_sync,
    }


def load_dotenv(dotenv_path: Path) -> None:
    if not dotenv_path.exists():
        return
    try:
        lines = dotenv_path.read_text().splitlines()
    except OSError:
        return
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def ensure_model_id(cur, runtime: dict[str, object]) -> int:
    provider = "local+hf_tei" if runtime["hf_enabled"] else "local"
    cur.execute(
        """
        INSERT INTO core.embedding_models (name, provider, dimensions, is_local, is_active)
        VALUES (%s, %s, %s, true, true)
        ON CONFLICT (name) DO UPDATE SET
            provider = EXCLUDED.provider,
            dimensions = EXCLUDED.dimensions
        RETURNING id
        """,
        (runtime["model_name"], provider, runtime["dimensions"]),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError("Unable to resolve embedding model id")
    return int(row[0])


def fetch_batch(cur, batch_size: int, domain: str | None, document_type: str | None):
    clauses = ["c.embedding IS NULL", "COALESCE(c.content, '') <> ''"]
    params: list[object] = []

    if domain:
        clauses.append("d.domain = %s")
        params.append(domain)
    if document_type:
        clauses.append("d.document_type = %s")
        params.append(document_type)

    params.append(batch_size)
    where = " AND ".join(clauses)
    cur.execute(
        f"""
        SELECT c.id, c.content
        FROM core.document_chunks c
        JOIN core.documents d ON d.id = c.document_id
        WHERE {where}
        ORDER BY c.created_at NULLS FIRST, c.id
        LIMIT %s
        """,
        params,
    )
    return cur.fetchall()


def count_remaining(cur, domain: str | None, document_type: str | None) -> int:
    clauses = ["c.embedding IS NULL"]
    params: list[object] = []

    if domain:
        clauses.append("d.domain = %s")
        params.append(domain)
    if document_type:
        clauses.append("d.document_type = %s")
        params.append(document_type)

    where = " AND ".join(clauses)
    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM core.document_chunks c
        JOIN core.documents d ON d.id = c.document_id
        WHERE {where}
        """,
        params,
    )
    return int(cur.fetchone()[0])


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill missing chunk embeddings")
    parser.add_argument("--dsn", default=None, help="Override DATABASE_URL")
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Maximum number of missing chunks to process this run (0 = no cap)",
    )
    parser.add_argument("--domain", default=None)
    parser.add_argument("--document-type", default=None)
    parser.add_argument("--count-only", action="store_true")
    args = parser.parse_args()

    if args.batch_size < 1:
        raise SystemExit("--batch-size must be >= 1")

    if args.dsn is None:
        load_dotenv(Path("/opt/wdws/.env"))
    dsn = args.dsn or os.environ.get("DATABASE_URL")
    if not dsn:
        print("DATABASE_URL is required", file=sys.stderr)
        return 2

    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    cur = conn.cursor()
    try:
        remaining = count_remaining(cur, args.domain, args.document_type)
        print(f"Missing chunk embeddings before run: {remaining}", flush=True)
        if args.count_only:
            return 0

        runtime = load_embedding_runtime()
        model_id = ensure_model_id(cur, runtime)
        print(
            f"Using embedding model {runtime['model_name']} (id={model_id}, dims={runtime['dimensions']})",
            flush=True,
        )

        processed = 0
        started = time.time()
        run_limit = args.limit if args.limit and args.limit > 0 else None

        while True:
            if run_limit is not None:
                remaining_budget = run_limit - processed
                if remaining_budget <= 0:
                    break
                batch_size = min(args.batch_size, remaining_budget)
            else:
                batch_size = args.batch_size

            batch = fetch_batch(cur, batch_size, args.domain, args.document_type)
            if not batch:
                break

            chunk_ids = [row[0] for row in batch]
            texts = [row[1] for row in batch]
            vectors = runtime["embed_texts_sync"](texts, batch_size=len(texts))

            now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            rows = []
            for chunk_id, text, vector in zip(chunk_ids, texts, vectors):
                rows.append(
                    (
                        chunk_id,
                        runtime["vec_literal"](vector),
                        model_id,
                        text,
                        json.dumps(
                            {
                                "embedding_backfill": "missing_chunk_embeddings_2026_05_06",
                                "embedded_at": now_iso,
                                "embedding_model": runtime["model_name"],
                            }
                        ),
                    )
                )

            execute_values(
                cur,
                """
                UPDATE core.document_chunks AS c
                   SET embedding = v.embedding::halfvec(1024),
                       embedding_model_id = v.embedding_model_id::int,
                       embedded_content = COALESCE(c.embedded_content, v.embedded_content),
                       embedded_at = NOW(),
                       metadata = COALESCE(c.metadata, '{}'::jsonb) || v.metadata::jsonb
                FROM (VALUES %s) AS v(id, embedding, embedding_model_id, embedded_content, metadata)
                WHERE c.id = v.id
                """,
                rows,
                template="(%s, %s, %s, %s, %s)",
                page_size=len(rows),
            )

            processed += len(rows)
            elapsed = time.time() - started
            rate = processed / elapsed * 60 if elapsed > 0 else 0.0
            print(
                f"Processed {processed} chunk(s) in {elapsed:.1f}s ({rate:.0f} chunks/min)",
                flush=True,
            )

        remaining_after = count_remaining(cur, args.domain, args.document_type)
        print(f"Missing chunk embeddings after run: {remaining_after}", flush=True)
        return 0
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())