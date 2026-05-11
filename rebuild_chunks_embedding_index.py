#!/usr/bin/env python3
"""Safely restore the missing ANN index for core.document_chunks.

This script exists because the repo's minimal migration runner wraps SQL in a
single transaction, while CREATE INDEX CONCURRENTLY / DROP INDEX CONCURRENTLY
must run outside a transaction block.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import psycopg2


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


def main() -> int:
    parser = argparse.ArgumentParser(description="Restore the HNSW index for core.document_chunks")
    parser.add_argument("--dsn", default=None, help="Override DATABASE_URL")
    parser.add_argument(
        "--drop-existing",
        action="store_true",
        help="Drop known legacy chunk embedding indexes before rebuilding",
    )
    parser.add_argument(
        "--analyze-only",
        action="store_true",
        help="Only run ANALYZE and print current index state",
    )
    args = parser.parse_args()

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
        cur.execute("SELECT extversion FROM pg_extension WHERE extname = 'vector'")
        row = cur.fetchone()
        if not row:
            raise RuntimeError("pgvector extension is not installed")
        print(f"pgvector version: {row[0]}", flush=True)

        cur.execute(
            """
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = 'core'
              AND tablename = 'document_chunks'
              AND (
                    indexdef ILIKE '%embedding%'
                 OR indexname ILIKE '%embedding%'
              )
            ORDER BY indexname
            """
        )
        existing = cur.fetchall()
        if existing:
            print("Existing embedding-related indexes:", flush=True)
            for indexname, indexdef in existing:
                print(f"  - {indexname}: {indexdef}", flush=True)
        else:
            print("No embedding-related indexes currently present on core.document_chunks", flush=True)

        cur.execute(
            """
            SELECT COUNT(*) FILTER (WHERE embedding IS NOT NULL) AS embedded,
                   COUNT(*) FILTER (WHERE embedding IS NULL) AS missing,
                   COUNT(*) AS total
            FROM core.document_chunks
            """
        )
        embedded, missing, total = cur.fetchone()
        print(f"Chunk coverage: embedded={embedded} missing={missing} total={total}", flush=True)

        if args.analyze_only:
            cur.execute("ANALYZE core.document_chunks")
            print("ANALYZE complete", flush=True)
            return 0

        if args.drop_existing:
            for index_name in (
                "idx_chunks_embedding_hnsw",
                "idx_chunks_embedding",
                "idx_document_chunks_embedding_hnsw",
                "document_chunks_embedding_idx",
            ):
                print(f"Dropping {index_name} if it exists...", flush=True)
                cur.execute(f"DROP INDEX CONCURRENTLY IF EXISTS core.{index_name}")

        start = time.time()
        print("Creating idx_chunks_embedding_hnsw concurrently...", flush=True)
        cur.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_chunks_embedding_hnsw
            ON core.document_chunks
            USING hnsw ((embedding::halfvec(1024)) halfvec_cosine_ops)
            WITH (m = 16, ef_construction = 200)
            """
        )
        elapsed = time.time() - start
        print(f"Index build finished in {elapsed:.1f}s", flush=True)

        cur.execute("ANALYZE core.document_chunks")
        print("ANALYZE complete", flush=True)
        return 0
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
