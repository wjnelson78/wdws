#!/usr/bin/env python3
"""Athena Cognitive Engine — Personal Memory Re-embedding Helper.

Re-embeds existing core.memories rows from stored text using BGE-M3 (1024 dims).
This is the correct migration path for personal memories after the move away from
OpenAI text-embedding-3-large (3072 dims).

The script intentionally skips morning briefing rows so they remain chronological
context only and do not enter semantic recall.

Usage:
    python migrate_personal_memories.py --dry-run
    python migrate_personal_memories.py
    python migrate_personal_memories.py --limit 100
    python migrate_personal_memories.py --backup-only
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, "/opt/wdws")

_ENV = Path("/opt/wdws/.env")
if _ENV.exists():
    for line in _ENV.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())

import psycopg2
from psycopg2.extras import execute_values

from embedding_service import EMBEDDING_DIMENSIONS, EMBEDDING_MODEL_NAME, _vec_literal, embed_texts_sync

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s %(message)s")
log = logging.getLogger("memory-migrate")

DATABASE_URL = os.environ["DATABASE_URL"]
EMBEDDING_PROVIDER = (os.getenv("EMBEDDING_PROVIDER", "local") or "local").strip()
BACKUP_DIR = Path("/opt/wdws/backups")
DEFAULT_BATCH_SIZE = int(os.getenv("PERSONAL_MEMORY_BATCH_SIZE", "64"))
INDEX_NAME = "idx_memories_embedding_hnsw"


def _get_memory_schema(cur) -> dict[str, object]:
    cur.execute(
        """
        SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod)
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'core'
          AND c.relname = 'memories'
          AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY a.attnum
        """
    )
    columns = {name: data_type for name, data_type in cur.fetchall()}
    if not columns:
        raise RuntimeError("core.memories not found in wdws")

    id_type = str(columns.get("id", "integer"))
    if id_type not in {"integer", "bigint", "uuid"}:
        raise RuntimeError(f"Unsupported core.memories.id type: {id_type}")

    return {
        "columns": columns,
        "id_type": id_type,
        "embedding_type": columns.get("embedding"),
        "has_embedding_model_id": "embedding_model_id" in columns,
        "has_updated_at": "updated_at" in columns,
        "has_last_accessed": "last_accessed" in columns,
        "has_access_count": "access_count" in columns,
        "has_source_id": "source_id" in columns,
        "has_user_id": "user_id" in columns,
    }


def _require_post_migration_schema(schema: dict[str, object]) -> None:
    if not schema["has_embedding_model_id"]:
        raise RuntimeError("core.memories.embedding_model_id is missing. Apply migration 018 before re-embedding personal memories.")
    if not schema["has_updated_at"]:
        raise RuntimeError("core.memories.updated_at is missing. Apply migration 018 before re-embedding personal memories.")
    expected_embedding_type = f"halfvec({EMBEDDING_DIMENSIONS})"
    actual_embedding_type = schema.get("embedding_type")
    if actual_embedding_type != expected_embedding_type:
        raise RuntimeError(
            f"core.memories.embedding is {actual_embedding_type or 'missing'}, expected {expected_embedding_type}. Apply migration 018 before re-embedding personal memories."
        )


def _default_backup_path() -> Path:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return BACKUP_DIR / f"core_memories_backup_{stamp}.jsonl"


def _ensure_embedding_model(cur) -> int:
    cur.execute(
        """
        INSERT INTO core.embedding_models (name, provider, dimensions)
        VALUES (%s, %s, %s)
        ON CONFLICT (name) DO UPDATE SET
            provider = EXCLUDED.provider,
            dimensions = EXCLUDED.dimensions
        RETURNING id
        """,
        (EMBEDDING_MODEL_NAME, EMBEDDING_PROVIDER, EMBEDDING_DIMENSIONS),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError("Failed to resolve embedding model id for personal-memory migration")
    return int(row[0])


def _fetch_stats(cur, schema: dict[str, object]) -> dict:
    embedding_model_count_sql = (
        "COUNT(*) FILTER (WHERE embedding_model_id IS NOT NULL) AS rows_with_embedding_model"
        if schema["has_embedding_model_id"]
        else "0 AS rows_with_embedding_model"
    )
    cur.execute(
        f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(*) FILTER (WHERE COALESCE(btrim(content), '') <> '') AS rows_with_content,
            COUNT(*) FILTER (WHERE embedding IS NOT NULL) AS rows_with_embeddings,
            COUNT(*) FILTER (
                WHERE COALESCE(source, '') = 'morning_briefing'
                   OR COALESCE(metadata->>'type', '') = 'daily_briefing'
            ) AS briefing_rows,
            {embedding_model_count_sql}
        FROM core.memories
        """
    )
    total_rows, rows_with_content, rows_with_embeddings, briefing_rows, rows_with_embedding_model = cur.fetchone()
    return {
        "total_rows": int(total_rows or 0),
        "rows_with_content": int(rows_with_content or 0),
        "rows_with_embeddings": int(rows_with_embeddings or 0),
        "briefing_rows": int(briefing_rows or 0),
        "rows_with_embedding_model": int(rows_with_embedding_model or 0),
    }


def _export_backup(cur, backup_path: Path, schema: dict[str, object]) -> int:
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    cur.execute(
        f"""
        SELECT id::text,
               memory_type,
               content,
               source,
               {"source_id" if schema["has_source_id"] else "NULL::text AS source_id"},
               importance,
               embedding::text AS embedding,
               {"embedding_model_id" if schema["has_embedding_model_id"] else "NULL::int AS embedding_model_id"},
               metadata,
               is_active,
               created_at,
               {"updated_at" if schema["has_updated_at"] else "NULL::timestamptz AS updated_at"},
               {"last_accessed" if schema["has_last_accessed"] else "NULL::timestamptz AS last_accessed"},
               {"access_count" if schema["has_access_count"] else "0 AS access_count"},
               {"user_id" if schema["has_user_id"] else "NULL::int AS user_id"}
        FROM core.memories
        ORDER BY created_at, id
        """
    )

    count = 0
    with backup_path.open("w", encoding="utf-8") as handle:
        for row in cur.fetchall():
            payload = {
                "id": row[0],
                "memory_type": row[1],
                "content": row[2],
                "source": row[3],
                "source_id": row[4],
                "importance": float(row[5]) if row[5] is not None else None,
                "embedding": row[6],
                "embedding_model_id": row[7],
                "metadata": row[8] if isinstance(row[8], dict) else {},
                "is_active": row[9],
                "created_at": row[10].isoformat() if row[10] else None,
                "updated_at": row[11].isoformat() if row[11] else None,
                "last_accessed": row[12].isoformat() if row[12] else None,
                "access_count": int(row[13]) if row[13] is not None else None,
                "user_id": row[14],
            }
            handle.write(json.dumps(payload, default=str) + "\n")
            count += 1
    return count


def _load_candidates(cur, model_id: int, schema: dict[str, object], limit: int | None) -> list[tuple]:
    model_filter_sql = (
        "AND (embedding IS NULL OR embedding_model_id IS DISTINCT FROM %s)"
        if schema["has_embedding_model_id"]
        else ""
    )
    sql = f"""
        SELECT id::text, content, source, memory_type, metadata
        FROM core.memories
        WHERE COALESCE(is_active, true) = true
          AND COALESCE(btrim(content), '') <> ''
          AND COALESCE(source, '') <> 'morning_briefing'
          AND COALESCE(metadata->>'type', '') <> 'daily_briefing'
          {model_filter_sql}
        ORDER BY created_at, id
    """
    params: list[object] = [model_id] if schema["has_embedding_model_id"] else []
    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)
    cur.execute(sql, tuple(params))
    return cur.fetchall()


def _update_batch(cur, batch_rows: list[tuple], model_id: int, schema: dict[str, object]) -> None:
    now_iso = datetime.now(timezone.utc).isoformat()
    texts = [row[1] for row in batch_rows]
    embeddings = embed_texts_sync(texts, batch_size=len(batch_rows))

    values = []
    for row, emb in zip(batch_rows, embeddings):
        row_id, _content, source, memory_type, metadata = row
        merged_meta = dict(metadata or {})
        merged_meta.update(
            {
                "embedding_model": EMBEDDING_MODEL_NAME,
                "embedding_provider": EMBEDDING_PROVIDER,
                "embedding_dimensions": EMBEDDING_DIMENSIONS,
                "embedded_at": now_iso,
                "embedding_migration": {
                    "tool": "migrate_personal_memories.py",
                    "memory_type": memory_type,
                    "source": source,
                    "reembedded": True,
                },
            }
        )
        values.append((row_id, _vec_literal(emb), model_id, json.dumps(merged_meta)))

    execute_values(
        cur,
        f"""
        UPDATE core.memories AS m
        SET embedding = v.embedding::halfvec({EMBEDDING_DIMENSIONS}),
            embedding_model_id = v.embedding_model_id::int,
            metadata = COALESCE(m.metadata, '{{}}'::jsonb) || v.metadata::jsonb,
            updated_at = now()
        FROM (VALUES %s) AS v(id, embedding, embedding_model_id, metadata)
        WHERE m.id = v.id::{schema['id_type']}
        """,
        values,
        template="(%s, %s, %s, %s)",
    )


def _rebuild_index(cur) -> None:
    log.info("Rebuilding memory HNSW index...")
    cur.execute(f"DROP INDEX IF EXISTS core.{INDEX_NAME}")
    cur.execute(
        f"""
        CREATE INDEX CONCURRENTLY {INDEX_NAME}
        ON core.memories
        USING hnsw (embedding halfvec_cosine_ops)
        WITH (m = 16, ef_construction = 200)
        """
    )
    cur.execute("ANALYZE core.memories")


def main() -> int:
    parser = argparse.ArgumentParser(description="Re-embed core.memories with BGE-M3")
    parser.add_argument("--dry-run", action="store_true", help="Show counts without modifying rows")
    parser.add_argument("--backup-only", action="store_true", help="Export a backup and exit")
    parser.add_argument("--skip-backup", action="store_true", help="Do not create a backup export before updating rows")
    parser.add_argument("--backup-file", help="Optional explicit backup file path (.jsonl)")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Rows to embed per batch")
    parser.add_argument("--limit", type=int, help="Maximum number of rows to re-embed")
    parser.add_argument("--skip-index", action="store_true", help="Skip rebuilding the HNSW index")
    args = parser.parse_args()

    if args.batch_size <= 0:
        raise SystemExit("--batch-size must be greater than zero")

    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True

    try:
        with conn.cursor() as cur:
            schema = _get_memory_schema(cur)
            stats = _fetch_stats(cur, schema)
            model_id = _ensure_embedding_model(cur)
            candidates = _load_candidates(cur, model_id, schema, args.limit)

            log.info(
                "Preflight: total=%d content=%d embedded=%d embedded_with_model=%d briefings=%d candidates=%d model_id=%d id_type=%s embedding_type=%s",
                stats["total_rows"],
                stats["rows_with_content"],
                stats["rows_with_embeddings"],
                stats["rows_with_embedding_model"],
                stats["briefing_rows"],
                len(candidates),
                model_id,
                schema["id_type"],
                schema.get("embedding_type"),
            )

            backup_path = Path(args.backup_file) if args.backup_file else _default_backup_path()
            if args.backup_only:
                exported = _export_backup(cur, backup_path, schema)
                log.info("Backup written: %s (%d rows)", backup_path, exported)
                return 0
            if not args.skip_backup and not args.dry_run:
                exported = _export_backup(cur, backup_path, schema)
                log.info("Backup written: %s (%d rows)", backup_path, exported)
            elif args.dry_run:
                log.info("Dry run only — backup not written. Suggested backup path: %s", backup_path)

            if args.dry_run:
                return 0

            _require_post_migration_schema(schema)

            if not candidates:
                log.info("No candidate memory rows require re-embedding")
                if not args.skip_index:
                    _rebuild_index(cur)
                return 0

            started = time.time()
            processed = 0
            for start in range(0, len(candidates), args.batch_size):
                batch = candidates[start:start + args.batch_size]
                _update_batch(cur, batch, model_id, schema)
                processed += len(batch)

                elapsed = max(time.time() - started, 0.001)
                rate = processed / elapsed * 60
                log.info(
                    "Re-embedded %d/%d rows (%.0f rows/min)",
                    processed,
                    len(candidates),
                    rate,
                )

            if not args.skip_index:
                _rebuild_index(cur)

            post_stats = _fetch_stats(cur, schema)
            with conn.cursor() as verify_cur:
                verify_cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM core.memories
                    WHERE embedding IS NOT NULL
                      AND embedding_model_id = %s
                    """,
                    (model_id,),
                )
                embedded_with_model = int(verify_cur.fetchone()[0])

            log.info(
                "Done: total=%d content=%d embedded=%d embedded_with_bge=%d",
                post_stats["total_rows"],
                post_stats["rows_with_content"],
                post_stats["rows_with_embeddings"],
                embedded_with_model,
            )
            return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
