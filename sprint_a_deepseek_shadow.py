#!/usr/bin/env python3
"""DeepSeek V4 Pro shadow classifier — parallel comparison vs production Sonnet.

Polls core.documents_backfill_staging for Sonnet classifications that have not
yet been shadowed by DeepSeek, fetches the source document, calls DeepSeek with
the *identical* prompt + tool schema as the Sonnet worker, and writes the
result to ops.deepseek_shadow_classifications.

This worker:
  - Does NOT modify core.documents_backfill_staging (production worker's table)
  - Does NOT alter the production Sonnet queue or interfere with the worker
  - Stores both Sonnet's verdict and DeepSeek's verdict for analysis
  - Is rate-limited and concurrency-bounded to avoid swamping the DeepSeek API

Usage:
    set -a && source /opt/wdws/.env && set +a
    sudo -u postgres env DEEPSEEK_API_KEY="$DEEPSEEK_API_KEY" \\
        /opt/wdws/venv/bin/python3 -u /opt/wdws/sprint_a_deepseek_shadow.py \\
        --dsn "postgresql:///wdws?host=/var/run/postgresql" \\
        --concurrency 4 --idle-sleep 60

Quick analysis after some data accumulates:
    SELECT COUNT(*) total,
           SUM((sonnet_privilege=deepseek_privilege)::int) priv_match,
           SUM((sonnet_confidentiality=deepseek_confidentiality)::int) conf_match
      FROM ops.deepseek_shadow_classifications
     WHERE error_detail IS NULL;
"""
from __future__ import annotations

import argparse
import asyncio
import os
import signal
import sys
import time

import anthropic
import asyncpg

sys.path.insert(0, '/opt/wdws')
from sprint_a_llm_backfill import DomainClassifier  # noqa: E402

DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com/anthropic")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-v4-pro")

CLAIM_BATCH = """
SELECT s.id              AS sonnet_staging_id,
       s.document_id,
       s.proposed_privilege       AS sonnet_privilege,
       s.proposed_confidentiality AS sonnet_confidentiality,
       s.confidence               AS sonnet_confidence,
       d.title, d.filename, d.document_type, d.created_at, d.full_content
  FROM core.documents_backfill_staging s
  JOIN core.documents d ON d.id = s.document_id
 WHERE s.classifier_version = 'agent_athena_v1'
   AND s.status IN ('pending', 'promoted')
   AND d.full_content IS NOT NULL
   AND d.document_type <> 'statute'
   AND NOT EXISTS (
       SELECT 1 FROM ops.deepseek_shadow_classifications x
        WHERE x.sonnet_staging_id = s.id
   )
 ORDER BY s.created_at DESC
 LIMIT $1
"""

INSERT_RESULT = """
INSERT INTO ops.deepseek_shadow_classifications
  (document_id, sonnet_staging_id,
   sonnet_privilege, sonnet_confidentiality, sonnet_confidence,
   deepseek_model,
   deepseek_privilege, deepseek_confidentiality, deepseek_confidence, deepseek_rationale,
   latency_ms, error_detail)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
ON CONFLICT (sonnet_staging_id) DO NOTHING
"""


async def classify_with_deepseek(
    classifier: DomainClassifier, doc: asyncpg.Record,
) -> tuple[dict | None, str | None]:
    """Returns (proposal_dict, error_str). One is always None."""
    content = doc["full_content"] or ""
    max_chars = 120_000
    if len(content) > max_chars:
        content = content[:max_chars] + "\n\n[...document truncated for classification...]"
    user_msg = (
        f"Classify the following document.\n\n"
        f"Title: {doc['title'] or '(no title)'}\n"
        f"Filename: {doc['filename'] or '(no filename)'}\n"
        f"Document type: {doc['document_type']}\n"
        f"Created at: {doc['created_at']}\n\n"
        f"Content:\n\n{content}"
    )
    try:
        response = await classifier.client.messages.create(
            model=DEEPSEEK_MODEL,
            max_tokens=4096,
            system=classifier.system_prompt + (
                f"\n\nYou MUST call the `{classifier.tool_name}` tool with your "
                f"classification. Do not respond with prose."
            ),
            tools=[classifier.tool_schema],
            messages=[{"role": "user", "content": user_msg}],
        )
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"[:2000]

    for block in response.content:
        if getattr(block, 'type', None) == 'tool_use' and block.name == classifier.tool_name:
            return block.input, None
    return None, f"no tool_use in response (stop_reason={response.stop_reason})"[:2000]


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--dsn', required=True)
    ap.add_argument('--concurrency', type=int, default=4)
    ap.add_argument('--batch-size', type=int, default=50)
    ap.add_argument('--idle-sleep', type=int, default=60,
                    help='seconds to sleep when no work available')
    ap.add_argument('--max-iterations', type=int, default=0,
                    help='exit after N batches; 0 = run forever')
    args = ap.parse_args()

    if not DEEPSEEK_API_KEY:
        print("ERROR: DEEPSEEK_API_KEY not set", file=sys.stderr)
        return 2

    deepseek_client = anthropic.AsyncAnthropic(
        api_key=DEEPSEEK_API_KEY,
        base_url=DEEPSEEK_BASE_URL,
    )
    classifier = DomainClassifier('legal', deepseek_client)
    pool = await asyncpg.create_pool(args.dsn, min_size=1, max_size=4)

    stop_flag = asyncio.Event()
    def _on_signal(*_): stop_flag.set()
    for s in (signal.SIGTERM, signal.SIGINT):
        try: signal.signal(s, _on_signal)
        except Exception: pass

    sem = asyncio.Semaphore(args.concurrency)
    iter_n = 0
    total_processed = 0

    async def shadow_one(doc):
        async with sem:
            t0 = time.time()
            proposal, err = await classify_with_deepseek(classifier, doc)
            latency_ms = int((time.time() - t0) * 1000)
            async with pool.acquire() as conn:
                await conn.execute(
                    INSERT_RESULT,
                    doc['document_id'], doc['sonnet_staging_id'],
                    doc['sonnet_privilege'], doc['sonnet_confidentiality'],
                    doc['sonnet_confidence'],
                    DEEPSEEK_MODEL,
                    (proposal or {}).get('privilege'),
                    (proposal or {}).get('confidentiality'),
                    (proposal or {}).get('confidence'),
                    (proposal or {}).get('rationale'),
                    latency_ms, err,
                )
            return err is None

    print(f"shadow worker starting — model={DEEPSEEK_MODEL} concurrency={args.concurrency}", flush=True)

    while not stop_flag.is_set():
        async with pool.acquire() as conn:
            docs = await conn.fetch(CLAIM_BATCH, args.batch_size)
        if not docs:
            print(f"[idle] no work — sleeping {args.idle_sleep}s", flush=True)
            try:
                await asyncio.wait_for(stop_flag.wait(), timeout=args.idle_sleep)
            except asyncio.TimeoutError:
                pass
            continue

        t0 = time.time()
        results = await asyncio.gather(*(shadow_one(d) for d in docs))
        ok = sum(1 for r in results if r)
        total_processed += len(docs)
        dt = time.time() - t0
        print(f"[batch {iter_n}] processed={len(docs)} ok={ok} fail={len(docs)-ok} "
              f"in {dt:.1f}s ({len(docs)/dt:.1f}/s) — total_processed={total_processed}",
              flush=True)
        iter_n += 1
        if args.max_iterations and iter_n >= args.max_iterations:
            break

    print(f"shutdown — total_processed={total_processed}", flush=True)
    await pool.close()
    return 0


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))
