#!/usr/bin/env python3
"""Parallel-sample eval: DeepSeek V4 Pro vs Sonnet 4.6 on legal privilege classification.

Picks N already-Sonnet-classified docs (high-confidence pending staging rows),
re-classifies each through DeepSeek V4 Pro using the *identical* system prompt
and tool schema, and reports agreement on privilege / confidentiality / confidence.

Usage:
    DEEPSEEK_API_KEY=... python3 sprint_a_deepseek_eval.py \
        --dsn postgresql:///wdws?host=/var/run/postgresql \
        --sample-size 200 \
        --output /tmp/sprint_a_deepseek_eval.json
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from collections import Counter
from pathlib import Path

import anthropic
import asyncpg

sys.path.insert(0, '/opt/wdws')
from sprint_a_llm_backfill import DomainClassifier  # noqa: E402

DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com/anthropic")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-v4-pro")


SAMPLE_QUERY = """
SELECT s.document_id,
       s.proposed_privilege       AS sonnet_privilege,
       s.proposed_confidentiality AS sonnet_confidentiality,
       s.confidence               AS sonnet_confidence,
       s.rationale                AS sonnet_rationale,
       d.title, d.filename, d.document_type, d.created_at, d.full_content
  FROM core.documents_backfill_staging s
  JOIN core.documents d ON d.id = s.document_id
 WHERE s.status = 'pending'
   AND s.classifier_version = 'agent_athena_v1'
   AND s.confidence >= 0.85
   AND d.full_content IS NOT NULL
   AND d.document_type <> 'statute'
 ORDER BY random()
 LIMIT $1
"""


async def classify_with_deepseek(
    classifier: DomainClassifier, doc: asyncpg.Record,
) -> dict:
    """Run the same classify() flow but against DeepSeek's Anthropic-compatible endpoint."""
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
        return {'ok': False, 'error': f"{type(exc).__name__}: {exc}"}

    for block in response.content:
        if getattr(block, 'type', None) == 'tool_use' and block.name == classifier.tool_name:
            return {'ok': True, 'proposal': block.input}
    return {'ok': False, 'error': f"no tool_use in response: {response.content!r}"[:500]}


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--dsn', required=True)
    ap.add_argument('--sample-size', type=int, default=200)
    ap.add_argument('--output', default='/tmp/sprint_a_deepseek_eval.json')
    ap.add_argument('--concurrency', type=int, default=4)
    args = ap.parse_args()

    if not DEEPSEEK_API_KEY:
        print("ERROR: DEEPSEEK_API_KEY not set", file=sys.stderr)
        return 2

    deepseek_client = anthropic.AsyncAnthropic(
        api_key=DEEPSEEK_API_KEY,
        base_url=DEEPSEEK_BASE_URL,
    )
    classifier = DomainClassifier('legal', deepseek_client)

    pool = await asyncpg.create_pool(args.dsn, min_size=1, max_size=2)
    async with pool.acquire() as conn:
        docs = await conn.fetch(SAMPLE_QUERY, args.sample_size)
    print(f"Sampled {len(docs)} docs (confidence>=0.85, non-statute legal pending)")

    results = []
    sem = asyncio.Semaphore(args.concurrency)
    completed = 0

    async def run_one(doc):
        nonlocal completed
        async with sem:
            t0 = time.time()
            r = await classify_with_deepseek(classifier, doc)
            dt = time.time() - t0
            completed += 1
            if completed % 10 == 0:
                print(f"  [{completed}/{len(docs)}] last latency={dt:.1f}s ok={r.get('ok')}")
            return {
                'document_id': str(doc['document_id']),
                'title': (doc['title'] or '')[:120],
                'document_type': doc['document_type'],
                'sonnet': {
                    'privilege': doc['sonnet_privilege'],
                    'confidentiality': doc['sonnet_confidentiality'],
                    'confidence': float(doc['sonnet_confidence']),
                    'rationale': (doc['sonnet_rationale'] or '')[:300],
                },
                'deepseek': r.get('proposal') if r.get('ok') else None,
                'error': r.get('error'),
                'latency_sec': round(dt, 2),
            }

    results = await asyncio.gather(*(run_one(d) for d in docs))

    # Aggregate
    n = len(results)
    n_ok = sum(1 for r in results if r['deepseek'])
    n_priv_match = sum(1 for r in results if r['deepseek']
                       and r['deepseek'].get('privilege') == r['sonnet']['privilege'])
    n_conf_match = sum(1 for r in results if r['deepseek']
                       and r['deepseek'].get('confidentiality') == r['sonnet']['confidentiality'])

    privilege_confusion = Counter()
    for r in results:
        if r['deepseek']:
            privilege_confusion[(r['sonnet']['privilege'],
                                 r['deepseek'].get('privilege'))] += 1

    summary = {
        'n_sampled': n,
        'n_deepseek_ok': n_ok,
        'n_failures': n - n_ok,
        'privilege_agreement': f"{n_priv_match}/{n_ok} ({100*n_priv_match/n_ok:.1f}%)" if n_ok else "n/a",
        'confidentiality_agreement': f"{n_conf_match}/{n_ok} ({100*n_conf_match/n_ok:.1f}%)" if n_ok else "n/a",
        'privilege_confusion': {f"{k[0]}->{k[1]}": v for k, v in privilege_confusion.most_common()},
    }

    Path(args.output).write_text(json.dumps(
        {'summary': summary, 'results': results}, indent=2, default=str))
    print("\n=== SUMMARY ===")
    for k, v in summary.items():
        print(f"  {k}: {v}")
    print(f"\nFull results written to: {args.output}")
    await pool.close()
    return 0


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))
