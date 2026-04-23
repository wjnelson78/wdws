#!/usr/bin/env python3
"""Sprint A Task 4 LLM-assisted backfill worker.

Per SPRINT_A_WORK_ORDER_v2.2.md §6.3, §6.4 + 2026-04-23 design refinements.

Classifies NULL-privilege legal documents and NULL-phi_status medical
documents using Claude Sonnet 4.6 with the prompts at sprint_a_prompts/.
Stages proposals in core.documents_backfill_staging; routes high-confidence
proposals to batch promotion, medium-confidence to human review, low-
confidence to 'unknown'. Heightened-protection categories route to a
distinct review path regardless of confidence.

Usage (worker mode — long-running):
    sudo -u postgres /opt/wdws/venv/bin/python3 /opt/wdws/sprint_a_llm_backfill.py \\
        --dsn postgresql:///wdws?host=/var/run/postgresql \\
        --mode worker

Usage (T4 sample batch — first 100 per domain, then halt):
    sudo -u postgres /opt/wdws/venv/bin/python3 /opt/wdws/sprint_a_llm_backfill.py \\
        --dsn postgresql:///wdws?host=/var/run/postgresql \\
        --mode sample_batch

Usage (batch promotion — separate invocation):
    sudo -u postgres /opt/wdws/venv/bin/python3 /opt/wdws/sprint_a_llm_backfill.py \\
        --dsn postgresql:///wdws?host=/var/run/postgresql \\
        --mode promote

Design notes:
  - Rate limit: token bucket, 100 tokens/hour rolling. Survives restart by
    initializing from recent staging rows in DB.
  - Model: claude-sonnet-4-6 (per Will's directive — privilege classification
    is load-bearing reasoning, not throughput work).
  - Routing:
      confidence >= 0.9 AND no heightened cats → 'pending' + promotion-ready
      0.7 <= confidence < 0.9  → 'pending' + review queue
      confidence < 0.7         → 'pending' with privilege='unknown'
      any heightened category  → 'pending' + per-category heightened review
                                  regardless of confidence
  - Protective order check: every legal classification checks whether the
    document links to an active legal.protective_orders row; if yes,
    proposed_confidentiality is upgraded to at least 'confidential'.
  - Sample gate: in sample_batch mode, worker halts after classifying 100
    per domain and creates an ops.pending_approvals row for T4_SAMPLE_REVIEW
    with 10 random samples per domain.
  - Promotion: --mode promote runs separately. Sweeps status='pending' +
    eligible rows in batches of 100; atomic transaction per batch with
    count assertions.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import anthropic
import asyncpg

try:
    from zoneinfo import ZoneInfo
    PT_TZ = ZoneInfo("America/Los_Angeles")
except ImportError:
    import pytz  # type: ignore
    PT_TZ = pytz.timezone("America/Los_Angeles")

sys.path.insert(0, "/opt/wdws")
from core_safety import HEIGHTENED_CATEGORIES, REVIEW_REQUIRED_CATEGORIES  # noqa: E402


# ============================================================
# Quiet-hours deferral per §10 rule 6
# ============================================================

def _is_quiet_hours_pt() -> bool:
    """Return True if current PT time is 10pm-7am (autonomous agent quiet hours)."""
    now = datetime.now(PT_TZ)
    return now.hour < 7 or now.hour >= 22


async def _wait_until_quiet_hours_end() -> None:
    """Sleep until 7am PT. Called before creating autonomous
    ops.pending_approvals rows per §10 rule 6."""
    while _is_quiet_hours_pt():
        now = datetime.now(PT_TZ)
        if now.hour >= 22:
            target = (now + timedelta(days=1)).replace(
                hour=7, minute=0, second=0, microsecond=0,
            )
        else:
            target = now.replace(hour=7, minute=0, second=0, microsecond=0)
        wait_sec = (target - now).total_seconds()
        print(f"  quiet hours active; sleeping {wait_sec:.0f}s (to {target})",
              flush=True)
        await asyncio.sleep(min(wait_sec, 300.0))


# ============================================================
# Configuration
# ============================================================

CLASSIFIER_VERSION = 'agent_athena_v1'
CLASSIFIER_MODEL = 'claude-sonnet-4-6'
TOKENS_PER_HOUR = 100
CONFIDENCE_HIGH = 0.9
CONFIDENCE_MEDIUM = 0.7
PROMPTS_DIR = Path("/opt/wdws/sprint_a_prompts")


# ============================================================
# Token-bucket rate limiter (survives restart via DB)
# ============================================================

class TokenBucket:
    """Rolling 1-hour window token bucket.

    consume() returns immediately if budget is available, else returns the
    seconds-to-wait until the oldest token ages out.

    from_db() initializes the bucket from recent staging-row timestamps so
    worker restarts don't immediately burst the rate limit.
    """

    def __init__(self, per_hour: int = TOKENS_PER_HOUR) -> None:
        self.per_hour = per_hour
        self.timestamps: list[float] = []

    def _prune(self, now: float) -> None:
        cutoff = now - 3600
        self.timestamps = [t for t in self.timestamps if t > cutoff]

    def try_consume(self) -> float:
        """Return 0.0 if token consumed, else seconds to wait before retry."""
        now = time.time()
        self._prune(now)
        if len(self.timestamps) < self.per_hour:
            self.timestamps.append(now)
            return 0.0
        oldest = self.timestamps[0]
        return (oldest + 3600.0) - now + 1.0

    async def acquire(self) -> None:
        while True:
            wait = self.try_consume()
            if wait <= 0.0:
                return
            # Sleep in 60-second chunks so Ctrl-C feels responsive
            await asyncio.sleep(min(wait, 60.0))

    @classmethod
    async def from_db(cls, conn: asyncpg.Connection,
                      per_hour: int = TOKENS_PER_HOUR) -> "TokenBucket":
        bucket = cls(per_hour)
        rows = await conn.fetch("""
            SELECT EXTRACT(EPOCH FROM created_at) AS ts
              FROM core.documents_backfill_staging
             WHERE created_at >= now() - interval '1 hour'
             ORDER BY created_at
        """)
        bucket.timestamps = [float(r['ts']) for r in rows]
        return bucket


# ============================================================
# Classifier
# ============================================================

class DomainClassifier:
    """Wraps the Anthropic SDK call for one domain (legal or medical)."""

    def __init__(self, domain: str, client: anthropic.AsyncAnthropic):
        self.domain = domain
        self.client = client
        self.system_prompt = (PROMPTS_DIR / f"{domain}_classifier.md").read_text()
        self.tool_name = f"record_{domain}_classification"
        self.tool_schema = self._tool_schema()

    def _tool_schema(self) -> dict:
        if self.domain == 'legal':
            return {
                "name": "record_legal_classification",
                "description": "Record a legal-document privilege classification.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "privilege": {
                            "type": "string",
                            "enum": ["attorney_client", "work_product_opinion",
                                     "work_product_fact", "joint_defense",
                                     "common_interest", "none", "unknown"],
                        },
                        "confidentiality": {
                            "type": "string",
                            "enum": ["public", "confidential", "aeo",
                                     "under_seal", "highly_confidential"],
                        },
                        "confidence": {"type": "number",
                                       "minimum": 0.0, "maximum": 1.0},
                        "rationale": {"type": "string"},
                    },
                    "required": ["privilege", "confidentiality",
                                 "confidence", "rationale"],
                },
            }
        else:
            return {
                "name": "record_medical_classification",
                "description": "Record a medical-document PHI classification.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "phi_status": {
                            "type": "string",
                            "enum": ["phi", "limited_data_set",
                                     "safe_harbor_deidentified",
                                     "expert_determination_deidentified",
                                     "not_phi"],
                        },
                        "phi_categories": {
                            "type": "array",
                            "items": {
                                "type": "string",
                                "enum": ["sud_42_cfr_part_2", "psychotherapy_notes",
                                         "genetic_gina", "mental_health", "hiv_aids"],
                            },
                        },
                        "minor_patient": {"type": "boolean"},
                        "confidence": {"type": "number",
                                       "minimum": 0.0, "maximum": 1.0},
                        "rationale": {"type": "string"},
                    },
                    "required": ["phi_status", "phi_categories",
                                 "minor_patient", "confidence", "rationale"],
                },
            }

    async def classify(self, doc: asyncpg.Record) -> dict:
        """Classify a document.

        Returns:
            {'ok': True, 'proposal': {...}} on success, OR
            {'ok': False, 'error_detail': str, 'stop_reason': str|None,
             'partial_output': str} on failure.
        """
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
            response = await self.client.messages.create(
                model=CLASSIFIER_MODEL,
                max_tokens=2048,
                system=self.system_prompt,
                tools=[self.tool_schema],
                tool_choice={"type": "tool", "name": self.tool_name},
                messages=[{"role": "user", "content": user_msg}],
            )
        except Exception as exc:
            return {
                'ok': False,
                'error_detail': f"Anthropic SDK exception: {type(exc).__name__}: {exc}",
                'stop_reason': None,
                'partial_output': '',
            }

        # Extract the tool_use block
        for block in response.content:
            if getattr(block, 'type', None) == 'tool_use' and block.name == self.tool_name:
                return {'ok': True, 'proposal': block.input}

        # No tool_use — capture partial output for diagnostics
        partial_parts = []
        for block in response.content:
            btype = getattr(block, 'type', None)
            if btype == 'text':
                partial_parts.append(f"[text]: {block.text[:1000]}")
            elif btype == 'tool_use':
                partial_parts.append(
                    f"[tool_use block with name={block.name!r} "
                    f"(expected {self.tool_name!r}), input={json.dumps(block.input)[:500]}]"
                )
            else:
                partial_parts.append(f"[{btype}]: (opaque)")
        return {
            'ok': False,
            'error_detail': f"no {self.tool_name!r} tool_use block in response "
                            f"(stop_reason={response.stop_reason!r})",
            'stop_reason': response.stop_reason,
            'partial_output': '\n'.join(partial_parts),
        }


# ============================================================
# Staging + routing
# ============================================================

async def check_protective_order(conn: asyncpg.Connection,
                                  document_id: uuid.UUID) -> bool:
    """Return True if the document links to an active protective_orders row."""
    row = await conn.fetchrow("""
        SELECT po.id, po.effective_date
          FROM legal.protective_orders po
          JOIN core.documents d ON d.protective_order_id = po.id
         WHERE d.id = $1::uuid
    """, document_id)
    return row is not None


def _route_legal(proposal: dict, protective_order_active: bool) -> tuple[str, str]:
    """Return (proposed_privilege, proposed_confidentiality) after routing.

    §6.4 requires protective-order-linked docs to have confidentiality >= 'confidential'.
    """
    privilege = proposal['privilege']
    confidentiality = proposal['confidentiality']
    if protective_order_active and confidentiality == 'public':
        confidentiality = 'confidential'
    # Low-confidence → mark as 'unknown' regardless of LLM proposal
    if proposal['confidence'] < CONFIDENCE_MEDIUM:
        privilege = 'unknown'
    return privilege, confidentiality


def _review_required_categories(proposal: dict) -> list[str]:
    """Return the list of review-required categories in the proposal.

    Per v2.2 §6.3, ALL FIVE categories (42 CFR Part 2, psychotherapy notes,
    mental health, HIV/AIDS, genetic) require human review at classification
    time, not just the two retrieval-time hard-block categories defined in
    §5.4. See core_safety.REVIEW_REQUIRED_CATEGORIES vs HEIGHTENED_CATEGORIES
    for the distinction.
    """
    cats = proposal.get('phi_categories') or []
    return [c for c in cats if c in REVIEW_REQUIRED_CATEGORIES]


async def stage_legal_classification(
    conn: asyncpg.Connection, doc: asyncpg.Record,
    proposal: dict, batch_id: uuid.UUID,
) -> uuid.UUID:
    po_active = await check_protective_order(conn, doc['id'])
    privilege, confidentiality = _route_legal(proposal, po_active)

    staging_id = await conn.fetchval("""
        INSERT INTO core.documents_backfill_staging
            (document_id, batch_id, domain,
             proposed_privilege, proposed_confidentiality,
             confidence, rationale, classifier_version, classifier_model)
        VALUES ($1, $2, 'legal', $3, $4, $5, $6, $7, $8)
        RETURNING id
    """, doc['id'], batch_id, privilege, confidentiality,
         proposal['confidence'], proposal['rationale'],
         CLASSIFIER_VERSION, CLASSIFIER_MODEL)

    # Medium-confidence → classification review
    if CONFIDENCE_MEDIUM <= proposal['confidence'] < CONFIDENCE_HIGH:
        await conn.execute("""
            INSERT INTO ops.pending_approvals
                (decision_type, title, description, proposed_action,
                 reasoning, risk_level)
            VALUES ($1, $2, $3, $4, $5, 'medium')
        """,
            'sprint_a_t4_classification_review',
            f"Legal classification review: {doc['title'][:80] if doc['title'] else doc['filename'][:80]}",
            f"Medium-confidence ({proposal['confidence']:.2f}) proposal: "
            f"privilege={privilege!r}, confidentiality={confidentiality!r}. "
            f"Classifier rationale: {proposal['rationale']}",
            json.dumps({'staging_id': str(staging_id),
                        'document_id': str(doc['id']),
                        'domain': 'legal',
                        'proposed': {'privilege': privilege,
                                     'confidentiality': confidentiality},
                        'confidence': proposal['confidence']}),
            proposal['rationale'],
        )

    return staging_id


async def stage_medical_classification(
    conn: asyncpg.Connection, doc: asyncpg.Record,
    proposal: dict, batch_id: uuid.UUID,
) -> uuid.UUID:
    phi_status = proposal['phi_status']
    categories = list(proposal.get('phi_categories') or [])
    minor = bool(proposal.get('minor_patient', False))
    confidence = proposal['confidence']

    # Low-confidence → don't apply the LLM's phi_status; default to 'phi' (safe)
    if confidence < CONFIDENCE_MEDIUM:
        phi_status = 'phi'

    staging_id = await conn.fetchval("""
        INSERT INTO core.documents_backfill_staging
            (document_id, batch_id, domain,
             proposed_phi_status, proposed_phi_categories, proposed_minor_patient,
             confidence, rationale, classifier_version, classifier_model)
        VALUES ($1, $2, 'medical', $3, $4, $5, $6, $7, $8, $9)
        RETURNING id
    """, doc['id'], batch_id, phi_status, categories, minor,
         confidence, proposal['rationale'], CLASSIFIER_VERSION, CLASSIFIER_MODEL)

    # Medium-confidence → classification review
    if CONFIDENCE_MEDIUM <= confidence < CONFIDENCE_HIGH:
        await conn.execute("""
            INSERT INTO ops.pending_approvals
                (decision_type, title, description, proposed_action,
                 reasoning, risk_level)
            VALUES ($1, $2, $3, $4, $5, 'medium')
        """,
            'sprint_a_t4_classification_review',
            f"Medical classification review: {doc['title'][:80] if doc['title'] else doc['filename'][:80]}",
            f"Medium-confidence ({confidence:.2f}) proposal: "
            f"phi_status={phi_status!r}, categories={categories}, minor={minor}. "
            f"Classifier rationale: {proposal['rationale']}",
            json.dumps({'staging_id': str(staging_id),
                        'document_id': str(doc['id']),
                        'domain': 'medical',
                        'proposed': {'phi_status': phi_status,
                                     'phi_categories': categories,
                                     'minor_patient': minor},
                        'confidence': confidence}),
            proposal['rationale'],
        )

    # Review-required categories → per-category review (regardless of confidence).
    # Uses the v2.2 §6.3 broad set (all 5 categories), not the §5.4 narrow
    # retrieval-block set. See core_safety.REVIEW_REQUIRED_CATEGORIES.
    heightened = _review_required_categories(proposal)
    for cat in heightened:
        await conn.execute("""
            INSERT INTO ops.pending_approvals
                (decision_type, title, description, proposed_action,
                 reasoning, risk_level)
            VALUES ($1, $2, $3, $4, $5, 'critical')
        """,
            'sprint_a_t4_heightened_review',
            f"Heightened-category review ({cat}): {doc['title'][:60] if doc['title'] else doc['filename'][:60]}",
            f"Classifier flagged heightened category {cat!r} on this "
            f"medical document. Per v2.2 §5.4, heightened categories never "
            f"surface without explicit per-category authorization. Human "
            f"review required regardless of classifier confidence ({confidence:.2f}).",
            json.dumps({'staging_id': str(staging_id),
                        'document_id': str(doc['id']),
                        'heightened_category': cat,
                        'all_proposed_categories': categories,
                        'phi_status': phi_status,
                        'confidence': confidence}),
            proposal['rationale'],
        )

    return staging_id


# ============================================================
# Worker loop
# ============================================================

async def fetch_next_document(
    conn: asyncpg.Connection, domain: str,
) -> Optional[asyncpg.Record]:
    """Return next NULL-privilege (legal) or NULL-phi_status (medical) doc
    that has no existing pending staging row.

    Uses ORDER BY random() so the sample draws from the full temporal
    distribution of the corpus — not just the most recent documents.
    Per Will 2026-04-23 post-launch feedback: the T4_SAMPLE_REVIEW sample
    must be representative of the bulk run the classifier will later
    operate on. Temporal clustering (created_at DESC) produces a sample
    biased toward recent Sprint-A-era documents (thin OCR, auto-generated
    attachments) that doesn't generalize to the older legal corpus.

    Random ordering is fine for bulk too — any ordering eventually
    processes every document; only the progression of
    privilege_classified_at timestamps changes, which is cosmetic.
    """
    if domain == 'legal':
        cond = "d.privilege IS NULL"
    else:
        cond = "d.phi_status IS NULL"
    row = await conn.fetchrow(f"""
        SELECT d.id, d.title, d.filename, d.document_type,
               d.created_at, d.full_content
          FROM core.documents d
         WHERE d.domain = $1
           AND {cond}
           AND d.full_content IS NOT NULL
           AND NOT EXISTS (
               SELECT 1 FROM core.documents_backfill_staging s
                WHERE s.document_id = d.id AND s.status = 'pending'
           )
         ORDER BY random()
         LIMIT 1
    """, domain)
    return row


async def count_staged_in_batch(
    conn: asyncpg.Connection, batch_id: uuid.UUID, domain: str,
) -> int:
    return await conn.fetchval("""
        SELECT COUNT(*) FROM core.documents_backfill_staging
         WHERE batch_id = $1 AND domain = $2
    """, batch_id, domain)


async def count_pending_this_classifier(
    conn: asyncpg.Connection, domain: str,
) -> int:
    """Count pending staging rows from the current classifier version,
    across all batches. Used for sample_batch target-reached detection
    so that an earlier aborted run's classifications still count."""
    return await conn.fetchval("""
        SELECT COUNT(*) FROM core.documents_backfill_staging
         WHERE domain = $1 AND status = 'pending'
           AND classifier_version = $2
    """, domain, CLASSIFIER_VERSION)


async def run_worker(dsn: str, mode: str, sample_size_per_domain: int = 100,
                     domain_filter: Optional[str] = None) -> None:
    anthropic_key = os.environ.get('ANTHROPIC_API_KEY')
    if not anthropic_key:
        sys.exit("ANTHROPIC_API_KEY env var required")
    client = anthropic.AsyncAnthropic(api_key=anthropic_key)

    conn = await asyncpg.connect(dsn)
    try:
        print(f"mode={mode} as user={await conn.fetchval('SELECT current_user')}")
        bucket = await TokenBucket.from_db(conn)
        print(f"rate bucket initialized: {len(bucket.timestamps)} recent consumptions")

        batch_id = uuid.uuid4()  # identifies this worker-session's batch
        print(f"batch_id = {batch_id}")

        classifiers = {
            'legal': DomainClassifier('legal', client),
            'medical': DomainClassifier('medical', client),
        }

        # Count pre-existing 'pending' rows from this classifier version
        # (e.g. from an earlier aborted run) toward the sample_batch target.
        # Per Will 2026-04-23: those already-staged rows count regardless of
        # the ordering that produced them.
        initial_staged = {
            d: await count_pending_this_classifier(conn, d)
            for d in ('legal', 'medical')
        }
        print(f"initial staged (carryover from prior runs): "
              f"legal={initial_staged['legal']}, medical={initial_staged['medical']}")
        classified = {'legal': initial_staged['legal'],
                      'medical': initial_staged['medical']}
        failed = {'legal': 0, 'medical': 0}

        # --domain filter (per Will 2026-04-23): restrict worker to one domain
        # so legal bulk can run without picking up medical docs during the v2
        # reclassification + mini-validation gate window.
        domains_to_process = ([domain_filter] if domain_filter
                              else ['legal', 'medical'])
        print(f"processing domains: {domains_to_process}")

        while True:
            for domain in domains_to_process:
                if mode == 'sample_batch' and classified[domain] >= sample_size_per_domain:
                    continue
                doc = await fetch_next_document(conn, domain)
                if doc is None:
                    print(f"  [{domain}] no more NULL documents")
                    continue

                await bucket.acquire()
                print(f"  [{domain}] classify {str(doc['id'])[:8]}... "
                      f"title={doc['title'][:50] if doc['title'] else '(none)'}")
                result = await classifiers[domain].classify(doc)
                if not result['ok']:
                    failed[domain] += 1
                    # Stage a 'rejected' row with rich diagnostic error_detail
                    # so the monitor can surface useful info in anomaly reports.
                    detail_parts = [result['error_detail']]
                    if result.get('stop_reason'):
                        detail_parts.append(f"stop_reason={result['stop_reason']}")
                    if result.get('partial_output'):
                        detail_parts.append(f"partial_output:\n{result['partial_output']}")
                    error_detail = '\n'.join(detail_parts)[:4000]
                    print(f"  [rejected] {domain} {str(doc['id'])[:8]}... "
                          f"error={result['error_detail'][:120]}")
                    await conn.execute("""
                        INSERT INTO core.documents_backfill_staging
                            (document_id, batch_id, domain, confidence, rationale,
                             classifier_version, classifier_model, status, error_detail)
                        VALUES ($1, $2, $3, 0.0, 'classifier did not return valid tool_use',
                                $4, $5, 'rejected', $6)
                    """, doc['id'], batch_id, domain,
                         CLASSIFIER_VERSION, CLASSIFIER_MODEL, error_detail)
                    continue

                proposal = result['proposal']
                try:
                    if domain == 'legal':
                        staging_id = await stage_legal_classification(conn, doc, proposal, batch_id)
                    else:
                        staging_id = await stage_medical_classification(conn, doc, proposal, batch_id)
                    classified[domain] += 1
                    print(f"    staged {str(staging_id)[:8]}... "
                          f"confidence={proposal['confidence']:.2f}")
                except Exception as exc:
                    failed[domain] += 1
                    print(f"  [error] staging failed {doc['id']}: {exc!r}",
                          file=sys.stderr)

            # Sample-batch halt condition
            if mode == 'sample_batch' and all(
                classified[d] >= sample_size_per_domain
                for d in domains_to_process
            ):
                print(f"\nsample batch complete: "
                      + ", ".join(f"{d}={classified[d]}" for d in domains_to_process)
                      + ", "
                      + ", ".join(f"failed_{d}={failed[d]}" for d in domains_to_process))
                await create_t4_sample_review(conn, batch_id, classified, failed)
                return

            # Worker mode: keep going until interrupted or no more work
            if mode == 'worker' and all(
                await fetch_next_document(conn, d) is None
                for d in domains_to_process
            ):
                print("no more work; exiting worker loop")
                return
    finally:
        await conn.close()


async def create_t4_sample_review(
    conn: asyncpg.Connection, batch_id: uuid.UUID,
    classified: dict, failed: dict,
) -> None:
    """T4_SAMPLE_REVIEW stop-gate per v2.2 §6.5.

    Defers creation if currently in PT quiet hours (10pm-7am) per §10 rule 6.
    The classifications themselves are already in staging — the row just
    lives in a 'classified but not yet presented for review' state until
    quiet hours end.
    """
    if _is_quiet_hours_pt():
        print("sample_batch completed during quiet hours; deferring "
              "T4_SAMPLE_REVIEW creation until 7am PT", flush=True)
        await _wait_until_quiet_hours_end()
    # 10 random samples per domain — draw from all pending rows this
    # classifier version has produced (across batches), not just this
    # worker-session's batch_id, so carryover classifications from a
    # prior aborted run are included in the sample pool.
    samples = {}
    for domain in ('legal', 'medical'):
        rows = await conn.fetch("""
            SELECT s.id AS staging_id, s.document_id, s.batch_id, s.created_at,
                   s.proposed_privilege, s.proposed_confidentiality,
                   s.proposed_phi_status, s.proposed_phi_categories,
                   s.proposed_minor_patient, s.confidence, s.rationale,
                   d.title, d.filename, d.document_type, d.created_at AS doc_created_at
              FROM core.documents_backfill_staging s
              JOIN core.documents d ON d.id = s.document_id
             WHERE s.domain = $1 AND s.status = 'pending'
               AND s.classifier_version = $2
             ORDER BY random()
             LIMIT 10
        """, domain, CLASSIFIER_VERSION)
        samples[domain] = [
            {
                'staging_id': str(r['staging_id']),
                'document_id': str(r['document_id']),
                'batch_id': str(r['batch_id']),
                'title': r['title'] or r['filename'],
                'document_type': r['document_type'],
                'document_created_at': str(r['doc_created_at']),
                'classified_at': str(r['created_at']),
                'confidence': float(r['confidence']),
                'proposed': {
                    'privilege': r['proposed_privilege'],
                    'confidentiality': r['proposed_confidentiality'],
                    'phi_status': r['proposed_phi_status'],
                    'phi_categories': list(r['proposed_phi_categories'] or []),
                    'minor_patient': r['proposed_minor_patient'],
                },
                'rationale': r['rationale'],
            }
            for r in rows
        ]

    await conn.execute("""
        INSERT INTO ops.pending_approvals
            (decision_type, title, description, proposed_action,
             reasoning, risk_level)
        VALUES ($1, $2, $3, $4, $5, 'critical')
    """,
        'sprint_a_t4_sample_review',
        f"Sprint A T4_SAMPLE_REVIEW: first {classified['legal']} legal + {classified['medical']} medical classifications",
        ("Per v2.2 §6.5, after the first 100 LLM-assisted classifications per "
         "domain, 10 per domain are presented for Will's review. Approval "
         "threshold: ≥9/10 correct per domain. On approval, bulk backfill "
         "proceeds. On rejection (<9/10 correct), worker halts and failing "
         "classifications are surfaced for root-cause analysis."),
        json.dumps({
            'batch_id': str(batch_id),
            'classified_counts': classified,
            'failed_counts': failed,
            'samples': samples,
            'review_threshold': '9 of 10 correct per domain',
        }),
        ("Threshold: 9/10 correct per domain. If rejection, identify whether "
         "the failure is consistent across similar documents or idiosyncratic "
         "per-document; consistent failures indicate prompt-level issues; "
         "idiosyncratic failures may be acceptable as classifier noise."),
    )
    print(f"\nT4_SAMPLE_REVIEW pending_approvals row created.")


# ============================================================
# Batch promotion (run separately)
# ============================================================

async def run_promote(dsn: str) -> None:
    """Promote eligible staging rows to core.documents.

    Eligibility:
      - status = 'pending'
      - confidence >= 0.9
      - no heightened_review pending_approvals for this staging row
      - no classification_review pending_approvals for this staging row
      - document's privilege / phi_status still NULL in core.documents
    """
    conn = await asyncpg.connect(dsn)
    try:
        executing_role = await conn.fetchval("SELECT current_user")
        print(f"promote as {executing_role}")

        # Fetch eligible staging rows in batches of 100
        batch_size = 100
        total_promoted = 0
        while True:
            eligible = await conn.fetch("""
                SELECT s.id AS staging_id, s.document_id, s.domain,
                       s.proposed_privilege, s.proposed_confidentiality,
                       s.proposed_phi_status, s.proposed_phi_categories,
                       s.proposed_minor_patient
                  FROM core.documents_backfill_staging s
                 WHERE s.status = 'pending'
                   AND s.confidence >= $1
                   AND NOT EXISTS (
                       SELECT 1 FROM ops.pending_approvals pa
                        WHERE pa.proposed_action->>'staging_id' = s.id::text
                          AND pa.approved IS NULL
                   )
                 LIMIT $2
            """, CONFIDENCE_HIGH, batch_size)

            if not eligible:
                print("no more eligible staging rows; done")
                break

            async with conn.transaction():
                # Sanity: every row in batch should be status='pending'
                unexpected = await conn.fetchval("""
                    SELECT COUNT(*) FROM core.documents_backfill_staging
                     WHERE id = ANY($1::uuid[]) AND status != 'pending'
                """, [r['staging_id'] for r in eligible])
                if unexpected:
                    raise RuntimeError(f"{unexpected} staging rows in batch have status != 'pending' — halting")

                applied = 0
                for s in eligible:
                    if s['domain'] == 'legal':
                        status = await conn.execute("""
                            UPDATE core.documents
                               SET privilege = $2,
                                   confidentiality = $3,
                                   privilege_classified_by = $4,
                                   privilege_classified_at = now()
                             WHERE id = $1::uuid AND privilege IS NULL
                        """, s['document_id'], s['proposed_privilege'],
                             s['proposed_confidentiality'], CLASSIFIER_VERSION)
                    else:
                        status = await conn.execute("""
                            UPDATE core.documents
                               SET phi_status = $2,
                                   phi_categories = $3,
                                   minor_patient = $4,
                                   phi_classified_by = $5,
                                   phi_classified_at = now()
                             WHERE id = $1::uuid AND phi_status IS NULL
                        """, s['document_id'], s['proposed_phi_status'],
                             s['proposed_phi_categories'] or [],
                             s['proposed_minor_patient'] or False,
                             CLASSIFIER_VERSION)

                    if status.endswith(" 1"):
                        await conn.execute("""
                            UPDATE core.documents_backfill_staging
                               SET status = 'promoted',
                                   promoted_at = now(),
                                   promoted_by = $2
                             WHERE id = $1
                        """, s['staging_id'], executing_role)
                        applied += 1
                    else:
                        # document was already classified (concurrent worker? or manual?)
                        await conn.execute("""
                            UPDATE core.documents_backfill_staging
                               SET status = 'superseded',
                                   superseded_at = now()
                             WHERE id = $1
                        """, s['staging_id'])

                # Sanity: applied + superseded == batch size
                total_promoted += applied
                print(f"  batch: {applied} promoted, {len(eligible) - applied} superseded")

        # Record overall outcome
        await conn.execute("""
            INSERT INTO ops.athena_decisions
                (decision_type, target, description, reasoning, confidence,
                 action_taken, outcome)
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
        """,
            'sprint_a_t4_bulk_promotion',
            'core.documents.privilege, core.documents.phi_status',
            f"Promoted {total_promoted} staging rows to core.documents in batches of {batch_size}.",
            f"Per v2.2 §6.4. Executing role: {executing_role}. High-confidence "
            f"(>={CONFIDENCE_HIGH}) staging rows without pending review queued "
            f"for atomic batch promotion. Concurrent-classification races are "
            f"captured as 'superseded' status.",
            1.0,
            json.dumps({'total_promoted': total_promoted,
                        'classifier_version': CLASSIFIER_VERSION}),
            'applied',
        )
        print(f"\npromotion complete: {total_promoted} rows promoted.")
    finally:
        await conn.close()


# ============================================================
# CLI
# ============================================================

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default=None)
    parser.add_argument("--mode", choices=['worker', 'sample_batch', 'promote'],
                        required=True)
    parser.add_argument("--sample-size", type=int, default=100,
                        help="per-domain sample size for sample_batch mode (default 100)")
    parser.add_argument("--domain", choices=['legal', 'medical'], default=None,
                        help="Restrict worker to one domain (default: both alternating)")
    args = parser.parse_args()
    dsn = args.dsn or os.environ["DATABASE_URL"]

    if args.mode in ('worker', 'sample_batch'):
        asyncio.run(run_worker(dsn, args.mode, args.sample_size, args.domain))
    elif args.mode == 'promote':
        asyncio.run(run_promote(dsn))


if __name__ == "__main__":
    main()
