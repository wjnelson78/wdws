"""core_safety.py — Sprint A privilege + PHI retrieval-safety filter.

Per SPRINT_A_WORK_ORDER_v2.2.md §5.3-§5.6.

Public API:
    build_document_safety_filter   — composable WHERE fragment for search paths
    fetch_safe_document            — hardened fetch-by-ID with denial exceptions
    build_morning_brief_safety_filter — stricter locked-in defaults for SMTP brief
    PrivilegeDeniedException, PHIAccessDeniedException
    KEYWORD_PATTERNS               — §6.2 Task 4 keyword auto-classification set

Caller contract (fragment path):
    clause, params, log = await build_document_safety_filter(...)
    rows = await conn.fetch(f"SELECT d.id, d.title, d.domain, d.phi_status
                               FROM core.documents d JOIN ...
                               WHERE ... AND {clause}", *args, *params)
    await log(rows)   # MUST be called exactly once after materializing rows

    The caller's SELECT must include d.id at minimum. For medical+purpose_of_use
    calls, SELECT should also include d.phi_status (and optionally a join to
    surface medical.patients.id into patient_id) so the disclosure log is
    accurate.

Caller contract (fetch path):
    try:
        doc = await fetch_safe_document(doc_id, conn=conn, caller_context={...})
    except PrivilegeDeniedException:
        return denial_response(...)    # NOT a silent empty result
    except PHIAccessDeniedException:
        return phi_denial_response(...)

Footgun note: forgetting to invoke the returned log callback is a known
hazard. The Task 3 diff-review checklist greps every call site that unpacks
`clause, params, log` from this helper and confirms `await log(` appears in
the same function. Closeout captures this as a lint-rule candidate.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date
from typing import Any, Awaitable, Callable, Literal, Optional
from uuid import UUID

import asyncpg


# ============================================================
# Exceptions
# ============================================================

class PrivilegeDeniedException(Exception):
    """Raised when a privileged document is requested without
    include_privileged=True (and optional privileged_categories) matching
    the document's privilege category."""


class PHIAccessDeniedException(Exception):
    """Raised when a PHI document is requested without purpose_of_use, OR
    when a heightened-protection category (psychotherapy_notes,
    sud_42_cfr_part_2) is requested without matching authorization, OR when
    a minor_patient=true document is requested without purpose_of_use."""


# ============================================================
# Constants
# ============================================================

VALID_PRIVILEGE_VALUES = frozenset([
    'attorney_client', 'work_product_opinion', 'work_product_fact',
    'joint_defense', 'common_interest', 'none', 'unknown',
])

VALID_PHI_STATUS = frozenset([
    'phi', 'limited_data_set', 'safe_harbor_deidentified',
    'expert_determination_deidentified', 'not_phi',
])

# Default surfaces: privilege='none' for legal
LEGAL_DEFAULT_ALLOWED = frozenset(['none'])

# Added to default when include_privileged=True
LEGAL_PRIVILEGED_BASE = frozenset(['attorney_client', 'work_product_fact'])

# Opt-in via privileged_categories (requires include_privileged=True)
LEGAL_PRIVILEGED_EXTENDED = frozenset([
    'work_product_opinion', 'joint_defense', 'common_interest',
])

MEDICAL_DEFAULT_ALLOWED = frozenset([
    'safe_harbor_deidentified', 'expert_determination_deidentified', 'not_phi',
])

# Widened to default when purpose_of_use is set
MEDICAL_WITH_PURPOSE_ADDITIONAL = frozenset(['phi', 'limited_data_set'])

# Heightened-protection categories — blocked by default; auth unlocks by scope
HEIGHTENED_CATEGORIES = frozenset(['psychotherapy_notes', 'sud_42_cfr_part_2'])


# ============================================================
# Task 4 keyword-auto-classification pattern set (§6.2)
# ============================================================
#
# Refined after the first sanity-check round identified two failure modes in
# the naive patterns:
#
#   1. `ATTORNEY-CLIENT PRIVILEGED` without position anchoring caught
#      law-firm email-footer disclaimer boilerplate ("this email may contain
#      attorney-client privileged information") — factually wrong to
#      auto-tag as privileged since it's a reflexive disclaimer, not an
#      author-asserted designation. Opposing-counsel routine emails would
#      have been mis-classified as our own privileged documents.
#
#   2. `PRIVILEGED AND CONFIDENTIAL` with proximity-to-counsel produced zero
#      matches in the actual corpus — the phrase appears (on insurance
#      correspondence, NDAs, etc.) but not in proximity to "attorney" or
#      "counsel." Zero contribution with cognitive overhead; dropped.
#
# Refinements applied:
#   (a) Position anchor: match must occur within the first 500 chars of the
#       document. Header assertions pass; body/footer disclaimers fail.
#   (b) Qualifier disqualification: match context (±50 chars) must not
#       contain "may contain" / "may be" / "may include" — the hedging
#       language of a disclaimer versus the affirmative language of a
#       designation.
#
# Both patterns implied privilege='attorney_client' per §6.2. Work-product
# assertions are tagged the same — a finer-grained distinction (work_product_
# opinion vs fact) is the LLM classifier's job, not keyword pass.

KEYWORD_PATTERNS = {
    # Literal header assertion: "ATTORNEY-CLIENT PRIVILEGED" or "ATTORNEY CLIENT
    # PRIVILEGED" in the first 500 chars of the document.
    'attorney_client_literal': re.compile(
        r'\A.{0,500}?ATTORNEY[-\s]CLIENT\s+PRIVILEGED',
        re.IGNORECASE | re.DOTALL,
    ),
    # "WORK PRODUCT" as a document-header assertion (first 500 chars).
    'work_product_header': re.compile(
        r'\A.{0,500}?\bWORK\s+PRODUCT\b',
        re.IGNORECASE | re.DOTALL,
    ),
}

# Pattern → privilege classification mapping for the Task 4 mechanical pass.
#
# Per Will 2026-04-23: work_product_header → 'work_product_opinion'. Documents
# with a "WORK PRODUCT" header assertion in the first 500 chars are typically
# author-prepared analyses, memoranda, and master references — which are
# opinion work product by nature (containing mental impressions, conclusions,
# legal theories). Tagging these as 'work_product_fact' would under-classify:
# fact work product is discoverable on substantial-need showing, whereas
# opinion work product carries near-absolute protection. Under-classification
# of opinion-WP as fact-WP is not a safe failure — it would silently degrade
# the document's protection posture in every downstream workflow.
#
# Factual work product (witness-statement compilations, investigation
# materials without legal analysis) is the LLM-assisted phase's job to
# distinguish. The mechanical pass is high-precision header-assertion
# classification only.
PATTERN_TO_PRIVILEGE = {
    'attorney_client_literal': 'attorney_client',
    'work_product_header': 'work_product_opinion',
}

# Disqualifier: a privilege-ish phrase in proximity to one of these hedges is
# a disclaimer or a conditional assertion, not a designation. Applied to a
# ±50-char window around each candidate match.
#
# Three categories of disqualifiers:
#
#   1. Hedging disclaimers (may-contain class):
#        "may contain", "may be", "may include" — the language of email-
#        footer boilerplate that hedges because the sender doesn't know
#        whether any given outbound message actually contains privileged
#        content.
#
#   2. Conditional assertions (if/when class), added 2026-04-23 after the
#      FASA breach report surfaced a false positive with "Attorney-Client
#      Privileged IF REVIEWED BY COUNSEL" as a conditional header:
#        "if reviewed", "if shared", "if routed", "if sent", "if disclosed",
#        "if forwarded", "if produced" — the author is flagging that
#        privilege *could* attach downstream, not asserting that it
#        currently applies.
#        "when reviewed", "when shared" — same semantics, different
#        conjunction.
#
#   3. Subjunctive framings (should class):
#        "should this document", "should this communication", "should the
#        recipient" — conditional-subjunctive phrasings that hedge the
#        privilege assertion.
#
# Design principle captured in the closeout: false positives in the keyword
# pass are not recoverable by downstream processing — the LLM-assisted phase
# operates on NULL-privilege rows only and does not revisit keyword-
# classified documents. Pattern refinements must be evaluated against that
# standard.
_QUALIFIER_DISQUALIFIERS = re.compile(
    r'may\s+(?:contain|be|include)'
    r'|\bif\s+(?:reviewed|shared|routed|sent|disclosed|forwarded|produced)'
    r'|\bwhen\s+(?:reviewed|shared)'
    r'|\bif/when\b'
    r'|should\s+th(?:is|e)\s+(?:document|communication|recipient)',
    re.IGNORECASE,
)

_KEYWORD_CONTEXT_WINDOW = 50  # chars either side of match for disqualifier check


def classify_by_keywords(content: Optional[str]) -> Optional[dict]:
    """Deterministic keyword classifier for Task 4 §6.2 pre-pass.

    Returns None if no pattern matches (or all matches disqualified), or a
    dict with {privilege, pattern_matched, match_span, context_preview}
    describing the classification.

    Implementation:
      1. For each KEYWORD_PATTERNS entry, search in the document content.
      2. If a pattern matches, extract a ±50-char context window around the
         match; if that window contains a disqualifier phrase ('may contain'
         / 'may be' / 'may include'), the match is treated as a disclaimer,
         not a designation — skip to the next pattern.
      3. First non-disqualified match wins. Returns immediately.
      4. All keyword matches imply privilege='attorney_client' per §6.2.
    """
    if not content:
        return None
    for pattern_name, pattern in KEYWORD_PATTERNS.items():
        m = pattern.search(content)
        if not m:
            continue
        # Disqualifier check in a window around the match
        win_start = max(0, m.start() - _KEYWORD_CONTEXT_WINDOW)
        win_end = min(len(content), m.end() + _KEYWORD_CONTEXT_WINDOW)
        window = content[win_start:win_end]
        if _QUALIFIER_DISQUALIFIERS.search(window):
            continue  # disclaimer-shape; not a designation
        # Return the first non-disqualified match
        preview_start = max(0, m.start() - 200)
        preview_end = min(len(content), m.end() + 200)
        return {
            'privilege': PATTERN_TO_PRIVILEGE[pattern_name],
            'pattern_matched': pattern_name,
            'match_span': (m.start(), m.end()),
            'matched_text': m.group(0)[-80:] if len(m.group(0)) > 80 else m.group(0),
            'context_preview': content[preview_start:preview_end],
        }
    return None


def scan_corpus_for_match_delta(
    content: str,
) -> tuple[Optional[str], Optional[str]]:
    """Pre-refinement vs post-refinement classifier for delta reporting.

    Returns (old_pattern_name_or_None, new_pattern_name_or_None) describing
    which pattern caught this content under the permissive (pre-refinement)
    versus the strict (post-refinement) pass. Used by the sanity-check
    reporter to identify documents removed by the refinement.
    """
    if not content:
        return None, None
    # OLD permissive patterns (the v1 set from first sanity check)
    old_attorney_client = re.compile(
        r'ATTORNEY[-\s]CLIENT\s+PRIVILEGED', re.IGNORECASE,
    )
    old_work_product = KEYWORD_PATTERNS['work_product_header']  # unchanged
    old_hit = None
    if old_attorney_client.search(content):
        old_hit = 'attorney_client_literal'
    elif old_work_product.search(content):
        old_hit = 'work_product_header'
    new = classify_by_keywords(content)
    new_hit = new['pattern_matched'] if new else None
    return old_hit, new_hit


# ============================================================
# Parameter validation
# ============================================================

def _validate_privilege_parameters(
    include_privileged: bool,
    privileged_categories: Optional[list[str]],
) -> None:
    if privileged_categories is not None and not include_privileged:
        raise ValueError(
            "privileged_categories provided without include_privileged=True; "
            "pass include_privileged=True explicitly or remove privileged_categories"
        )
    if privileged_categories is not None:
        invalid = set(privileged_categories) - LEGAL_PRIVILEGED_EXTENDED
        if invalid:
            raise ValueError(
                f"privileged_categories contains invalid values {sorted(invalid)}; "
                f"valid extended values are {sorted(LEGAL_PRIVILEGED_EXTENDED)}"
            )


def _resolve_legal_allowed(
    include_privileged: bool,
    privileged_categories: Optional[list[str]],
) -> frozenset[str]:
    if not include_privileged:
        return LEGAL_DEFAULT_ALLOWED
    base = LEGAL_DEFAULT_ALLOWED | LEGAL_PRIVILEGED_BASE
    if privileged_categories:
        base = base | frozenset(privileged_categories)
    return base


# ============================================================
# Authorization lookup
# ============================================================

@dataclass
class _AuthorizationState:
    id: UUID
    scope: str
    active: bool
    reason_if_inactive: Optional[str]


async def _resolve_authorization(
    conn: asyncpg.Connection,
    authorization_id: UUID,
    authorizations_table: str = 'medical.authorizations',
) -> _AuthorizationState:
    row = await conn.fetchrow(
        f"SELECT id, scope, revoked_at, expiration_date "
        f"FROM {authorizations_table} WHERE id = $1",
        authorization_id,
    )
    if row is None:
        raise PHIAccessDeniedException(f"authorization {authorization_id} not found")
    if row['revoked_at'] is not None:
        return _AuthorizationState(
            id=row['id'], scope=row['scope'],
            active=False,
            reason_if_inactive=f"revoked at {row['revoked_at']}",
        )
    if row['expiration_date'] is not None and row['expiration_date'] < date.today():
        return _AuthorizationState(
            id=row['id'], scope=row['scope'],
            active=False,
            reason_if_inactive=f"expired {row['expiration_date']}",
        )
    return _AuthorizationState(
        id=row['id'], scope=row['scope'],
        active=True, reason_if_inactive=None,
    )


# ============================================================
# Access logger (default + tests can subclass / inject)
# ============================================================

class AccessLogger:
    """Writes retrieval events to ops.access_log and medical.disclosures_log.

    Tests pass a subclass or mock that captures calls for assertions. Table
    names can be overridden for integration tests that need to avoid
    polluting the production audit tables.
    """

    def __init__(
        self,
        conn: asyncpg.Connection,
        *,
        access_log_table: str = 'ops.access_log',
        disclosures_table: str = 'medical.disclosures_log',
    ) -> None:
        self.conn = conn
        self.access_log_table = access_log_table
        self.disclosures_table = disclosures_table

    async def log_access(
        self,
        *,
        document_ids: list[str],
        result_count: int,
        domain: Optional[str],
        caller_context: dict,
        purpose_of_use: Optional[str] = None,
        include_privileged: Optional[bool] = None,
        privileged_categories: Optional[list[str]] = None,
        authorization_id: Optional[str] = None,
        outcome: str = 'surfaced',
    ) -> None:
        await self.conn.execute(
            f"""
            INSERT INTO {self.access_log_table}
                (timestamp, agent_id, session_id, tool_name, action, domain,
                 result_count, metadata)
            VALUES (now(), $1, $2, $3, $4, $5, $6, $7::jsonb)
            """,
            caller_context.get('agent_id') or caller_context.get('user') or 'unknown',
            caller_context.get('session_id'),
            caller_context.get('tool') or 'unknown',
            f"document_filter_{outcome}",
            domain,
            result_count,
            json.dumps({
                'document_ids': document_ids[:200],  # cap to avoid huge logs
                'purpose_of_use': purpose_of_use,
                'include_privileged': include_privileged,
                'privileged_categories': privileged_categories,
                'authorization_id': authorization_id,
                'caller_context': caller_context,
                'outcome': outcome,
            }),
        )

    async def log_count_only(
        self,
        *,
        domain: Optional[str],
        caller_context: dict,
        result_count: int,
    ) -> None:
        await self.conn.execute(
            f"""
            INSERT INTO {self.access_log_table}
                (timestamp, agent_id, session_id, tool_name, action, domain,
                 result_count, metadata)
            VALUES (now(), $1, $2, $3, 'document_filter_count_only', $4, $5, $6::jsonb)
            """,
            caller_context.get('agent_id') or caller_context.get('user') or 'unknown',
            caller_context.get('session_id'),
            caller_context.get('tool') or 'unknown',
            domain,
            result_count,
            json.dumps({'caller_context': caller_context, 'mode': 'count_only'}),
        )

    async def log_disclosure(
        self,
        *,
        document_id: str,
        patient_id: Optional[int],
        disclosed_to: str,
        purpose_of_use: str,
        authorization_id: Optional[UUID],
        disclosed_by: Optional[str],
    ) -> None:
        await self.conn.execute(
            f"""
            INSERT INTO {self.disclosures_table}
                (document_id, patient_id, disclosed_to, purpose_of_use,
                 authorization_id, disclosed_by)
            VALUES ($1::uuid, $2, $3, $4, $5, $6)
            """,
            document_id, patient_id, disclosed_to,
            purpose_of_use, authorization_id, disclosed_by,
        )


# ============================================================
# Fragment builder
# ============================================================

async def build_document_safety_filter(
    *,
    conn: asyncpg.Connection,
    domain: Optional[str],
    caller_context: dict,
    purpose_of_use: Optional[str] = None,
    include_privileged: bool = False,
    privileged_categories: Optional[list[str]] = None,
    authorization_id: Optional[UUID] = None,
    mode: Literal['content', 'count_only'] = 'content',
    table_alias: str = 'd',
    next_param_index: int = 1,
    logger: Optional[AccessLogger] = None,
    authorizations_table: str = 'medical.authorizations',
) -> tuple[str, list[Any], Callable[[list[Any]], Awaitable[None]]]:
    """Build (where_clause_fragment, params, log_callback) per §5.3/§5.4."""
    _validate_privilege_parameters(include_privileged, privileged_categories)
    if mode not in ('content', 'count_only'):
        raise ValueError(f"mode must be 'content' or 'count_only', got {mode!r}")
    if domain not in (None, 'legal', 'medical'):
        raise ValueError(
            f"domain must be 'legal', 'medical', or None; got {domain!r}"
        )

    logger = logger if logger is not None else AccessLogger(conn)
    auth_state: Optional[_AuthorizationState] = None
    if authorization_id is not None:
        auth_state = await _resolve_authorization(
            conn, authorization_id, authorizations_table,
        )

    # Count-only mode: no safety filter on the fragment; aggregate-only logging.
    if mode == 'count_only':
        async def log_aggregate(rows: list[Any]) -> None:
            await logger.log_count_only(
                domain=domain,
                caller_context=caller_context,
                result_count=_aggregate_count(rows),
            )
        return "TRUE", [], log_aggregate

    # Content mode
    a = table_alias
    p = next_param_index
    params: list[Any] = []

    if domain == 'legal':
        allowed = sorted(_resolve_legal_allowed(include_privileged, privileged_categories))
        clause = f"{a}.privilege = ANY(${p}::text[])"
        params.append(allowed)
        p += 1

    elif domain == 'medical':
        med_parts, med_params, p = _build_medical_parts(a, p, purpose_of_use, auth_state)
        params.extend(med_params)
        clause = " AND ".join(med_parts) if med_parts else "TRUE"

    else:  # domain is None — cross-domain
        # Per v2.2 §5.4 headline intent + checkpoint clarification: apply BOTH
        # filters to every row. Legal-domain rows get privilege filter; medical
        # rows get phi filter; non-scope domains (web, business, personal,
        # paperless, coding, general, research, ...) require privilege to be
        # NULL or 'none' AND phi_status to be NULL or not in (phi, lds). This
        # catches mis-domained privileged/PHI content without requiring the
        # caller to enumerate every non-scope domain.
        allowed = sorted(_resolve_legal_allowed(include_privileged, privileged_categories))
        legal_expr = f"{a}.privilege = ANY(${p}::text[])"
        params.append(allowed)
        p += 1
        med_parts, med_params, p = _build_medical_parts(a, p, purpose_of_use, auth_state)
        params.extend(med_params)
        medical_expr = " AND ".join(med_parts) if med_parts else "TRUE"
        # Non-scope: strict default-safe. Mis-classified privileged content stays blocked.
        nonscope_expr = (
            f"({a}.privilege IS NULL OR {a}.privilege = 'none') "
            f"AND ({a}.phi_status IS NULL OR {a}.phi_status NOT IN ('phi', 'limited_data_set'))"
        )
        clause = (
            f"(({a}.domain = 'legal' AND {legal_expr}) "
            f"OR ({a}.domain = 'medical' AND ({medical_expr})) "
            f"OR ({a}.domain NOT IN ('legal', 'medical') AND ({nonscope_expr})))"
        )

    # Closure capturing the parameters for logging
    log_closure_kwargs = dict(
        domain=domain,
        caller_context=caller_context,
        purpose_of_use=purpose_of_use,
        include_privileged=include_privileged,
        privileged_categories=privileged_categories,
        authorization_id=str(authorization_id) if authorization_id else None,
    )

    async def log_surfaced(rows: list[Any]) -> None:
        doc_ids = [_extract_doc_id(r) for r in rows]
        await logger.log_access(
            document_ids=doc_ids,
            result_count=len(rows),
            outcome='surfaced',
            **log_closure_kwargs,
        )
        # Log a disclosure row for each PHI document surfaced (medical + purpose_of_use)
        if purpose_of_use and domain in ('medical', None):
            disclosed_to = (
                caller_context.get('user') or caller_context.get('agent_id') or 'unknown'
            )
            for r in rows:
                if _row_has_phi(r):
                    await logger.log_disclosure(
                        document_id=_extract_doc_id(r),
                        patient_id=_extract_patient_id(r),
                        disclosed_to=disclosed_to,
                        purpose_of_use=purpose_of_use,
                        authorization_id=authorization_id,
                        disclosed_by=caller_context.get('user'),
                    )

    return clause, params, log_surfaced


def _build_medical_parts(
    alias: str,
    next_param: int,
    purpose_of_use: Optional[str],
    auth_state: Optional[_AuthorizationState],
) -> tuple[list[str], list[Any], int]:
    """Construct (clauses, params, next_param_after) for medical domain rows."""
    p = next_param
    params: list[Any] = []
    parts: list[str] = []

    # phi_status allow-list widens with purpose_of_use
    allowed = set(MEDICAL_DEFAULT_ALLOWED)
    if purpose_of_use is not None:
        allowed |= MEDICAL_WITH_PURPOSE_ADDITIONAL
    parts.append(f"{alias}.phi_status = ANY(${p}::text[])")
    params.append(sorted(allowed))
    p += 1

    # Heightened categories blocked by default; unlocked by matching auth scope
    blocked = set(HEIGHTENED_CATEGORIES)
    if auth_state is not None and auth_state.active and auth_state.scope in blocked:
        blocked.discard(auth_state.scope)
    if blocked:
        parts.append(f"NOT ({alias}.phi_categories && ${p}::text[])")
        params.append(sorted(blocked))
        p += 1

    # minor_patient=true requires explicit purpose_of_use
    if purpose_of_use is None:
        parts.append(f"{alias}.minor_patient = false")

    return parts, params, p


# ============================================================
# Hardened fetch-by-ID
# ============================================================

async def fetch_safe_document(
    doc_id: UUID,
    *,
    conn: asyncpg.Connection,
    caller_context: dict,
    purpose_of_use: Optional[str] = None,
    include_privileged: bool = False,
    privileged_categories: Optional[list[str]] = None,
    authorization_id: Optional[UUID] = None,
    logger: Optional[AccessLogger] = None,
    documents_table: str = 'core.documents',
    authorizations_table: str = 'medical.authorizations',
) -> dict:
    """Fetch one document by id; raise on denial. Always logs access."""
    _validate_privilege_parameters(include_privileged, privileged_categories)
    logger = logger if logger is not None else AccessLogger(conn)

    auth_state: Optional[_AuthorizationState] = None
    if authorization_id is not None:
        auth_state = await _resolve_authorization(
            conn, authorization_id, authorizations_table,
        )

    doc = await conn.fetchrow(
        f"SELECT * FROM {documents_table} WHERE id = $1", doc_id,
    )

    async def _log(outcome: str, doc_domain: Optional[str]) -> None:
        await logger.log_access(
            document_ids=[str(doc_id)],
            result_count=(1 if outcome == 'surfaced' else 0),
            domain=doc_domain,
            caller_context=caller_context,
            purpose_of_use=purpose_of_use,
            include_privileged=include_privileged,
            privileged_categories=privileged_categories,
            authorization_id=str(authorization_id) if authorization_id else None,
            outcome=outcome,
        )

    if doc is None:
        await _log('not_found', None)
        raise PrivilegeDeniedException(f"document {doc_id} not found")

    # Legal privilege enforcement
    if doc['domain'] == 'legal':
        allowed = _resolve_legal_allowed(include_privileged, privileged_categories)
        priv = doc['privilege']
        if priv not in allowed:
            await _log('denied_privilege', 'legal')
            raise PrivilegeDeniedException(
                f"document {doc_id} has privilege={priv!r}, not in allowed set "
                f"(include_privileged={include_privileged}, "
                f"privileged_categories={privileged_categories})"
            )

    # Medical/PHI enforcement
    if doc['domain'] == 'medical':
        allowed_phi = set(MEDICAL_DEFAULT_ALLOWED)
        if purpose_of_use is not None:
            allowed_phi |= MEDICAL_WITH_PURPOSE_ADDITIONAL
        if doc['phi_status'] not in allowed_phi:
            await _log('denied_phi', 'medical')
            raise PHIAccessDeniedException(
                f"document {doc_id} has phi_status={doc['phi_status']!r}, not allowed "
                f"(purpose_of_use={purpose_of_use!r})"
            )

        # Heightened-protection categories
        categories = set(doc['phi_categories'] or [])
        blocked = HEIGHTENED_CATEGORIES & categories
        if auth_state is not None and auth_state.active:
            blocked -= {auth_state.scope}
        if blocked:
            await _log('denied_heightened', 'medical')
            raise PHIAccessDeniedException(
                f"document {doc_id} has heightened categories {sorted(blocked)} "
                f"not unlocked by authorization {authorization_id}"
            )

        if doc['minor_patient'] and purpose_of_use is None:
            await _log('denied_minor', 'medical')
            raise PHIAccessDeniedException(
                f"document {doc_id} is minor_patient=true; requires explicit purpose_of_use"
            )

    await _log('surfaced', doc['domain'])

    # Disclosure log for PHI surfaced with purpose_of_use
    if (doc['domain'] == 'medical'
            and purpose_of_use is not None
            and doc['phi_status'] in ('phi', 'limited_data_set')):
        await logger.log_disclosure(
            document_id=str(doc_id),
            patient_id=None,  # caller may enrich via separate call if patient linkage is known
            disclosed_to=(
                caller_context.get('user') or caller_context.get('agent_id') or 'unknown'
            ),
            purpose_of_use=purpose_of_use,
            authorization_id=authorization_id,
            disclosed_by=caller_context.get('user'),
        )

    return dict(doc)


# ============================================================
# Morning-brief wrapper
# ============================================================

async def build_morning_brief_safety_filter(
    *,
    conn: asyncpg.Connection,
    domain: Optional[str],
    caller_context: dict,
    mode: Literal['content', 'count_only'] = 'content',
    table_alias: str = 'd',
    next_param_index: int = 1,
    logger: Optional[AccessLogger] = None,
    authorizations_table: str = 'medical.authorizations',
) -> tuple[str, list[Any], Callable[[list[Any]], Awaitable[None]]]:
    """Morning-brief locked defaults per §5.5.

    Calls base helper with include_privileged=True and
    purpose_of_use='morning_brief' (so PHI metadata can be surfaced by
    trusted caller code that restricts to non-content fields), then
    augments:
      - heightened protection ALWAYS excluded regardless of authorization
        (achieved by passing authorization_id=None to base)
      - minor_patient=true ALWAYS excluded (brief never surfaces minors;
        base helper relaxes this when purpose_of_use is set, so we re-add
        the exclusion as an augmentation)

    Direct use of the content-mode base filter from morning_briefing.py is
    forbidden. morning_briefing.py must import and call this wrapper only.
    """
    clause, params, log = await build_document_safety_filter(
        conn=conn,
        domain=domain,
        caller_context={**caller_context, 'surface': 'morning_brief'},
        purpose_of_use='morning_brief',
        include_privileged=True,
        privileged_categories=None,  # brief never sees work_product_opinion / joint_defense / common_interest
        authorization_id=None,       # heightened categories stay blocked
        mode=mode,
        table_alias=table_alias,
        next_param_index=next_param_index,
        logger=logger,
        authorizations_table=authorizations_table,
    )

    if mode == 'count_only':
        return clause, params, log

    a = table_alias
    if domain == 'medical':
        clause = f"({clause}) AND {a}.minor_patient = false"
    elif domain is None:
        clause = f"({clause}) AND ({a}.domain != 'medical' OR {a}.minor_patient = false)"
    # For domain='legal' no augmentation needed

    return clause, params, log


# ============================================================
# Row-extraction helpers
# ============================================================

def _aggregate_count(rows: list[Any]) -> int:
    """Sum aggregate 'count' fields for count_only mode; len() fallback."""
    total = 0
    for r in rows:
        try:
            total += r['count']
        except (KeyError, TypeError, IndexError):
            total += 1
    return total


def _extract_doc_id(row: Any) -> str:
    try:
        val = row['id']
    except (KeyError, TypeError):
        val = None
    return str(val) if val is not None else ''


def _extract_patient_id(row: Any) -> Optional[int]:
    try:
        val = row['patient_id']
    except (KeyError, TypeError):
        return None
    return int(val) if val is not None else None


def _row_has_phi(row: Any) -> bool:
    try:
        status = row['phi_status']
    except (KeyError, TypeError):
        return False
    return status in ('phi', 'limited_data_set')
