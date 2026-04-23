# SPRINT A — Critical Privilege and Privacy Gates (v2)

**Work Order ID:** `SPRINT_A_PRIVILEGE_PHI`
**Reference:** Athena Strategic Framework v2.0, Section 9.1
**Supersedes:** SPRINT_A_PRIVILEGE_PHI (v1 draft)
**Target Duration:** 14 days
**Status:** DRAFT — awaiting human approval to execute

**Executing Agent:** `agent_athena` (primary), with `agent_code_doctor` for verification

---

## 0. Changelog from v1

- Corrected `ops.migrations_applied` INSERT shape. Migration runner loads SQL files from disk and populates `sql_forward`/`sql_rollback` (§7.1 of preflight).
- Stop-gate convention updated to `ops.pending_approvals.approved = true` filtered by `decision_type` string (§7.2).
- Agent-run records use `trigger` field, not `run_type` (§7.3).
- Agent findings use existing severity vocabulary (`critical`) plus a `category` tag (§7.4).
- `wdws-agents.service` health is now gated per-task instead of a hard global prerequisite (§7.5); synchronous Task 1–3 work does not require it.
- Working tree must be clean before branching; `sprint-a-privilege-phi` branches from a clean `master`, not `main` (§7.6, §7.9).
- Task 3 re-pointed from `contextual_retrieval.py` (chunk-enrichment, not retrieval) to four real retrieval surfaces (§7.7).
- Task 3 scope is **(b+)**: MCP + dashboard live tools + `morning_briefing.py` + `legal_module.py`. Agent-internal reads deferred to Sprint A.5.
- Task 3 architecture is a central helper returning composable WHERE-clause fragments, plus a hardened `fetch_safe_document` for bypass paths.
- Task 4 adds a deterministic keyword-auto-classification pre-pass as first action (209 assertion-header legal documents).
- Closeout (§12) names Sprint A.5 scope and flags the two-parallel-MCP-tool-sets anti-pattern.

---

## 1. Context

Version 2.0 of the Strategic Framework audit identified four ship-blocking gaps between Athena's current state and its claimed capability as a privilege-aware, HIPAA-defensible professional workflow platform:

1. `core.documents` has no privilege flag (attorney-client, work product, etc.)
2. `core.documents` has no HIPAA PHI classification
3. No heightened-protection flags for 42 CFR Part 2 records or psychotherapy notes
4. `legal.cases` lacks first-class fields for assigned judge and opposing firm

Sprint A closes all four gaps. Work is primarily schema additions plus filtering at the retrieval layer. Every change is additive — no existing columns are dropped or repurposed.

**Risk posture:** HIGH. This sprint touches the retrieval layer for production legal and medical data. Every schema change is backed up, verified in a shadow schema before application, and accompanied by a rollback migration. No retrieval behavior changes until human review approves.

**Headline claim of Sprint A:** every retrieval result *surfaced to humans or external systems* is filtered by privilege and PHI status, and every surfacing of privileged or PHI content is logged to an immutable audit trail. (Agent-internal reads are Sprint A.5 scope — see §12.)

---

## 2. Pre-Flight Checklist

Agent verifies every item before proceeding. Verification is recorded in `ops.agent_runs` with `trigger='sprint_a_preflight'`.

- [ ] Database full dump exists dated within last 24 hours. Target location: `/opt/wdws/backups/pre_sprint_a/` or the latest `/opt/wdws/backups/wdws_YYYYMMDD_*.dump`.
- [ ] `master` branch working tree is clean. All 9 modified tracked files and 23 untracked files observed at preflight draft time are either committed with clear intent, stashed with a recoverable label, or explicitly deleted. Untracked migration files (023, 024, 025) are investigated first — their schema is already in the live database; they should have been committed.
- [ ] New branch `sprint-a-privilege-phi` is created from a clean `master`.
- [ ] Shadow schemas `legal_shadow`, `medical_shadow`, `core_shadow` do not currently exist.
- [ ] `ops.migrations_applied` is queryable and accepts the corrected INSERT shape (verified by a test INSERT inside a transaction that is rolled back).
- [ ] `pgcrypto` extension installed and `gen_random_uuid()` returns a UUID.
- [ ] Athena is quiescent (no running ingestion job).

**NOT a preflight requirement for Tasks 1–3:** `wdws-agents.service` health. See §10 rule 7 for the per-task gate.

**Stop-gate: PREFLIGHT**
If any item fails, agent halts and writes a finding to `ops.agent_findings` with `severity='critical'` and `category='sprint_a_blocker'`. Human operator must resolve or approve override before proceeding.

---

## 3. Task 1 — Draft Migration 026 (Legal Privilege + Judge Fields)

### 3.1 Objective

Add privilege classification, confidentiality tier, and protective order linkage to `core.documents`. Add `assigned_judge` and `opposing_firm` to `legal.cases`. Create `legal.protective_orders` table. All additive.

### 3.2 Draft SQL

Agent writes to `/opt/wdws/migrations/026_legal_privilege_judge.sql`:

```sql
-- Migration 026: Legal privilege, confidentiality, protective orders, judge fields
-- Sprint A: Critical Privilege and Privacy Gates
-- Additive only. No drops. No data loss.

BEGIN;

-- ---------- Part A: Privilege classification on core.documents ----------

ALTER TABLE core.documents
    ADD COLUMN privilege text,
    ADD COLUMN privilege_classified_at timestamp with time zone,
    ADD COLUMN privilege_classified_by text;

ALTER TABLE core.documents
    ADD CONSTRAINT documents_privilege_check
    CHECK (privilege IS NULL OR privilege IN (
        'attorney_client', 'work_product_opinion', 'work_product_fact',
        'joint_defense', 'common_interest', 'none', 'unknown'
    ));

COMMENT ON COLUMN core.documents.privilege IS
    'Privilege status. NULL = not yet classified. Retrieval layer treats NULL as potentially privileged until classified.';

-- ---------- Part B: Confidentiality tier ----------

ALTER TABLE core.documents
    ADD COLUMN confidentiality text;

ALTER TABLE core.documents
    ADD CONSTRAINT documents_confidentiality_check
    CHECK (confidentiality IS NULL OR confidentiality IN (
        'public', 'confidential', 'aeo', 'under_seal', 'highly_confidential'
    ));

-- ---------- Part C: Protective orders table ----------

CREATE TABLE legal.protective_orders (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    case_id integer NOT NULL REFERENCES legal.cases(id) ON DELETE CASCADE,
    order_document_id uuid REFERENCES core.documents(id),
    effective_date date NOT NULL,
    tiers text[] NOT NULL DEFAULT '{}',
    notes text,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);
CREATE INDEX idx_protective_orders_case ON legal.protective_orders(case_id);

ALTER TABLE core.documents
    ADD COLUMN protective_order_id uuid REFERENCES legal.protective_orders(id);

-- ---------- Part D: Judge and opposing firm on cases ----------

ALTER TABLE legal.cases
    ADD COLUMN assigned_judge text,
    ADD COLUMN opposing_firm text;

CREATE INDEX idx_cases_assigned_judge
    ON legal.cases(assigned_judge) WHERE assigned_judge IS NOT NULL;

-- ---------- Part E: Indexes for privilege-aware retrieval ----------

CREATE INDEX idx_documents_privilege
    ON core.documents(privilege) WHERE privilege IS NOT NULL;
CREATE INDEX idx_documents_confidentiality
    ON core.documents(confidentiality) WHERE confidentiality IS NOT NULL;

-- ---------- Part F: Safe-retrieval view ----------

CREATE OR REPLACE VIEW core.v_documents_safe_retrieval AS
    SELECT * FROM core.documents
    WHERE privilege = 'none' AND purged_at IS NULL;

COMMENT ON VIEW core.v_documents_safe_retrieval IS
    'Documents explicitly classified non-privileged and not purged. Default view for non-privileged retrieval contexts. NULL-privilege documents are excluded until classified.';

COMMIT;
```

**Note on migration bookkeeping.** Unlike v1, this SQL file does NOT contain an `INSERT INTO ops.migrations_applied`. That insert is performed by the migration runner (new or existing) after the file executes successfully, reading the full SQL body and the rollback body from their files on disk. Concretely:

```python
# Pseudo-code for the runner (to be implemented or extended)
async def apply_migration(conn, forward_path: Path, rollback_path: Path, approved_by: str):
    forward_sql = forward_path.read_text()
    rollback_sql = rollback_path.read_text()
    t0 = time.monotonic()
    async with conn.transaction():
        await conn.execute(forward_sql)
        duration_ms = int((time.monotonic() - t0) * 1000)
        await conn.execute("""
            INSERT INTO ops.migrations_applied
                (migration_name, sql_forward, sql_rollback, applied_by,
                 status, duration_ms, analysis)
            VALUES ($1, $2, $3, $4, 'applied', $5, $6)
        """, forward_path.stem, forward_sql, rollback_sql,
             approved_by, duration_ms,
             'Sprint A: privilege, confidentiality, protective_orders, assigned_judge, opposing_firm. Additive only.')
```

`approved_by` is the `approved_by` string from the matching `ops.pending_approvals` row. For this sprint, a minimal runner that does just this is acceptable; a full migration-framework rewrite is out of scope.

### 3.3 Rollback SQL

Agent writes to `/opt/wdws/migrations/026_legal_privilege_judge_down.sql`:

```sql
BEGIN;

DROP VIEW IF EXISTS core.v_documents_safe_retrieval;

DROP INDEX IF EXISTS core.idx_documents_confidentiality;
DROP INDEX IF EXISTS core.idx_documents_privilege;
DROP INDEX IF EXISTS legal.idx_cases_assigned_judge;

ALTER TABLE core.documents DROP COLUMN IF EXISTS protective_order_id;
DROP TABLE IF EXISTS legal.protective_orders;

ALTER TABLE legal.cases DROP COLUMN IF EXISTS opposing_firm;
ALTER TABLE legal.cases DROP COLUMN IF EXISTS assigned_judge;

ALTER TABLE core.documents DROP CONSTRAINT IF EXISTS documents_confidentiality_check;
ALTER TABLE core.documents DROP COLUMN IF EXISTS confidentiality;

ALTER TABLE core.documents DROP CONSTRAINT IF EXISTS documents_privilege_check;
ALTER TABLE core.documents DROP COLUMN IF EXISTS privilege_classified_by;
ALTER TABLE core.documents DROP COLUMN IF EXISTS privilege_classified_at;
ALTER TABLE core.documents DROP COLUMN IF EXISTS privilege;

COMMIT;
```

Rollback is recorded by the runner updating the `ops.migrations_applied` row to `status='rolled_back'`, `rolled_back_at=now()`, `rolled_back_by=<approver>`.

### 3.4 Shadow-Schema Verification

Before applying to production:

1. Create `legal_shadow` and `core_shadow` schemas.
2. Replicate `core.documents` and `legal.cases` structure into shadow (`CREATE TABLE ... LIKE core.documents INCLUDING ALL`).
3. Apply migration (modified to reference shadow schemas) to shadow tables. Verify no errors.
4. Sample queries:
   - `SELECT COUNT(*) FROM core_shadow.documents WHERE privilege IS NULL` equals full row count (no defaults applied).
   - Insert one test row per valid `privilege` value — all succeed.
   - Insert a row with invalid `privilege='not_a_value'` — CHECK constraint rejects.
5. Drop shadow schemas.
6. Log verification outcome to `ops.agent_findings` with `severity='info'` and `category='sprint_a_t1_verification'`.

**Stop-gate: T1_VERIFICATION**
Agent creates an entry in `ops.pending_approvals` with `decision_type='sprint_a_t1_verification'`, `risk_level='high'`, `sql_preview=<026 body>`, `rollback_sql=<rollback body>`. Agent polls `WHERE decision_type='sprint_a_t1_verification' AND approved IS NOT NULL`; human sets `approved=true` (or `false` + `rejection_note`). On rejection, agent halts and does not retry.

### 3.5 Production Application

On approval:
1. Migration runner opens transaction, reads both SQL files, applies forward, inserts `ops.migrations_applied` row per §3.2, commits.
2. Post-application sanity: all new columns queryable; `\d core.documents` and `\d legal.cases` show the additions.

On any error inside the transaction: ROLLBACK, write finding with `severity='critical'`, `category='sprint_a_blocker'`, halt.

---

## 4. Task 2 — Draft Migration 027 (Medical PHI Classification)

### 4.1 Objective

Add PHI classification and heightened-protection flags to `core.documents`. Create `medical.authorizations` and `medical.disclosures_log` tables for HIPAA accounting-of-disclosures substrate. All additive.

### 4.2 Draft SQL

Agent writes to `/opt/wdws/migrations/027_medical_phi_classification.sql`:

```sql
-- Migration 027: Medical PHI classification and HIPAA tracking
-- Sprint A: Critical Privilege and Privacy Gates
-- Additive only. No drops.

BEGIN;

-- ---------- Part A: PHI classification on core.documents ----------

ALTER TABLE core.documents
    ADD COLUMN phi_status text,
    ADD COLUMN phi_categories text[] DEFAULT '{}',
    ADD COLUMN minor_patient boolean DEFAULT false,
    ADD COLUMN phi_classified_at timestamp with time zone,
    ADD COLUMN phi_classified_by text;

ALTER TABLE core.documents
    ADD CONSTRAINT documents_phi_status_check
    CHECK (phi_status IS NULL OR phi_status IN (
        'phi', 'limited_data_set', 'safe_harbor_deidentified',
        'expert_determination_deidentified', 'not_phi'
    ));

COMMENT ON COLUMN core.documents.phi_status IS
    'HIPAA classification. NULL = not yet classified. Retrieval treats NULL in medical domain as PHI until classified.';

COMMENT ON COLUMN core.documents.phi_categories IS
    'Heightened-protection categories. Valid values: sud_42_cfr_part_2, psychotherapy_notes, genetic_gina, mental_health, hiv_aids.';

-- ---------- Part B: Authorization tracking ----------

CREATE TABLE medical.authorizations (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    patient_id integer NOT NULL REFERENCES medical.patients(id),
    purpose_of_use text NOT NULL,
    scope text NOT NULL,
    effective_date date NOT NULL,
    expiration_date date,
    revoked_at timestamp with time zone,
    authorization_document_id uuid REFERENCES core.documents(id),
    notes text,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);
CREATE INDEX idx_auth_patient ON medical.authorizations(patient_id);
CREATE INDEX idx_auth_active ON medical.authorizations(patient_id)
    WHERE revoked_at IS NULL AND (expiration_date IS NULL OR expiration_date >= CURRENT_DATE);

ALTER TABLE core.documents
    ADD COLUMN phi_authorization_id uuid REFERENCES medical.authorizations(id);

-- ---------- Part C: HIPAA accounting-of-disclosures log ----------

CREATE TABLE medical.disclosures_log (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id uuid NOT NULL REFERENCES core.documents(id),
    patient_id integer REFERENCES medical.patients(id),
    disclosed_to text NOT NULL,
    purpose_of_use text NOT NULL,
    disclosure_date timestamp with time zone NOT NULL DEFAULT now(),
    authorization_id uuid REFERENCES medical.authorizations(id),
    disclosed_by text,
    notes text,
    created_at timestamp with time zone DEFAULT now()
);
CREATE INDEX idx_disclosures_document ON medical.disclosures_log(document_id);
CREATE INDEX idx_disclosures_patient ON medical.disclosures_log(patient_id);
CREATE INDEX idx_disclosures_date ON medical.disclosures_log(disclosure_date);

COMMENT ON TABLE medical.disclosures_log IS
    'HIPAA 45 CFR 164.528 accounting of disclosures. Every disclosure of PHI outside TPO is logged here.';

-- ---------- Part D: Indexes for PHI-aware retrieval ----------

CREATE INDEX idx_documents_phi_status
    ON core.documents(phi_status) WHERE phi_status IS NOT NULL;
CREATE INDEX idx_documents_phi_categories
    ON core.documents USING GIN(phi_categories) WHERE phi_categories <> '{}';
CREATE INDEX idx_documents_minor_patient
    ON core.documents(minor_patient) WHERE minor_patient = true;

-- ---------- Part E: Safe-retrieval view for medical ----------

CREATE OR REPLACE VIEW core.v_medical_safe_retrieval AS
    SELECT * FROM core.documents
    WHERE domain = 'medical'
      AND phi_status IN ('safe_harbor_deidentified', 'expert_determination_deidentified', 'not_phi')
      AND purged_at IS NULL;

COMMENT ON VIEW core.v_medical_safe_retrieval IS
    'Medical documents safe for non-clinical retrieval contexts. Excludes raw PHI and unclassified documents.';

COMMIT;
```

Bookkeeping insert performed by the migration runner, per §3.2.

### 4.3 Rollback SQL

Agent writes to `/opt/wdws/migrations/027_medical_phi_classification_down.sql`:

```sql
BEGIN;

DROP VIEW IF EXISTS core.v_medical_safe_retrieval;

DROP INDEX IF EXISTS core.idx_documents_minor_patient;
DROP INDEX IF EXISTS core.idx_documents_phi_categories;
DROP INDEX IF EXISTS core.idx_documents_phi_status;

ALTER TABLE core.documents DROP COLUMN IF EXISTS phi_authorization_id;
DROP TABLE IF EXISTS medical.disclosures_log;
DROP TABLE IF EXISTS medical.authorizations;

ALTER TABLE core.documents DROP CONSTRAINT IF EXISTS documents_phi_status_check;
ALTER TABLE core.documents DROP COLUMN IF EXISTS phi_classified_by;
ALTER TABLE core.documents DROP COLUMN IF EXISTS phi_classified_at;
ALTER TABLE core.documents DROP COLUMN IF EXISTS minor_patient;
ALTER TABLE core.documents DROP COLUMN IF EXISTS phi_categories;
ALTER TABLE core.documents DROP COLUMN IF EXISTS phi_status;

COMMIT;
```

### 4.4 Verification and Stop-Gate

Same shadow-schema approach as Task 1.

Verify:
- All six new `core.documents` columns appear.
- CHECK constraint rejects invalid `phi_status`.
- GIN index on `phi_categories` functional.
- `medical.authorizations` and `medical.disclosures_log` accept inserts and reject invalid FK references.

**Stop-gate: T2_VERIFICATION** — `ops.pending_approvals` entry with `decision_type='sprint_a_t2_verification'`, same polling convention as T1.

---

## 5. Task 3 — Privilege and PHI Filtering at Retrieval Surfaces

### 5.1 Objective

Every retrieval call that surfaces `core.documents` content to a human or external system filters by privilege and PHI classification. Every surfaced document ID plus caller context is logged to `ops.access_log`. Every PHI disclosure is additionally logged to `medical.disclosures_log`.

Agent-internal reads (agents reading for data quality, self-healing, case strategy, etc.) are **out of Sprint A scope** — they are Sprint A.5. See §12.

### 5.2 In-Scope Surfaces

Task 3 modifies the four surfaces where retrieved content egresses to humans or external systems:

| Surface | Reason in scope | Call sites |
|---|---|---|
| `mcp-server/mcp_server_v2.py` | Primary MCP surface used by Claude Desktop, Claude Code, and other clients. Agents and humans alike. | ~15 `@mcp.tool` functions; critical ones listed in §5.4 |
| `dashboard/app.py` | Dashboard backend. Its chat UI exposes 9 tools to the LLM via `CHAT_TOOLS` at [L2312](dashboard/app.py#L2312); 8 of those 9 hit `core.documents`. Also drives HTTP endpoints that return documents to the browser. | 8 LLM-exposed `_tool_*` functions + HTTP endpoints (`api_documents`, `api_document_detail`, `api_search`, `api_chat_email_search`, `api_chat_email_semantic_search`, etc.) |
| `morning_briefing.py` | Emails daily brief to Will via SMTP. Counts as HIPAA "transmission" — a brief that pulls PHI snippets without a purpose_of_use is exactly the leak the filter prevents. | `get_recent_important_emails` [L113](morning_briefing.py#L113), `get_case_summary` [L133](morning_briefing.py#L133) |
| `legal_module.py` | Shared helper module imported by MCP and agents. Natural home for the filter implementation. | 5 `core.documents` sites ([L176,L200](legal_module.py#L176)) |

Live dashboard LLM tools: `semantic_search`, `fulltext_search`, `lookup_case`, `get_document`, `search_emails`, `list_cases`, `list_case_documents`, `get_documents_bulk`. The ninth LLM tool `web_search` is out of scope (external). Two additional dispatch-table entries `database_stats` and `consult_agents` are NOT registered with the LLM — `database_stats` is in scope if it reads `core.documents` for aggregates (use count-only mode per §5.6); `consult_agents` is a meta-tool with no direct document read.

### 5.3 Architecture: Central Helper + Hardened Fetch

A new module **`core_safety.py`** (top-level, next to `blob_storage.py`) hosts the filter. `legal_module.py` imports from it; the other three surfaces also import from it directly.

**Composable fragment for search-style queries:**

```python
# core_safety.py — signatures

class PrivilegeDeniedException(Exception): ...
class PHIAccessDeniedException(Exception): ...

async def build_document_safety_filter(
    *,
    domain: Optional[str],               # 'legal', 'medical', or None (cross-domain)
    caller_context: dict,                # {'tool': str, 'user': str|None, 'session_id': str|None, 'agent_id': str|None}
    purpose_of_use: Optional[str] = None, # e.g. 'litigation_support', 'clinical_care', 'morning_brief'
    include_privileged: bool = False,    # legal domain: allow 'attorney_client' and 'work_product_fact'
    authorization_id: Optional[UUID] = None,  # explicit auth for PHI / heightened-protection
    mode: Literal['content', 'count_only'] = 'content',
    table_alias: str = 'd',
    next_param_index: int = 1,           # caller manages $N placeholders
) -> tuple[str, list[Any], Callable[[list[dict]], Awaitable[None]]]:
    """
    Returns (where_clause_fragment, params_to_append, log_surfaced_callback).

    where_clause_fragment uses placeholders $N .. starting at next_param_index and
    references columns on the given table_alias. Caller composes into their query:

        clause, params, log = await build_document_safety_filter(domain='legal', ...)
        rows = await conn.fetch(
            f"SELECT d.* FROM core.documents d JOIN ... WHERE cd.case_id = $1 AND {clause}",
            case_id, *params,
        )
        await log(rows)

    log_surfaced_callback writes to ops.access_log with {tool, user, agent_id, domain,
    filter_mode, document_ids, caller_context} and, in medical domain with purpose_of_use
    set and PHI actually surfaced, additionally writes to medical.disclosures_log.
    """

async def fetch_safe_document(
    doc_id: UUID,
    *,
    caller_context: dict,
    purpose_of_use: Optional[str] = None,
    include_privileged: bool = False,
    authorization_id: Optional[UUID] = None,
) -> dict:
    """
    Hardened fetch-by-ID. Returns document row or raises
    PrivilegeDeniedException / PHIAccessDeniedException. Always writes to
    ops.access_log with the fetched id + outcome. Writes to
    medical.disclosures_log on successful PHI fetch.
    """
```

**Why a fragment, not a wrapper.** The existing retrieval paths have bespoke joins (legal.case_documents, legal.email_metadata, medical.record_metadata, etc.) and ordering (ts_rank, vector distance, timeline ordering). A wrapper would fight those. A fragment composes cleanly into any query without constraining the join topology.

**Why a separate `fetch_safe_document`.** `fetch(id)` / `_tool_get_document(id)` bypass the search layer entirely — a caller with a UUID can pull any row. These paths need their own enforcement function, and the contract is simpler (one document, one outcome), so a function is better than a fragment.

### 5.4 Filter Logic

**Legal domain (`domain='legal'`):**

| `include_privileged` | Allowed `privilege` values |
|---|---|
| `False` (default) | `'none'` only |
| `True` | `'none'`, `'attorney_client'`, `'work_product_fact'` |
| `True` with explicit category allow-list | above plus any listed (`'work_product_opinion'`, `'joint_defense'`, `'common_interest'`) |

NULL-privilege documents are always excluded until classified. Explicit `privilege='unknown'` is treated as NULL for retrieval purposes.

**Medical domain (`domain='medical'`):**

| `purpose_of_use` | Allowed `phi_status` values |
|---|---|
| None (default) | `'safe_harbor_deidentified'`, `'expert_determination_deidentified'`, `'not_phi'` |
| Set (e.g. `'litigation_support'`, `'clinical_care'`) | above plus `'phi'` and `'limited_data_set'` — **logs to `medical.disclosures_log`** |

NULL-phi documents in the medical domain are excluded until classified.

**Heightened-protection categories** (applies regardless of `purpose_of_use`):

- Documents with `'psychotherapy_notes'` in `phi_categories`: NEVER surfaced unless `authorization_id` references a non-revoked `medical.authorizations` row whose `scope` includes psychotherapy notes.
- Documents with `'sud_42_cfr_part_2'` in `phi_categories`: NEVER surfaced unless `authorization_id` references a non-revoked Part 2 authorization.
- Documents with `minor_patient=true`: require `purpose_of_use` even in deidentified contexts; default is exclude.

**Cross-domain queries (`domain=None`):**

Apply both legal and medical filters — a document surfaces only if it satisfies whichever domain it belongs to.

### 5.5 `morning_briefing.py` Stricter Defaults

Morning briefing emails content to Will. Per the rationale in §5.2, it gets its own flag set:

| Category | Interactive retrieval default | Morning-brief default |
|---|---|---|
| Legal privileged | excluded | **metadata only** (case name + filing date + document_type; never title or content) |
| Legal NULL-privilege | excluded | excluded |
| Medical PHI | excluded | **metadata only** (patient initials or redacted patient identifier + record_type + date_of_service; never full content, never narrative) |
| Medical heightened-protection | excluded | **fully excluded** regardless of authorization — brief is never the right surface for Part 2 or psychotherapy notes |
| Unclassified documents | excluded | excluded |

`morning_briefing.py` calls a separate helper `build_morning_brief_safety_filter(...)` that wraps `build_document_safety_filter` with these defaults locked in. Direct use of the content-mode filter from the briefing path is forbidden — the briefing code imports only the brief-specific wrapper.

### 5.6 Count-Only Mode

Audit endpoints, stats dashboards, and tools like `database_stats` have a legitimate need to count privileged/PHI documents without exposing content. `mode='count_only'` causes the helper to return:

- A WHERE fragment that passes all documents matching the non-safety predicates (privilege-unaware for counting purposes).
- A logging callback that records aggregate counts only, never document IDs.
- Calling code must return only aggregate numbers — column lists must be restricted to `COUNT(*)` / `GROUP BY` dimensions that are not document content (e.g., `domain`, `document_type`, `COUNT(*)`). Returning `title`, `filename`, `full_content`, `source_path` in count-only mode is a Task 3 test failure.

### 5.7 Call-Site Changes

Per surface, the modification pattern is:

**MCP `semantic_search`, `fulltext_search`, `search_emails`, `search_case_documents`, `search_medical_records`, etc.** — compose the fragment into the existing WHERE, call `log` after materializing rows.

**MCP `fetch`, `_tool_get_document`, email-thread-fetch, case-document fetch-by-id** — replace the direct `SELECT * FROM core.documents WHERE id = $1` with a call to `fetch_safe_document`. Caller handles `PrivilegeDeniedException` / `PHIAccessDeniedException` and returns a denial response (not a silent empty result — callers must know they were denied so they can ask for privileged access if legitimate).

**Dashboard `_tool_*` functions + `api_*` endpoints** — same pattern as MCP. The 8 live chat tools plus the HTTP endpoints that return documents to the browser.

**Dashboard audit endpoints (`api_audit_access`, `api_audit_searches`)** — count-only mode.

**`morning_briefing.py`** — use `build_morning_brief_safety_filter` exclusively.

**`legal_module.py`** — refactor its 5 call sites to use the fragment. As the helper module also imported by agents in Sprint A.5, this refactor is load-bearing: getting it right in Sprint A means Sprint A.5 can ride on already-filtered calls where appropriate.

### 5.8 Test Matrix

Tests in `/opt/wdws/tests/test_retrieval_filters.py`. Organized by surface × filter case:

**Filter-logic unit tests on `build_document_safety_filter` and `fetch_safe_document`:**

1. Default legal retrieval excludes NULL-privilege documents.
2. Default legal retrieval excludes `attorney_client` documents.
3. Legal retrieval with `include_privileged=True` surfaces `attorney_client` and `work_product_fact` but not `work_product_opinion` / `joint_defense` / `common_interest` without explicit allow-list.
4. Default medical retrieval excludes `phi` and `limited_data_set` documents.
5. Medical retrieval with `purpose_of_use='litigation_support'` surfaces PHI and logs a row to `medical.disclosures_log` with the correct `purpose_of_use`, `document_id`, `authorization_id`.
6. Documents with `'psychotherapy_notes'` in `phi_categories` are NEVER surfaced without `authorization_id` referencing a non-revoked psychotherapy-scope authorization, regardless of `purpose_of_use`.
7. Documents with `'sud_42_cfr_part_2'` in `phi_categories` are NEVER surfaced without `authorization_id` referencing a non-revoked Part 2 authorization, regardless of `purpose_of_use`.
8. `minor_patient=true` documents require explicit `purpose_of_use` even when `phi_status='safe_harbor_deidentified'`.
9. `fetch_safe_document` raises `PrivilegeDeniedException` when called on an `attorney_client` doc without `include_privileged`.
10. `fetch_safe_document` raises `PHIAccessDeniedException` when called on a `phi` doc without `purpose_of_use`.
11. `fetch_safe_document` on a denied fetch still writes to `ops.access_log` (record of attempted access).
12. `count_only` mode returns aggregate counts but never document IDs in the logging callback.

**Per-surface integration tests:**

13. MCP `semantic_search` filters with default params (no privileged results).
14. MCP `fetch` raises on privileged doc without `include_privileged`.
15. Dashboard `_tool_get_document` returns denial response (not an empty result) on privileged doc.
16. Dashboard `api_documents` HTTP endpoint filters with default params; response body never contains privileged content.
17. `morning_briefing.get_case_summary` with a case that has privileged emails returns metadata only, never title / content / body.
18. `morning_briefing.get_recent_important_emails` excludes PHI documents entirely (not metadata-only — the briefing doesn't reference medical in this function).
19. `legal_module` case-document list calls surface only `privilege='none'` documents when called by a non-privileged context.
20. `ops.access_log` receives one row per retrieval call with correct `tool`, `domain`, `result_count`.
21. `medical.disclosures_log` receives one row per PHI document surfaced (not one row per retrieval — one per document).

All 21 tests must pass before deployment. Tests run against the post-migration shadow schemas (not production). A staging dataset of 20–50 pre-classified documents (5 legal privileged, 5 legal non-privileged, 5 medical PHI, 5 medical de-identified, 2 psychotherapy-notes, 2 Part-2, 1 minor-patient) is fixture data seeded before the test run.

### 5.9 Stop-Gate

**Stop-gate: T3_REVIEW**
Agent produces:
- Full diff of `core_safety.py`, `mcp-server/mcp_server_v2.py`, `dashboard/app.py`, `morning_briefing.py`, `legal_module.py`.
- Test results (all 21 must pass).
- One-paragraph summary of any call sites that deviated from the template (there will be some — bespoke joins sometimes need care).

`ops.pending_approvals` entry with `decision_type='sprint_a_t3_review'`, `risk_level='critical'`. Human reviews diff + tests, sets `approved=true` to promote to production path.

---

## 6. Task 4 — Backfill Classification

### 6.1 Objective

Classify the 10,344 existing in-scope documents (9,539 legal + 805 medical). Per preflight counts.

### 6.2 First Action (No Approval Needed — Deterministic)

**Keyword auto-classification pass.** Before any LLM involvement, agent runs a deterministic SQL pass that sets `privilege='attorney_client'` and `privilege_classified_by='keyword_autoclassify_v1'` on the 209 legal documents whose `full_content` matches any of:

- `'ATTORNEY-CLIENT PRIVILEGED'`
- `'ATTORNEY CLIENT PRIVILEGED'`
- `'PRIVILEGED AND CONFIDENTIAL'` followed within 200 chars by `'attorney'` or `'counsel'`
- `'WORK PRODUCT'` as a document-header assertion (first 500 chars)

Exact patterns finalized before execution; pattern set checked in to `core_safety.py` for reproducibility. These are zero-risk classifications — the document's author has explicitly asserted privilege, which is binding under ABA Model Rule 1.6 regardless of whether an LLM later disagrees.

Outcome:
- 209 documents (approximately) receive deterministic privilege tags.
- LLM workload reduced by ~2% on the legal side.
- End-to-end classification pipeline proved against low-risk data before the interesting cases run.
- Single progress row written to `ops.athena_decisions` with `decision_type='sprint_a_keyword_autoclassify'`, `action_taken` detailing the pattern set and row counts.

No stop-gate for this action — it's mechanical and strictly additive on a column that was NULL.

### 6.3 LLM-Assisted Classification (Gated)

1. **Legal domain:**
   - For remaining `privilege IS NULL` legal documents, LLM classifier anchored to ABA Model Rule 1.6 + work product doctrine.
   - Returns: `privilege`, `confidentiality`, `confidence_score`, `rationale`.
   - High-confidence (>0.9): auto-apply, mark `privilege_classified_by='agent_athena_v1'`.
   - Medium-confidence (0.7–0.9): queue for human review via `ops.pending_approvals` with `decision_type='sprint_a_t4_classification_review'`.
   - Low-confidence (<0.7): flag, set `privilege='unknown'`.

2. **Medical domain:**
   - For `phi_status IS NULL` medical documents, default presumption is `phi` unless classifier identifies no PHI (non-medical content erroneously tagged medical domain).
   - Classifier additionally identifies heightened-protection categories (42 CFR Part 2, psychotherapy notes, mental health, HIV, genetic).
   - Any document tagged with heightened-protection categories: human review required before classification finalizes.
   - Minor-patient detection via DOB math from `medical.patients.date_of_birth` relative to `document.created_at`.

3. **Rate limiting:** 100 documents / hour, regardless of observed budget headroom. Rate is enforced in the backfill worker itself, not as a loose suggestion.

4. **Progress tracking:**
   - Every 100 documents: row in `ops.athena_decisions`.
   - Daily: summary line in Will's morning brief (once morning brief is healthy post-§5.5).

### 6.4 Safety

- Any document linked to an active `legal.protective_orders` row (via `core.documents.protective_order_id`) automatically gets `confidentiality` set to at least `'confidential'`. Enforced in the classifier output pipeline.
- Backfill runs on a staging table first: `core.documents_backfill_staging` is populated with `(document_id, proposed_privilege, proposed_phi_status, proposed_phi_categories, proposed_confidentiality, confidence, rationale)`, reviewed aggregate-wise, then promoted to `core.documents` in a single transaction per batch of 100.

### 6.5 Stop-Gate

**Stop-gate: T4_SAMPLE_REVIEW**
After the first 100 LLM-assisted classifications in each domain (post keyword pass), agent presents a random sample of 10 per domain to Will via `ops.pending_approvals` with `decision_type='sprint_a_t4_sample_review'`. Approval threshold: ≥9/10 correct per domain. On approval, bulk backfill proceeds. On rejection (or <9/10 correct), agent halts Task 4 and surfaces the failing classifications for root-cause analysis.

---

## 7. Task 5 — Extend Retention Agent

### 7.1 Objective

Extend `agents/agent_retention.py` (currently 165 lines, single `RetentionAgent.run` method) to also:

1. Generate weekly HIPAA accounting-of-disclosures report from `medical.disclosures_log`.
2. Generate weekly privilege log summary (per case) from `core.documents` filtered by case.
3. Flag documents where `phi_authorization_id` references an expired or revoked authorization — surface in Will's morning brief.

### 7.2 Changes

Add to `RetentionAgent`:
- `async def generate_disclosures_report(self, week_of: date) -> Path`
- `async def generate_privilege_summaries(self, week_of: date) -> Path`
- `async def check_expired_authorizations(self) -> list[dict]`

Reports written to `/opt/wdws/reports/weekly/YYYY-MM-DD/` with filenames `disclosures_accounting.pdf`, `privilege_log_summary.pdf`, `expired_auth_alerts.md`. Week number is ISO week starting Monday of the report window.

Privilege-log generation uses `build_document_safety_filter` in metadata-only mode — the privilege log lists privileged documents by the metadata (date, author, recipient, subject line, privilege type) that privilege logs legally require, but never includes content.

### 7.3 Stop-Gate

**Stop-gate: T5_REVIEW**
First generated instances of all three reports reviewed by Will via `ops.pending_approvals` with `decision_type='sprint_a_t5_review'` before the recurring Sunday 3 AM schedule is activated.

**Per §10 rule 7,** Task 5's recurring schedule does not activate until `wdws-agents.service` is healthy.

---

## 8. Acceptance Criteria

Sprint A is complete when every item below is verifiable:

- [ ] Migration 026 applied. `\d core.documents` shows `privilege`, `confidentiality`, `protective_order_id`. `\d legal.cases` shows `assigned_judge`, `opposing_firm`. `ops.migrations_applied` has `026_legal_privilege_judge` row with `status='applied'`.
- [ ] Migration 027 applied. `\d core.documents` shows `phi_status`, `phi_categories`, `minor_patient`, `phi_authorization_id`. `medical.authorizations` and `medical.disclosures_log` exist. `ops.migrations_applied` has `027_medical_phi_classification` row with `status='applied'`.
- [ ] `core_safety.py` exists and exports `build_document_safety_filter`, `fetch_safe_document`, `PrivilegeDeniedException`, `PHIAccessDeniedException`, `build_morning_brief_safety_filter`.
- [ ] All 21 tests in `tests/test_retrieval_filters.py` pass.
- [ ] All 4 in-scope retrieval surfaces (`mcp_server_v2.py`, `dashboard/app.py` — 8 live LLM tools + HTTP endpoints, `morning_briefing.py`, `legal_module.py`) route reads through the helper.
- [ ] At least 95% of existing legal documents have `privilege` classified (not NULL).
- [ ] At least 95% of existing medical documents have `phi_status` classified.
- [ ] Heightened-protection categories (42 CFR Part 2, psychotherapy notes) are human-verified on sample.
- [ ] Weekly retention reports generate successfully for one full cycle (once `wdws-agents.service` is healthy).
- [ ] Rollback SQL for 026 and 027 exists, has been tested in shadow schema, and an end-to-end dry rollback has been executed once in staging.
- [ ] All Sprint A work on branch `sprint-a-privilege-phi` branched from a clean `master`, with clear commit history.

---

## 9. Rollback Plan

If at any point Sprint A must be aborted:

1. `wdws-agents.service` halted (if running) to prevent further agent action on the Sprint A branch.
2. Apply `027_medical_phi_classification_down.sql` via migration runner (reverses 027 if applied). Runner updates the `ops.migrations_applied` row to `status='rolled_back'`.
3. Apply `026_legal_privilege_judge_down.sql` via migration runner (reverses 026 if applied). Same status update.
4. Restore retrieval surfaces (`mcp_server_v2.py`, `dashboard/app.py`, `morning_briefing.py`, `legal_module.py`, `core_safety.py`) from `sprint-a-privilege-phi` branch's parent commit on `master`.
5. Close any open `ops.pending_approvals` rows with `decision_type` prefix `sprint_a_*` — set `approved=false`, `rejection_note='sprint_aborted'`.
6. Restart `wdws-agents.service` (after ensuring its unrelated SyntaxError issue is resolved, if it was during Sprint A).
7. Write incident finding to `ops.agent_findings` with `severity='critical'`, `category='sprint_a_aborted'`, and a full narrative.

---

## 10. Agent Operating Rules

Non-negotiable for the duration of Sprint A.

1. **No production write before stop-gate approval.** Every stop-gate requires `ops.pending_approvals.approved = true` with human-authored `approved_by` and (optionally) `rejection_note` populated. Agent polls `WHERE decision_type = '<gate>' AND approved IS NOT NULL`; does not self-approve. On `approved = false`, agent halts and does not retry.
2. **All SQL applied in transactions.** If any statement fails, ROLLBACK and write finding.
3. **No rate-limit bypasses.** Backfill classification stays at 100 docs/hour regardless of observed budget headroom.
4. **No silent schema changes.** Every column addition, constraint, index is in the migration file — no ad-hoc ALTER TABLE outside migration scripts.
5. **No retrieval filter bypasses in in-scope surfaces.** The four Task 3 surfaces must route through `core_safety.py`. Direct SQL on `core.documents` bypassing the helper in any of those four files is a Task 3 failure. (Agent-internal reads — in the other agent files — are Sprint A.5, not Sprint A.)
6. **Human quiet hours respected for autonomous work.** No new `ops.pending_approvals` entries created autonomously between 10pm and 7am PT. *Directed* in-conversation work proceeds regardless of time of day — the human is actively present, so gates are synchronous. Autonomous overnight work queues gates to fire after 7am.
7. **Service-health gating.** Task 4's bulk LLM-assisted backfill and Task 5's recurring report schedule require `wdws-agents.service` to be healthy. Tasks 1, 2, 3 (all synchronous, human-directed) do not. The service SyntaxError in `agent_dba.py:681` is a separate incident — fix it before Task 4 bulk phase and Task 5 schedule activation, or accept that those remain in synchronous-directed mode.
8. **Daily progress to morning brief.** Every 24 hours, agent writes a concise Sprint A status line to Will's morning brief: tasks completed, pending-approval count, blockers, next stop-gate expected. (Contingent on morning brief being wired — once Task 3 lands.)

---

## 11. Success Signal

Sprint A produces a single, auditable change in Athena's behavior: **every retrieval result surfaced to humans or external systems is now filtered by privilege and PHI status, and every surfacing of privileged or PHI content is logged to an immutable audit trail.**

This is the foundation that makes every subsequent sprint defensible. Without it, Sprint B (medical module), Sprint C (template library + citation engine), Sprint D (medical-legal bridge), and Sprint E (terminology + learning gates) would build on unsafe ground.

The Sprint A headline is intentionally scoped to *human/external egress*, not "every SELECT in the codebase." See §12 for the follow-on.

---

## 12. Handoff + Sprint A.5 Scope

### 12.1 Closeout Report

On completion, agent writes closeout report to `/opt/wdws/reports/sprint_a_closeout.md` containing:
- Actual duration vs. target
- Classifications completed per domain with accuracy stats
- Any deviations from this work order with rationale
- Items deferred to follow-up sprints
- Recommendations for Sprint B kickoff

Will reviews closeout report and signs off in `ops.pending_approvals` with `decision_type='sprint_a_closeout'`. Sprint B work order is then released.

### 12.2 Sprint A.5 — Agent-Internal Reads (Follow-on, Not Prerequisite)

Five files do direct SQL on `core.documents` for internal agent processing:

- `agents/agent_data_quality.py` (7 sites)
- `agents/agent_self_healing.py` (4 sites)
- `agents/agent_quality_eval.py` (2 sites)
- `agents/agent_case_strategy.py` (3 sites)
- `email_agent.py` (4 sites, mostly write-path classification, with some reads)

**These do not egress to humans or external systems — they are Athena's own processing.** Routing them through the helper requires a purpose-of-use taxonomy that Sprint A does not define. Example open questions: what's the right `purpose_of_use` for a data-quality agent that needs to inspect every document to spot ingestion anomalies? For a self-healing agent that must read a document to determine why a downstream index rebuild failed? For a case-strategy agent whose "job" is synthesizing across all privileged material?

Sprint A.5 scope:
1. Define purpose-of-use taxonomy for agent-internal reads.
2. Decide which agents legitimately need full access (probably data quality, self-healing) vs. filtered access (case strategy morning analysis).
3. Route the 5 files through the helper with per-agent defaults.
4. Add 5 additional tests to the matrix: one per agent file, verifying the expected purpose_of_use is logged to `ops.access_log`.
5. Extend `ops.access_log` reporting so agent-internal reads are distinguishable from user/MCP reads in audit.

Sprint A.5 is sized at 2–3 days. It should run immediately after Sprint A closeout.

### 12.3 Future Refactor — Deduplicate the Two MCP-Style Tool Sets

**Observation flagged during Sprint A preflight.** `mcp-server/mcp_server_v2.py` exposes ~15 MCP tools that query `core.documents`; `dashboard/app.py` exposes 8 parallel `_tool_*` functions that do the same thing for the dashboard chat UI. The two sets:

- Duplicate logic (separate implementations of semantic_search, fulltext_search, get_document, search_emails, list_cases, list_case_documents, get_documents_bulk).
- Will drift over time.
- Are exactly the kind of surface where a safety rule (like Sprint A's privilege/PHI filter) risks being enforced in one set and not the other as new features land.

Not a Sprint A concern — Sprint A simply enforces the filter in both places. But the duplication should be retired in a future refactor, probably by having `dashboard/app.py` import the MCP tool implementations as a library or by extracting the retrieval tools to a shared module that both surfaces register. This recommendation belongs in the closeout report and in the Strategic Framework's near-term roadmap.

---

**END OF WORK ORDER v2**
