# Sprint A — Pre-Flight Reconciliation Report

**Generated:** 2026-04-23
**Work Order:** `SPRINT_A_PRIVILEGE_PHI`
**Author:** agent (directed reconnaissance, read-only)
**Scope:** verify substrate, detect schema drift, size classification backfill, confirm target code files. No writes to the database, migration files, or code.

---

## 0. Headline

**Sprint A is NOT yet ready for Task 1 execution.** The substrate is largely sound, but four schema-drift issues will cause the migration SQL to fail as written, the preflight checklist has two items that are objectively false against the current system, and Task 3 appears to target the wrong file. Fixes are small and inline — none requires a prerequisite sprint — but the work order SQL and Task 3 file target must be corrected before any stop-gate can be issued.

Sections 7 and 8 below enumerate the blockers and propose the minimum inline bootstrap for each.

---

## 1. Substrate Verification

### 1.1 Schemas and tables

| Referenced object | Exists? | Notes |
|---|---|---|
| schema `ops` | ✓ | present |
| schema `core` | ✓ | present |
| schema `legal` | ✓ | present |
| schema `medical` | ✓ | present |
| `ops.pending_approvals` | ✓ | column shape differs from work order — see §2.2 |
| `ops.agent_findings` | ✓ | free-text `severity` (no CHECK); current vocabulary differs from work order — see §2.4 |
| `ops.agent_runs` | ✓ | has no `run_type` column (work order refers to one) — see §2.3 |
| `ops.migrations_applied` | ✓ | column shape differs from work order INSERT **and table is empty** — see §2.1 |
| `ops.access_log` | ✓ | empty (0 rows); shape compatible with Task 3 logging |
| `ops.athena_decisions` | ✓ | shape compatible |
| `core.documents` | ✓ | 10,562 rows, 209 MB; no collision with proposed 026/027 column additions |
| `legal.cases` | ✓ | no `assigned_judge` / `opposing_firm` columns — ready for additive migration |
| `medical.patients` | ✓ | minimal (id, name, date_of_birth, metadata, created_at); FK target for migration 027 is valid |
| extension `pgcrypto` / `gen_random_uuid()` | ✓ | installed and callable — both migrations can use it |

### 1.2 Service status

| Service | Status | Notes |
|---|---|---|
| `wdws-agents.service` | **FAIL (auto-restart loop)** | `SyntaxError` originating from import chain; see §7.5 |

### 1.3 Backups

`/opt/wdws/backups/wdws_20260423_030001.dump` — today's dump exists. Preflight checklist §2 item 1 is satisfied.

### 1.4 Filesystem

- `/opt/wdws/reports/` — did not exist at start of this pass; created (empty) during this recon for the report itself.
- `/opt/wdws/contextual_retrieval.py` — exists.
- `/opt/wdws/agents/agent_retention.py` — exists.
- Last migration file on disk: `025_mailbox_ingest_strategy.sql`. Matches work order expectation.

### 1.5 Git state

- Current branch: **`master`** (not `main` — work order text says "created from main"; this is a cosmetic mismatch but the branch name in §2 of the work order and the rollback plan should both read `master`).
- Working tree is **not clean**: 9 modified tracked files, 23 untracked files including several unregistered migration files under `migrations/` (023, 024, 025) and a handful of new Python workers (ingestion_pipeline.py, email_chunker.py, etc.).
- Preflight checklist item "Git working tree is clean on main branch" is **false**. Sprint A should not branch off the current dirty tree without first resolving or explicitly stashing this state.

---

## 2. Schema Drift — Work Order SQL vs Live Schema

All four items below are drift against the live schema. Each breaks at least one part of the work order as written.

### 2.1 `ops.migrations_applied` — INSERT will fail

**Work order SQL (026 §3.2 and 027 §4.2):**
```sql
INSERT INTO ops.migrations_applied (migration_name, applied_at, notes)
VALUES ('026_legal_privilege_judge', now(), '...additive only.');
```

**Live schema:**
```
id, migration_name, sql_forward (NOT NULL), sql_rollback (NOT NULL),
applied_by (NOT NULL), applied_at, duration_ms, analysis, confidence,
status (NOT NULL, CHECK in 'applied','rolled_back','failed'),
rolled_back_at, rolled_back_by
```

- There is **no `notes` column**.
- `sql_forward`, `sql_rollback`, `applied_by`, `status` are **NOT NULL** with no defaults.
- As written, both `INSERT`s will fail with "column notes does not exist" and, even after removing that, with four NOT NULL violations.

**Additionally:** `ops.migrations_applied` is **empty (0 rows).** The preflight checklist item "`ops.migrations_applied` last entry is `025_mailbox_ingest_strategy`" is false. The table was created but historical migrations were never recorded there.

### 2.2 `ops.pending_approvals` — no `status` column, no `approval_type` column

- Work order refers to `ops.pending_approvals.status='approved'` (§3.4, §4.4, §10.1).
- Work order §12 refers to `approval_type='sprint_closeout'`.
- Live schema uses `approved` (boolean), `approved_by`, `approved_at`, `rejection_note`, `decision_type` (not `approval_type`).

Stop-gate polling logic in §10 rule 1 is not directly expressible against the current table. Either the work order's language adapts to `approved = true`, or the `decision_type` field carries the category ("sprint_a_stop_gate_t1", "sprint_closeout", etc.) and a `WHERE` clause on that plus `approved = true` serves as the gate.

### 2.3 `ops.agent_runs` — no `run_type` column

- Work order §2 says "Record verification in `ops.agent_runs` with `run_type='sprint_a_preflight'`."
- Live schema's closest free-text field is `trigger`. Using `trigger='sprint_a_preflight'` is the natural substitute.

### 2.4 `ops.agent_findings` — severity vocabulary mismatch

- No CHECK constraint on `severity` — free text, so work order values will be *accepted*. But current conventions in 48,986 existing rows are: `warning` (24914), `info` (17794), `critical` (4695), `suggestion` (1583).
- Work order proposes new values: `blocking`, `awaiting_review`, `sprint_aborted`.
- Not a blocker. But if Athena dashboards filter on the four existing values, new ones will be invisible until the dashboards are updated, and morning-brief tooling may need to learn them. Worth a single-line decision: use existing vocabulary (`critical` + a `category` field) or explicitly introduce the new values.

---

## 3. Additive Schema — Confirmed Non-Colliding

This is the good news. The proposed migrations do not conflict with anything in the current schema.

### 3.1 `core.documents`

Current columns:
```
id, domain, source_path, filename, document_type, title, content_hash,
total_chunks, full_content, full_content_tsv, metadata, created_at, updated_at,
user_id, raw_content, classification, classified_at, retain_until,
legal_hold, legal_hold_reason, purged_at, purge_reason,
storage_backend, storage_uri, storage_sha256, storage_size_bytes
```

- None of the proposed additions (`privilege`, `confidentiality`, `phi_status`, `phi_categories`, `minor_patient`, `phi_classified_at`, `phi_classified_by`, `privilege_classified_at`, `privilege_classified_by`, `protective_order_id`, `phi_authorization_id`) collide with existing columns.
- No existing CHECK constraints on `core.documents` (confirmed via `pg_constraint`) — the new `documents_privilege_check`, `documents_confidentiality_check`, `documents_phi_status_check` will land cleanly.
- `purged_at`, `domain` both present — the `v_documents_safe_retrieval` and `v_medical_safe_retrieval` views compile against the current schema.
- Table size 209 MB; ALTER TABLE ADD COLUMN with no DEFAULT is instant in Postgres ≥ 11, so no long lock.

### 3.2 `legal.cases`

Current columns do not include `assigned_judge` or `opposing_firm`. Additive migration is safe.

### 3.3 `medical.patients`

Has `id` (integer PK), which is the FK target used by `medical.authorizations` and `medical.disclosures_log`. Ready.

### 3.4 New tables from migrations 026/027

None of `legal.protective_orders`, `medical.authorizations`, `medical.disclosures_log` currently exist. Creation will succeed.

---

## 4. Classification Scope (Task 4 Sizing)

| Domain | Row count | Notes |
|---|---|---|
| `legal` | **9,539** | 100% created in 2026 |
| `medical` | **805** | 100% created in 2026 |
| `web` | 200 | not in Task 4 scope |
| `coding` | 7 | not in scope |
| `general` | 6 | not in scope |
| `paperless` | 4 | not in scope |
| `personal` | 1 | not in scope |
| **Total in-scope** | **10,344** | |
| `purged_at IS NOT NULL` | 0 | no purge filter needed |

At the work-order-mandated **100 docs/hour** cadence, classifying all 10,344 in-scope documents takes **≈ 104 hours wall-clock = 4.3 days** of continuous classification. Well within the 14-day sprint target.

**Keyword pre-filter bonus.** 209 of the 9,539 legal documents already contain an explicit "ATTORNEY-CLIENT PRIVILEGED" / "WORK PRODUCT" assertion in `full_content`. Per §6.3 of the work order these can be auto-classified `privilege='attorney_client'` without LLM involvement, reducing the LLM-classification load slightly.

**No temporal long tail.** Every in-scope document was created in 2026, so there is no pre-2026 corpus to skip or downweight. The backfill is effectively a single cohort.

---

## 5. Code File Surfaces

### 5.1 `/opt/wdws/contextual_retrieval.py` (312 lines)

Public functions:
```
generate_context(...)              async
_fallback_context(...)
generate_context_sync(...)
enrich_chunks(...)
prepend_context(context, chunk_text) -> str
generate_contexts_batch(...)       async
```

**⚠️ This file does not appear to be the retrieval layer.** The names (`generate_context`, `prepend_context`, `enrich_chunks`, `generate_contexts_batch`) strongly suggest this module implements Anthropic's "contextual retrieval" *chunk-enrichment* technique — i.e. it generates a short LLM-written context paragraph that gets prepended to each chunk *before embedding*. It does not look like the file that runs retrieval queries, filters results, or assembles search output.

Task 3 of the work order wants to add `include_privileged` / `purpose_of_use` / `caller_context` parameters and privilege/PHI `WHERE`-clause filtering to the retrieval path. Those additions almost certainly belong in the module that actually executes retrieval queries — most likely `mcp-server/mcp_server_v2.py` (MCP tools exposed to agents), `chat_api.py`, or whichever module calls `ops.access_log` today.

**This is a blocker of type "wrong target", not a true substrate gap.** Small fix: an early subtask to identify the real retrieval path (grep for functions that execute SELECTs against `core.documents` with user-facing parameters) and re-point Task 3 at it. I did not grep for that on this pass — not in scope for preflight.

### 5.2 `/opt/wdws/agents/agent_retention.py` (165 lines)

```
class RetentionAgent(BaseAgent):
    async def run(self, ctx: RunContext) -> dict
```

Single-class, single-method file. Task 5's `generate_disclosures_report()`, `generate_privilege_summaries()`, `check_expired_authorizations()` methods will be net-new additions to this class, not edits to existing methods. The work order's phrasing ("extend it to also…") is accurate; flagging only so nobody expects to find existing scaffolding to hook into.

---

## 6. Preflight Checklist vs Reality

Mapping §2 of the work order to observed state:

| Check | Work order says | Reality |
|---|---|---|
| Full dump dated within 24h | required | ✓ `wdws_20260423_030001.dump` exists |
| Git tree clean on main | required | ✗ 9 modified + 23 untracked files; branch is `master` not `main` |
| Branch `sprint-a-privilege-phi` from main | required | — (not yet created; will start from a dirty tree) |
| Shadow schemas `legal_shadow`/`medical_shadow` absent | required | ✓ absent |
| `ops.migrations_applied` last entry = `025_mailbox_ingest_strategy` | required | ✗ table is empty (0 rows); no history recorded |
| `wdws-agents.service` active | required | ✗ failed (syntax error in `agent_dba.py`, auto-restart loop) |
| Athena quiescent | required | — not verified; service is down, so trivially no running job |

**Two explicit checklist failures** (git tree, migrations_applied history) plus the service failure.

---

## 7. Blockers and Minimum Inline Bootstraps

Numbered in recommended resolution order. None of these require a prerequisite sprint. Most are small.

### 7.1 Correct the `ops.migrations_applied` INSERT in 026/027

**Fix:** rewrite both INSERTs in the migration drafts to use the real columns. The `sql_forward` and `sql_rollback` fields expect the full SQL text of the migration — the ops schema is designed to be self-describing. A minimal conforming INSERT looks like:

```sql
INSERT INTO ops.migrations_applied
    (migration_name, sql_forward, sql_rollback, applied_by, status, analysis)
VALUES (
    '026_legal_privilege_judge',
    $forward$ /* ...full migration body... */ $forward$,
    $rollback$ /* ...full rollback body... */ $rollback$,
    'sprint_a_human_approved',
    'applied',
    'Sprint A: privilege, confidentiality, protective_orders, assigned_judge, opposing_firm. Additive only.'
);
```

Either embed the full SQL via dollar-quoting, or load the file contents programmatically in the migration runner. Decide which before Task 1 draft finalizes.

### 7.2 Adapt §10 stop-gate language to `ops.pending_approvals` actual columns

**Fix:** change every reference to `ops.pending_approvals.status='approved'` → `ops.pending_approvals.approved = true`. Change `approval_type` → `decision_type`. Adopt a convention like `decision_type='sprint_a_t1_verification'` for each stop-gate.

### 7.3 Adapt §2 preflight record to `ops.agent_runs` actual columns

**Fix:** use `trigger='sprint_a_preflight'` in place of `run_type='sprint_a_preflight'`.

### 7.4 Decide on `ops.agent_findings.severity` vocabulary

**Fix (one-liner):** either (a) use existing `critical` + `category='sprint_a_blocker'`, or (b) introduce `blocking`, `awaiting_review`, `sprint_aborted` explicitly and update dashboards that filter on severity. Pick before Task 1.

### 7.5 Resolve `wdws-agents.service` SyntaxError

**Fix:** `agent_dba.py:681` has an `except Exception as main_error:` whose parent `try:` appears to be missing or the indentation above it is malformed. Runner (`/opt/wdws/agents/run.py`) does `from agent_dba import DBAAgent`, so the import fails at module load and the whole agent process dies. Trace:

```
File "/opt/wdws/agents/run.py", line 44, in <module>
    from agent_dba import DBAAgent
  File "/opt/wdws/agents/agent_dba.py", line 681
    except Exception as main_error:
```

This is not Sprint A work per se, but Sprint A relies on the agent runner for every stop-gate poll loop, backfill pacing, and morning-brief delivery. **The service must be healthy before autonomous Sprint A operation begins.** For *synchronous* in-conversation operation (what we're doing right now), it doesn't matter — the human is directing each step. If you intend to let Sprint A run overnight or across days, this is a prerequisite.

### 7.6 Clean git tree or branch anyway

**Decision needed:** either commit/stash the 9 modified tracked files + decide what to do with the 23 untracked files before creating `sprint-a-privilege-phi`, or branch from current HEAD and accept that Sprint A's branch contains unrelated in-flight work. The second path makes the eventual Sprint A PR noisy and the rollback plan harder to execute cleanly.

### 7.7 Re-point Task 3 at the real retrieval file

**Fix:** spend 10 minutes grepping for the actual privilege/PHI-surface code path (most likely `mcp-server/mcp_server_v2.py`), then rewrite Task 3's file target. Without this, any diff produced against `contextual_retrieval.py` will filter the wrong layer — a chunk-enrichment pipeline does not surface documents to users.

### 7.8 Create `/opt/wdws/reports/`

Done during this pass (empty directory created to hold this report). Noted for completeness.

### 7.9 Correct `master` vs `main` language

Work order refers to `main` throughout. Live default branch is `master`. Cosmetic but the rollback plan (§9 item 4) says "restore from branch's parent commit" — the operator doing the rollback will look for the wrong branch name if this stays.

---

## 8. What Is Actually Ready

Despite the drift, a substantial amount is in good shape:

- All nine referenced tables exist.
- pgcrypto is installed and `gen_random_uuid()` works.
- No column-name collisions on `core.documents` for any of the 11 proposed additions.
- No CHECK-constraint collisions.
- Neither of the three new tables (`legal.protective_orders`, `medical.authorizations`, `medical.disclosures_log`) pre-exists.
- Backfill scope (10,344 documents) fits comfortably in the 14-day window at the mandated 100/hr rate.
- 209 legal documents have a keyword-derivable privilege tag — cheap early win.
- Full DB dump from today exists in `/opt/wdws/backups/`.
- Target code files exist at the referenced paths (with one caveat about which file Task 3 should target).

---

## 9. Recommended Next Step

**Before Task 1 can proceed, please decide the following (each can be one-liner):**

1. Adopt the corrected `ops.migrations_applied` INSERT shape per §7.1. Embed full SQL via dollar-quoting vs programmatic load?
2. Adopt `approved = true` + `decision_type` convention for stop-gates per §7.2.
3. Use `trigger` field on `ops.agent_runs` per §7.3.
4. Severity vocabulary: reuse existing (`critical` + `category`) or introduce the new values? (§7.4)
5. Is `wdws-agents.service` being broken acceptable for synchronous operation now, deferred to its own fix later? (§7.5)
6. Git hygiene: clean the tree, or branch-from-dirty-master? (§7.6)
7. Task 3 file target: grep-and-repoint before Task 1 starts, or defer to start of Task 3? (§7.7)
8. Branch name in work order: keep `main` language or correct to `master`? (§7.9)

Once those are settled, I can proceed to Task 1 (draft migration 026 + rollback + shadow-schema verification) with the corrections baked in. No `ops.pending_approvals` entry will be created until the in-conversation fixes are agreed.

---

**STATUS: Sprint A requires targeted correction of work-order SQL (§7.1–§7.4) and a decision on Task 3's real file target (§7.7) before Task 1 execution. Substrate is sound. No prerequisite sprint needed.**
