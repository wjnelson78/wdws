# ACE Case Situational Awareness — Design Doc

**Author:** William Nelson (with Athena)
**Status:** Draft v0.1
**Last updated:** 2026-04-28
**Component owner:** Athena Cognitive Engine (ACE) / MCP server
**Related:** `mcp-server/mcp_server_v2.py`, `migrations/001_enterprise_schema.sql`, `migrations/032_memory_dedup_and_feedback.sql`, MCP privilege-filter work (post-2026-04-24)

---

## 1. Summary

ACE today exposes a rich set of read primitives — `fetch_case_details`, `case_timeline`, `recent_activity`, `list_upcoming_deadlines`, `search_emails`, `search_agent_notes`, `get_agent_context` — but it does **not** expose a single tool that an agent can call to load full operational situational awareness on a case.

In the absence of that tool, agents have started compensating by writing static markdown "case dossiers" into the working directory and hand-updating them as state changes. This is the wrong shape: those files duplicate state that already lives in Postgres, drift the moment any new email/filing/deadline lands, and put derived-state maintenance on every agent in every session.

This document proposes elevating **case situational awareness** to a first-class ACE capability: a composed, live-rendered, agent-aware dossier service backed by per-agent watermarks, structured handoffs, and event-driven materiality. It also surfaces several adjacent gaps found during the same analysis (case-id attachment at ingest, agent watermarks, materiality, structured handoff objects, privilege gating in composed views) and folds them into one coherent plan.

---

## 2. Problem Statement

### 2.1 The triggering failure mode

When a new email lands for a case (e.g., *Nelson v. Silver Lake*), an agent picking it up needs full operational context: parties, opposing counsel, current procedural posture, last filing, pending deadlines, active motions, recent thread, connected research, open questions from the prior session, and what the user expects next.

Today an agent has to assemble that itself by chaining 4–7 read tools, then guess at deltas since the last touch. The first response to this gap was to start producing static `*_case_dossier.md` files. That doesn't scale:

- **It goes stale on first commit.** The moment one new event lands, the dossier is wrong. There is no automated invalidation.
- **It pushes derived-state maintenance onto agents.** Step 7 of the proposed agent flow was literally "update the dossier with the new event." Every agent in every session is now responsible for keeping markdown in sync with Postgres.
- **It diverges across sessions.** Two agents working the same case in parallel will produce two different dossiers, neither authoritative.
- **It leaks the privilege boundary.** Static markdown is not run through the post-2026-04-24 NULL-tolerant first-party privilege filter; whatever the author saw becomes flat-file truth, including for readers who shouldn't see it.
- **It encodes the agent's view of the case as the ground truth.** ACE is the system of record. A markdown export is not.

### 2.2 The deeper problem

This isn't just a missing tool. The dossier symptom reveals a category of architectural gap: **ACE has rich read primitives, but no composed views, no per-agent state, and no event semantics.** Each agent must re-derive situational awareness from first principles every conversation. That's expensive, error-prone, and inherently divergent.

The right fix is not "build dossier markdown faster." It is to make ACE the place where situational awareness is composed, on demand, with the access control and freshness guarantees that only the system of record can provide.

---

## 3. Gaps Surfaced During Analysis

The dossier issue surfaced one missing tool. Examining the path an agent has to walk to *use* that tool surfaced a cluster of related gaps. Each is independently tractable; together they are the architecture being proposed.

### G1 — No composed case dossier
No tool returns "everything I need to know to act on this case right now." `fetch_case_details` (parties, doc counts), `case_timeline` (chronology), and `list_upcoming_deadlines` (per-case if filtered) are three separate calls that don't naturally compose, and none of them include open questions, prior-session handoffs, or the most recent inbound thread.

### G2 — No per-agent watermark
`recent_activity` exists but has no `since=` cursor and no notion of "what has *this agent* already seen." Every session re-reads the last 7 days from scratch. There is no `ops.agent_case_watermark` table tying `(agent_id, case_id) → last_seen_event_at`.

### G3 — No case-id attachment at ingestion
When an email arrives, the question "which case does this belong to?" is answered (today) by every agent that touches it. That is classification work — sender, subject, thread, body — that should happen once at ingest, write a candidate `case_id` (with a confidence score) onto `legal.case_documents` or an equivalent staging table, and never be repeated. Right now `legal.case_documents` only gets populated when an explicit ingestion job knows the case, leaving large swaths of email implicitly unbound.

### G4 — No event materiality
`case_timeline` returns every document linked to a case, equally. A typo correction on a filing title and a new sanctions order are both "events." Agents have no way to filter to "material events since X" without reading every row. There is no `event_type` enum, no `materiality` flag, and no surfacing of state-change events (status updates, deadline adds, party adds) into the timeline at all — only documents.

### G5 — No structured handoff / open-question object
`create_agent_note` and `search_agent_notes` exist (and per migration 033 are now embedded), but the body is free-text. There is no first-class `open_question`, `next_action`, or `blocker` shape. A composed dossier cannot deterministically surface "the three things the prior session left unresolved" because that information is not structured.

### G6 — Free-text agent sessions, no closeout transaction
`ops.agent_sessions` stores `(started_at, ended_at, summary)` — a free-text summary. There is no transactional closeout that records, in one shot: what the agent read (input set), what it decided, what it changed, what it left open. Without that, the next agent has to re-derive the prior agent's reasoning from logs and notes.

### G7 — Privilege filter not applied to composed views
The post-2026-04-24 NULL-tolerant first-party privilege filter is applied per-tool inside `case_timeline`, `search_emails`, etc. A composed dossier endpoint would have to re-apply it across every sub-query, and the failure mode (forgetting to wrap one sub-query) silently leaks privileged content. The filter needs to live at the composer layer too, with a single explicit `include_privileged` parameter that propagates downward.

### G8 — No subscription / push channel
Agents poll. There is no "tell me when something material changes on this case" channel. This pairs with G2 and G4 — once watermarks and materiality exist, a subscription layer is a thin wrapper around them.

### G9 — Conversation-thread → case binding for SMS
Per the Telnyx infrastructure (migration 031), SMS threads exist as their own table. It is not clear that each SMS conversation is tagged to a `case_id`. If not, the dossier cannot surface "recent SMS on this case." Same audit applies to email threads — `legal.email_metadata` ties messages to documents but not directly to cases except through `legal.case_documents`.

### G10 — No task-intent scoping
A sanctions-motion task wants different parts of the case (recent motions, sanctions law from research threads) than a discovery task (RFAs, RFPs, depositions, ESI protocol). One-size-fits-all dossiers waste tokens and bury the relevant slice.

### G11 — No delta mode
Even with watermarks, a dossier endpoint that only returns the *full* state on every call wastes tokens. The same agent on the same case 30 minutes later usually wants "what's new since my last touch," not the entire 12k-token payload again.

### G12 — Cross-database split is invisible to composers
Per the schema-split memory, `wdws` and `athena_chat` are two distinct Postgres DBs. `ingestion_pipeline` writes only to `wdws`. A composer needs to know which DB owns which slice (cases, emails, agent notes, SMS) and assemble across them deliberately, not by accident.

---

## 4. Vision

> **ACE is the place where case situational awareness is composed. Agents are consumers of that composition, not authors of it.**

Concretely:

1. An agent picking up any new artifact (email, filing, SMS, document, calendar event) calls **one** tool with the artifact id or a case identifier and receives a structured payload containing everything it needs to act intelligently.
2. The payload is **freshly composed from the system of record on every call** — never a cached markdown export. Staleness is measured in milliseconds, not commits.
3. The payload is **agent-aware**: it knows what *this* agent has already seen on this case and can return either a full snapshot or a delta since the agent's last watermark.
4. The payload is **task-aware**: the agent can declare an intent (`"motion-response"`, `"discovery"`, `"settlement-review"`, `"triage"`) and the composer scopes which slices to include and how deeply.
5. The payload is **privilege-correct by construction**: a single `include_privileged` parameter at the composer governs every sub-query. Default is the project-standard first-party-only behavior.
6. **Closeout is a transaction.** When the agent finishes acting, it calls one closeout tool that atomically records read-set, decisions, write-set, and open questions — and advances the per-agent watermark.
7. **Materiality is first-class.** Events carry an event_type and a materiality score. Subscriptions and "recent" queries filter on materiality, not on raw row count.
8. **Routing happens at ingest.** Inbound email, SMS, and document events get a candidate `case_id` (with confidence) at write-time. Agents don't re-run that classification.

The static markdown dossier is replaced by **a live composed object** with a stable schema, owned by ACE, regenerated per call, never written to disk by an agent.

---

## 5. Architecture

### 5.1 Layers

```
┌────────────────────────────────────────────────────────────────────────┐
│                          MCP Tool Surface                              │
│                                                                        │
│  get_case_dossier(case, since=, intent=, include_privileged=False)     │
│  resolve_case(artifact_id | text | sender)                             │
│  close_agent_session(case, decisions, writes, open_questions, ...)     │
│  subscribe_case(case, materiality_floor)  [phase 2]                    │
└────────────────────────┬───────────────────────────────────────────────┘
                         │
            ┌────────────▼────────────┐
            │     Dossier Composer    │  ── stateless, applies privilege gate ──┐
            │     (Python module)     │                                          │
            └────────────┬────────────┘                                          │
                         │                                                       │
   ┌─────────────────────┼─────────────────────┐                                │
   ▼                     ▼                     ▼                                │
┌────────┐          ┌────────┐           ┌──────────┐                           │
│ Case   │          │ Event  │           │ Agent    │                           │
│ View   │          │ Stream │           │ Session  │                           │
│ (ro)   │          │ (ro)   │           │ State    │                           │
└────┬───┘          └────┬───┘           └────┬─────┘                           │
     │                   │                    │                                 │
     ▼                   ▼                    ▼                                 │
┌────────────────────────────────────────────────────────┐    ┌────────────────▼─┐
│ legal.cases / case_parties / case_documents / parties  │    │ ops.privilege_   │
│ legal.email_metadata / filing_metadata                 │    │   filter         │
│ legal.deadlines / legal.tasks                          │    │ (NULL-tolerant,  │
│ legal.sms_threads (G9 — needs case_id)                 │    │  first-party     │
│ ops.case_events (NEW — see §6.2)                       │    │  default)        │
│ ops.agent_case_watermark (NEW — see §6.3)              │    └──────────────────┘
│ ops.agent_handoffs (NEW — see §6.4)                    │
│ ops.agent_sessions (extended — see §6.5)               │
└────────────────────────────────────────────────────────┘
```

### 5.2 Read path: `get_case_dossier`

Single composed call. Returns a structured object, not markdown. Pseudocode:

```python
async def get_case_dossier(
    case: str,                       # case_number, case_id, or document_id to resolve
    *,
    agent_id: str,                   # caller identity (from MCP context)
    since: Optional[datetime] = None,# delta cursor; None = use agent watermark
    intent: Optional[str] = None,    # "motion-response" | "discovery" | "triage" | ...
    include_privileged: bool = False,
    full: bool = False,              # force full snapshot, ignore watermark
) -> dict:
    case_row = await resolve_case(case)
    cursor = since or await get_agent_watermark(agent_id, case_row.id)

    return {
        "case": {                              # always included (cheap)
            "id": ..., "case_number": ..., "case_title": ...,
            "court": ..., "status": ..., "posture": derived_posture(case_row),
            "filed": ..., "days_since_last_action": ...,
        },
        "parties": [...],                      # always included
        "deadlines": [...],                    # always; intent may sort/filter
        "events_since": [...],                 # delta unless full=True
        "open_handoffs": [...],                # G5 — structured open questions
        "recent_threads": {                    # G9 — email + SMS, intent-scoped
            "email": [...],
            "sms": [...],
        },
        "research_links": [...],               # connected research threads
        "intent_pack": {...} if intent else None,  # G10 — intent-specific slice
        "watermark": {
            "current": ...,                    # the cursor used
            "advance_to": ...,                 # what closeout should advance to
        },
        "privilege": {
            "filter_applied": "first-party-default",
            "items_redacted": <count>,
        },
    }
```

Privilege gating is applied **once at the composer**, not per-sub-query, by passing the same `safety_clause` builder used today in `case_timeline` (mcp_server_v2.py:3168) into every sub-fetch through a shared context object.

### 5.3 Write path: `close_agent_session`

Single transactional call at end of agent work. Replaces the current pattern of separate `create_agent_note` + `add_case_deadline` + `update_case_status` + free-text `agent_sessions.summary`:

```python
async def close_agent_session(
    case: str,
    *,
    agent_id: str,
    session_id: str,
    decisions: list[str],            # what the agent concluded
    writes: list[dict],              # {tool, args, result_id} for each write
    open_questions: list[dict],      # G5 — structured: {question, blocker_type, owner}
    next_actions: list[dict],        # {action, due, assignee}
    summary: str,                    # short prose for human review
    advance_watermark_to: datetime,
) -> dict:
    # in one transaction:
    #   - INSERT into ops.agent_handoffs (open_questions, next_actions)
    #   - UPDATE ops.agent_sessions SET ended_at, summary, decisions_json, writes_json
    #   - UPSERT ops.agent_case_watermark
    #   - INSERT into ops.case_events (event_type='agent_session_close', materiality=...)
```

### 5.4 Ingest path: case-id attachment

Inbound email and document ingestion writes a candidate `case_id` and `case_match_confidence` at the same write that creates the document row. A new `resolve_case` helper (offered as both a tool and an internal function) takes:

- a known artifact id (preferred — derives case from `legal.case_documents`)
- a sender + subject pair
- a free-text query

…and returns ranked candidates. It is called once at ingest, the top candidate is stored, and downstream agents read the stored value rather than re-classifying.

### 5.5 Event stream: `ops.case_events`

A new append-only table that captures **state-change events**, not just document arrivals:

| event_type           | source                                  | materiality |
|----------------------|------------------------------------------|-------------|
| document_added       | core.documents insert                    | derived     |
| email_received       | legal.email_metadata insert (direction=in) | medium    |
| filing_received      | legal.filing_metadata insert             | high        |
| deadline_added       | add_case_deadline                        | high        |
| deadline_completed   | complete_deadline                        | medium      |
| status_changed       | update_case_status                       | high        |
| agent_session_close  | close_agent_session                      | low–high    |
| sms_received         | telnyx_webhook                           | medium      |
| privileged_flagged   | privilege classifier                     | high        |

`case_timeline` continues to work (document-centric view), but a new `case_events` view returns the materiality-classified stream and is what `get_case_dossier` consumes for `events_since`.

---

## 6. Schema Changes

All new tables in `wdws` (the database `ingestion_pipeline` and the MCP server already share). Migrations follow the project's numbered convention (next free is `034`).

### 6.1 `034_case_situational_awareness.sql`

#### 6.2 `ops.case_events`

```sql
CREATE TYPE ops.case_event_type AS ENUM (
  'document_added', 'email_received', 'filing_received',
  'deadline_added', 'deadline_completed', 'status_changed',
  'agent_session_close', 'sms_received', 'privileged_flagged',
  'party_added', 'tag_added'
);

CREATE TYPE ops.materiality AS ENUM ('low', 'medium', 'high', 'critical');

CREATE TABLE ops.case_events (
  id            BIGSERIAL PRIMARY KEY,
  case_id       UUID NOT NULL REFERENCES legal.cases(id) ON DELETE CASCADE,
  occurred_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  event_type    ops.case_event_type NOT NULL,
  materiality   ops.materiality NOT NULL DEFAULT 'medium',
  actor         TEXT,                      -- agent_id, 'ingest', user email
  ref_table     TEXT,                      -- e.g. 'core.documents'
  ref_id        TEXT,                      -- the FK value as text
  payload       JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT case_events_payload_obj CHECK (jsonb_typeof(payload) = 'object')
);

CREATE INDEX ON ops.case_events (case_id, occurred_at DESC);
CREATE INDEX ON ops.case_events (case_id, materiality, occurred_at DESC);
```

Triggers on `core.documents`, `legal.email_metadata`, `legal.filing_metadata`, and the deadline/status tools insert into `case_events` on write. Materiality defaults are set by trigger but can be overridden by classifiers (e.g., a sanctions-motion classifier marks the corresponding `filing_received` row `critical`).

#### 6.3 `ops.agent_case_watermark`

```sql
CREATE TABLE ops.agent_case_watermark (
  agent_id            TEXT NOT NULL,
  case_id             UUID NOT NULL REFERENCES legal.cases(id) ON DELETE CASCADE,
  last_seen_event_at  TIMESTAMPTZ NOT NULL,
  last_session_id     UUID,
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (agent_id, case_id)
);
```

Advanced atomically by `close_agent_session`. Read by `get_case_dossier` to set the default `since=` cursor.

#### 6.4 `ops.agent_handoffs`

```sql
CREATE TYPE ops.handoff_kind AS ENUM (
  'open_question', 'next_action', 'blocker', 'watch_item'
);

CREATE TABLE ops.agent_handoffs (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  case_id       UUID NOT NULL REFERENCES legal.cases(id) ON DELETE CASCADE,
  created_by    TEXT NOT NULL,             -- agent_id
  session_id    UUID,                      -- ops.agent_sessions
  kind          ops.handoff_kind NOT NULL,
  body          TEXT NOT NULL,             -- the question / action / blocker text
  metadata      JSONB NOT NULL DEFAULT '{}'::jsonb,
  status        TEXT NOT NULL DEFAULT 'open',  -- open | resolved | superseded
  resolved_by   TEXT,
  resolved_at   TIMESTAMPTZ,
  resolves_id   UUID REFERENCES ops.agent_handoffs(id),  -- supersession chain
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ON ops.agent_handoffs (case_id, status, kind);
```

This is the structured replacement for free-text "open question" notes (G5). `search_agent_notes` continues to exist for prose; `agent_handoffs` is the deterministic surface that the dossier composer reads.

#### 6.5 Extension to `ops.agent_sessions`

```sql
ALTER TABLE ops.agent_sessions
  ADD COLUMN case_id        UUID REFERENCES legal.cases(id),
  ADD COLUMN decisions_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN writes_json    JSONB NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN intent         TEXT,
  ADD COLUMN tool_call_count INTEGER NOT NULL DEFAULT 0;

CREATE INDEX ON ops.agent_sessions (case_id, started_at DESC) WHERE case_id IS NOT NULL;
```

This makes the session row a real audit record, not a `summary` blob.

#### 6.6 SMS → case binding (G9)

Audit `legal.sms_threads` (migration 031). If `case_id` is not present, add it:

```sql
ALTER TABLE legal.sms_threads
  ADD COLUMN IF NOT EXISTS case_id UUID REFERENCES legal.cases(id),
  ADD COLUMN IF NOT EXISTS case_match_confidence REAL;

CREATE INDEX IF NOT EXISTS sms_threads_case_idx
  ON legal.sms_threads(case_id) WHERE case_id IS NOT NULL;
```

Backfill via the same `resolve_case` helper used for email at ingest.

#### 6.7 Document → case-match confidence (G3)

`legal.case_documents` already binds documents to cases but is currently populated only when ingestion knows the case. Add a sibling table for **candidate** matches so unresolved inbound mail isn't invisible:

```sql
CREATE TABLE legal.case_document_candidates (
  document_id  UUID NOT NULL REFERENCES core.documents(id) ON DELETE CASCADE,
  case_id      UUID NOT NULL REFERENCES legal.cases(id) ON DELETE CASCADE,
  confidence   REAL NOT NULL,
  signal       TEXT NOT NULL,             -- 'caption_match' | 'thread_history' | 'sender_pattern' | 'classifier'
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (document_id, case_id, signal)
);
```

A trigger or batch promoter copies high-confidence (≥ threshold) candidates into `legal.case_documents`.

---

## 7. New / Changed Tool Surface

| Tool                       | Type   | Purpose                                                                  |
|----------------------------|--------|--------------------------------------------------------------------------|
| `get_case_dossier`         | NEW    | Composed situational-awareness payload (§5.2). Replaces markdown dossier. |
| `resolve_case`             | NEW    | Map artifact id / text / sender → ranked case candidates. Used at ingest and ad-hoc. |
| `close_agent_session`      | NEW    | Transactional closeout: decisions, writes, open questions, watermark. Replaces ad-hoc combos. |
| `list_open_handoffs`       | NEW    | Per-case structured open questions / next actions / blockers (reads `agent_handoffs`). |
| `record_handoff`           | NEW    | Create a single handoff outside of closeout (mid-session blocker etc.).  |
| `case_events`              | NEW    | Materiality-filtered event stream (read of `ops.case_events`).           |
| `subscribe_case`           | NEW (phase 2) | Register interest; agent receives next event ≥ floor materiality.   |
| `case_timeline`            | KEEP   | Document-centric timeline; unchanged.                                    |
| `recent_activity`          | EXTEND | Add `since=` and `materiality_floor=` parameters.                        |
| `get_agent_context`        | EXTEND | Include open handoffs and active-case watermarks.                        |
| `create_agent_note`        | KEEP   | Free-text prose notes; not the surface for structured handoffs.          |

All new tools accept the standard `include_privileged` parameter (default `False`, which means project-standard first-party-only filter). Privilege gating is centralized in the composer (§5.2) so individual sub-queries cannot accidentally bypass it.

---

## 8. Phased Plan

Each phase is independently shippable. Earlier phases unblock the later ones; none of them depend on the markdown-dossier work being completed.

### Phase 0 — Audit & decisions (1–2 days)

- Confirm `legal.sms_threads` schema (migration 031) — does `case_id` already exist? (G9)
- Confirm where `legal.case_documents` is written from outside ingestion jobs (manual ingest paths).
- Decide whether `case_events` is a materialized view over existing tables (cheaper, read-only) or a true append-only table with triggers (richer, requires write hooks). **Recommendation: append-only table with triggers** — needed for materiality classifier overrides and for state-change events (`status_changed`, `agent_session_close`) that have no document.
- Lock the materiality enum and the default mapping table.

### Phase 1 — Schema + event stream (3–5 days)

Migration `034_case_situational_awareness.sql`:

- `ops.case_events` + indexes (§6.2)
- `ops.agent_case_watermark` (§6.3)
- `ops.agent_handoffs` (§6.4)
- `ops.agent_sessions` ALTERs (§6.5)
- Triggers on `core.documents`, `legal.email_metadata`, `legal.filing_metadata` to populate `case_events` on insert.
- `legal.case_document_candidates` (§6.7)

Backfill `case_events` from existing rows so the timeline is continuous from history, not just from migration date forward. Backfill takes the same shape as the Sprint A bulk backfill — staging table, idempotent on `(ref_table, ref_id, event_type)`, full error captured per row.

### Phase 2 — `resolve_case` + ingest hook (2–3 days)

- Implement `resolve_case` as a Python module + MCP tool. Signals: caption regex on subject/body, sender → known-counsel map, thread history (parent message's `case_id`), classifier (cheap LLM call only for low-confidence cases).
- Hook into the email ingestion path so every inbound email gets a `legal.case_document_candidates` row written with confidence.
- Promote high-confidence (≥ 0.85) candidates to `legal.case_documents` automatically. Lower-confidence stays a candidate, surfaces in a triage queue.
- Audit existing inbound email — emit candidates for everything in the last 90 days; let agents see the long tail of unbound email that has been invisible until now.

### Phase 3 — `get_case_dossier` (3–5 days)

Build the composer (§5.2). Specifically:

- Composer module `ace_dossier.py` with one entry point.
- Privilege gate threaded through every sub-fetch via a single context object.
- Watermark integration (uses `ops.agent_case_watermark`).
- Intent registry: at first, hard-code three intents (`triage`, `motion-response`, `discovery`) and their slice configurations. New intents are added by editing the registry, not by new tools.
- MCP tool wrapper.

Acceptance test: an agent receiving a new inbound email on Silver Lake calls `get_case_dossier(case=<email_doc_id>, intent="triage")` and gets back a payload that lets it answer "what should I do with this?" without any further reads.

### Phase 4 — `close_agent_session` + handoffs UI surface (2–3 days)

- `close_agent_session` MCP tool (§5.3).
- `record_handoff` MCP tool for mid-session writes.
- `list_open_handoffs` MCP tool.
- Update `get_agent_context` to surface open handoffs and active-case watermarks.
- Soft-deprecate the pattern of agents writing dossier markdown: emit a console warning if any agent writes `*_case_dossier.md` to the working directory, pointing at `get_case_dossier`.

### Phase 5 — Subscriptions (2–4 days, optional v1)

`subscribe_case(case, materiality_floor)` registers the calling agent in a new `ops.case_subscriptions` table. A poll worker (or LISTEN/NOTIFY on `case_events`) pushes notifications into the existing agent inbox / SMS / email channel based on per-agent preference. This is genuinely optional for v1; phases 1–4 deliver most of the value.

### Phase 6 — Markdown dossier deprecation (1 day, after Phase 4 lands)

- Mark existing `*_case_dossier.md` files as historical; move to `archive/` with a banner pointing at the live tool.
- Remove from any agent prompt/system-instruction that tells agents to read or write them.
- Document the new pattern in `CLAUDE.md` so future agents don't reinvent the markdown dossier.

---

## 9. How This Addresses the Original Gap

| Original failure mode (§2.1)                             | Resolution                                                                          |
|----------------------------------------------------------|-------------------------------------------------------------------------------------|
| Static dossier goes stale on first event                 | Composed live on every call from the system of record. No file to drift.            |
| Derived-state maintenance pushed onto every agent        | Composer owns derivation. Agents read; only `close_agent_session` writes back.      |
| Agents diverge across sessions                           | Single canonical payload from a single tool; structured handoffs make prior session's open questions visible deterministically. |
| Privilege boundary leaks via flat-file truth             | Privilege gate centralized at composer; one parameter controls every sub-query.     |
| Markdown encodes agent's view as ground truth            | Ground truth stays in Postgres; the composer is a pure read.                        |
| Agents re-derive case-id from each artifact every time   | `resolve_case` runs once at ingest; downstream reads use the stored value.          |
| No notion of "what's new since I last looked"            | Per-agent watermarks + delta mode in `get_case_dossier`.                            |
| Free-text closeout → next session re-derives reasoning   | Transactional `close_agent_session` records read-set, writes, decisions, handoffs. |
| All events look equal in the timeline                    | Materiality enum + `case_events` stream; agents filter on materiality.              |
| Task-context not honored (sanctions vs discovery)        | `intent=` parameter routes to specific slice configuration.                         |
| Token waste on full reloads                              | Default delta mode; `full=True` is opt-in.                                          |
| Cross-DB split is invisible                              | Composer is the one place that knows which DB owns which slice; agents don't.       |

---

## 10. Open Questions

1. **Materiality classification** — should the default for `email_received` and `document_added` be a static `medium`, or should it run a cheap classifier (Haiku) at ingest? Cost vs. signal trade-off; static default + per-event override is probably right for v1.
2. **`case_events` as table vs. view** — recommendation in §8 Phase 0 is table. Open to revisiting if trigger maintenance proves heavier than expected.
3. **Subscription transport** — LISTEN/NOTIFY (cheap, in-process), polling worker (simple, slow), or push to an existing channel (SMS, email)? Phase 5 decides.
4. **Intent registry placement** — config file vs. table. Config file is faster to evolve in v1; promote to table once the set stabilizes.
5. **Privilege filter regression coverage** — once gating moves to the composer, do we need a property test that asserts no sub-query bypasses it? Likely yes; small explicit test suite.
6. **Backfill scope for `case_events`** — last 12 months, or full history? Full history is cleaner; sizing depends on `core.documents` row count. Decide in Phase 0.
7. **Multi-agent contention on `agent_handoffs`** — what happens if two agents resolve the same `open_question` simultaneously? Compare-and-swap on `status` column is fine; document the semantics.
8. **Cross-DB composers** — does the dossier need to read from `athena_chat`, or is `wdws` sufficient? Almost certainly wdws-only for v1; revisit if a real read-need appears.

---

## 11. Non-Goals (v1)

- Real-time UI dashboards rendering the dossier. The dossier is a structured payload for agents; human visualization is a separate concern.
- Replacing `case_timeline`, `fetch_case_details`, etc. They remain useful as targeted reads for narrow tasks.
- Cross-tenant dossiers. One case at a time.
- Speculative pre-warming of dossiers. Compose on demand; revisit if profiling shows latency pain.

---

## 12. Cross-References

- MCP privilege filter (post-2026-04-24, NULL-tolerant + first-party default) — applied at composer per §5.2 and §7.
- Sprint A backfill protocol — pattern reused for `case_events` historical backfill (staging table, full error capture, idempotency on natural key).
- Dropbox Integration Service (`dropbox_integration_design.md`) — uses the same `resolve_case` helper at ingest so Dropbox-sourced documents attach to cases on the same code path as email.
- Telnyx SMS infrastructure (migration 031) — audited in Phase 0; SMS threads bind to `case_id` per §6.6 if not already.
- Hand-back delivery artifacts — unaffected; `close_agent_session` records what was generated, but does not change the existing rule that external sends go through Athena's separate review process.
