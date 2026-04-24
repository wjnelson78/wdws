# wdws-agents.service SyntaxError — incident closeout

**Work order:** `INCIDENT_WDWS_AGENTS_SYNTAX`
**Branch:** `incident-wdws-agents-syntax` (not merged to master)
**Fix commit:** `3283c43`
**Executed:** 2026-04-23, single Claude session working in parallel with Sprint A `sprint-a-privilege-phi` branch (Sprint A in-flight work stashed, popped cleanly post-fix).
**Outcome:** Service healthy. All 13 agents registering runs. Restart counter zeroed.

---

## 1. Original parse error

```
  File "/opt/wdws/agents/agent_dba.py", line 681
    except Exception as main_error:
    ^^^^^^
SyntaxError: invalid syntax
```

Reproduced with `python3 -m py_compile /opt/wdws/agents/agent_dba.py`. Blocked `run.py`'s import chain (`from agent_dba import DBAAgent`), preventing the service from starting since 2026-04-19.

## 2. Root cause

`DBAAgent.run()` (a 12-section database health-check walk) has per-section `try/except` blocks at 8-space indentation. Commit `f512216` (2026-04-19, "Commit pending platform, dashboard, and agent updates", +637/-263) added an `except Exception as main_error:` at line 681 intended as an outermost wrapper around sections 1–12 — the handler body tracks `current_section`, runs a connection-pool reset on connection-related errors, and returns a structured error dict. The opening `try:` for that wrapper was never added to the commit. Python saw an `except` at method-body indent with no matching `try:` at the same level and rejected the file.

Commit `d32592a` (2026-04-21) was a small connection-pool query cleanup that silently fixed an *earlier* parse error at line 178, revealing the line 681 error that had been masked behind it. The file has been unparseable since April 19 (4 days), not April 23 as the work-order narrative framed it.

## 3. Fix applied

**Option B per §4.3 (preserve original intent):** insert `try:` at line 165 and re-indent the 516 lines of sections 1–12 by +4 spaces so they nest inside the new `try:` block, which the existing `except Exception as main_error:` at (now) line 682 closes.

Verified mechanical purity:
- `diff -w /tmp/dba_history/pre_fix.py /opt/wdws/agents/agent_dba.py` showed exactly one change: `164a165 > try:`. No statements moved, no blocks restructured — the re-indent was pure whitespace.
- `python3 -m py_compile agents/agent_dba.py` — pass.
- `python3 -c "from agent_dba import DBAAgent"` — pass.
- `python3 -m py_compile agents/run.py` — pass.
- `python3 -c "from run import *"` — pass.

Option A (delete the dangling except block) was considered and rejected on Will's direction. The cascade handler has never run in production (service has been broken its entire lifetime), but that's not the same as "aspirational code" — the author designed section-by-section error recovery deliberately for a database-monitoring agent, and a 515-line mechanical re-indent carries near-zero behavioral risk when verified via `diff -w`. Discarding the design on incident-fix grounds would violate §10 rule 4 ("preserve original intent").

## 4. Service state post-restart

Restart issued `2026-04-23 18:25:38 PDT`. State at +2 min:

| Check | Value |
|---|---|
| `systemctl is-active` | `active` |
| `NRestarts` | `0` (zeroed by clean start; pre-restart was `7598`) |
| `ActiveState` / `SubState` | `active` / `running` |
| Journal errors (2 min window) | Only pre-existing OpenAI 401 warnings (invalid OpenAI key; fallback providers handle it — unrelated to this incident) |

## 5. Fleet inventory — all 13 agents registering runs post-restart

Verified in `ops.agent_runs` in the 5-minute window following restart:

| Agent | First post-restart run | Status |
|---|---|---|
| orchestrator | 18:25:41 | success |
| code-doctor | 18:25:41 | success |
| self-healing | 18:25:46 | success |
| daily-digest | 18:26:11 | success (no notifications queued — expected) |
| security-sentinel | 18:26:11 | success (0 clients, 0 PII hits) |
| watchdog | 18:26:11 | success |
| **dba** | 18:26:41 | success *(this is the agent whose SyntaxError caused the incident)* |
| data-quality | 18:26:41 | success |
| quality-eval | 18:26:41 | success |
| case-strategy | 18:27:11 | success |
| query-insight | 18:27:11 | success |
| retention | 18:27:11 | success |
| athena | 18:30:11 | success (45s duration) |

## 6. What the fleet missed during the 4-day outage

Between 2026-04-19 ~17:31 PDT (commit `f512216` landed) and 2026-04-23 18:25:38 PDT (fix restart), these scheduled workflows did not run:

- Morning briefs (whatever cadence they're scheduled at)
- Retention agent Sunday 3 AM cron (would have fired 2026-04-19 03:00 and was already missed by the time the break landed; next would be 2026-04-26)
- Security Sentinel sweeps (every restart cycle now normally)
- Data Quality sweeps
- Self-Healing and Code Doctor autonomous triggers
- Watchdog scheduled runs
- DBA health checks (the agent whose own parse error caused the outage)
- Daily Digest email delivery (any queued notifications from that window are still pending and will be consolidated into the next daily digest that finds non-empty state)

Sprint A Task 5 recurring-report schedule can now use path (b) scheduled execution instead of path (a) manual invocation.

## 7. Latent issues surfaced — NOT fixed in this work order

These are called out for follow-up attention in a separate engagement, per §10 rule 1 (no scope creep in incident fixes).

### 7.1 Pre-commit parse-check hook

Commit `f512216` landed with a 637-insertion rewrite that didn't parse. No mechanism caught this at commit time. A minimal `.git/hooks/pre-commit` running `python3 -m py_compile` on staged `.py` files would have rejected the commit and prevented the entire 4-day outage. One-line fix, half-hour of work, prevents recurrence of this exact class of incident.

Analog to Sprint A's "preflight completeness must cover the full operating scenario" lesson — same idea at commit time instead of at sprint-start time.

### 7.2 External monitoring of wdws-agents.service

The agent framework contains Watchdog and Self-Healing agents whose job is to catch exactly this kind of failure. When the failure is "the service hosting the agents is down," those agents cannot run, and the failure goes undetected. 7554 restarts accumulated over 4 days with no alert.

Fix shape: a systemd timer (or cron) external to `wdws-agents.service` that runs `systemctl is-active wdws-agents.service` every N minutes and pages/emails on non-active. Must be external to the agent framework so it remains functional when the framework is down. Estimated half-day of work. Prevents this class of incident from recurring *silently* (doesn't prevent the crash itself, but cuts detection latency from days to minutes).

This is the more important of the three latent issues — the single-point-of-failure pattern of "fleet monitors the fleet" is architectural, not incidental.

### 7.3 Defensive try/except nesting in `agent_dba.py`

The cascade handler (lines ~682–732 of the fixed file) contains four levels of nested `try/except` — error logging wrapped in try, finding-log wrapped in try, return-dict construction wrapped in try, all wrapped in another try for "absolute last resort." Reads like the author was chasing ghost failures by adding another try around each one. Might be worth a simplification pass if the file is revisited, but it's stylistic — not broken, not a bug, not in scope for this incident. Lowest priority of the three.

## 8. Observations that don't fit cleanly elsewhere

### 8.1 Sprint A workers exited at 18:11 PDT — before my incident work

During preflight (§3, ~18:12 PDT) I checked `ps aux | grep sprint_a_llm_backfill` and saw 3 processes running. I confirmed to Will that workers were alive and proceeded with the stash-and-switch. I was wrong.

The workers had actually exited at 18:11 (log files' final mtime: legal 18:11:08, medical 18:11:27). `ps` showed the bash wrappers still alive because they execute `pwd -P >| /tmp/claude-*-cwd` as a post-python cleanup step — a brief window where bash is alive but the python child has exited. I caught them in that teardown window and misread "bash wrapper alive" as "worker alive."

State at exit:
- Legal worker: 350 docs classified under `agent_athena_v1`
- Medical worker: 184 docs classified under `agent_athena_v2`
- No error in either log; logs just stop mid-classification
- No kill/OOM/sigterm signal visible in systemd journal for the 18:09–18:13 window

Cause of the exit is unknown. This is pre-existing condition from before my incident fix (14 minutes before my service restart), and Sprint A worker management is out of this work order's scope (§2). **Surfacing for Will's attention**: the Sprint A Task 4 bulk classification workers are not currently running, and whatever caused their exit at 18:11 needs a look separate from this incident.

Preflight lesson: check log-file mtimes alongside `ps` — a process showing in `ps` is necessary but not sufficient evidence that work is happening. Sprint A's observability checklist probably deserves a line item on this.

### 8.2 `ops.agent_runs` entries from before my restart

Three entries appear at 18:24:19 PDT (athena, code-doctor, orchestrator) — 79 seconds *before* my `systemctl restart` at 18:25:38. During this window, `NRestarts` was climbing monotonically (7554 → 7598) with no clean-start reset, so the systemd-managed process could not have been successfully serving these runs.

Most likely explanation: the agents have an alternate invocation path (direct Python call, or a different systemd unit, or a manually-triggered worker) that writes to `ops.agent_runs` independently of `wdws-agents.service`. This is surprising and slightly concerning because it means "the state of the fleet" is not fully captured by "the state of the service," but it's not broken — just not what I'd predicted. Noting for the record.

### 8.3 Process lesson: chained git commands collapse intermediate failure visibility

During the merge-to-master step I chained `git checkout master && git merge incident-wdws-agents-syntax` (implicitly, via a batched Bash call that ran them as a sequence under one command invocation). The checkout aborted because uncommitted Sprint A working-tree changes conflicted with master's version of those files, but the merge ran anyway against the still-current branch (`sprint-a-privilege-phi`), producing a merge commit on the wrong branch. The aborting `error: Your local changes...` line was visible in the combined output but I read past it on first pass, catching the mistake only when reviewing the resulting `git log`. The merge was undone via `git reset --merge HEAD~1` (safe because the commit was local, two minutes old, unpushed), and master was subsequently merged correctly via a clean fast-forward.

**Guidance for future work (Sprint A remainder and Sprint B):** any sequence involving `checkout`, `reset`, `merge`, `rebase`, `cherry-pick`, or `push --force` runs as separate commands with explicit output verification between them. Not "I'll watch for errors" — explicit verification as its own step. Chained commands make the intermediate failure invisible unless every step's exit status is enforced (e.g., via `&&`, though even that doesn't help if later commands don't depend on earlier succeeding in a way the shell sees). The discipline is structural: destructive-adjacent operations never batch.

## 9. Post-work state

- **Fix:** merged to `master` as a clean fast-forward: `3283c43` (fix) and `ba8732c` (closeout) as direct descendants of `e63f886`. No merge commit.
- **Branch `incident-wdws-agents-syntax`:** still exists locally, harmless, can be deleted at leisure.
- **Sprint A branch:** restored to its pre-incident-work state after a separate reset-and-recover operation (see §8.3). In-flight modifications and untracked files intact.
- **Service:** healthy, all agents firing.
- **Sprint A workers:** not running (pre-existing condition, see §8.1).

---

*Written by Claude Opus 4.7, 2026-04-23 PDT.*
