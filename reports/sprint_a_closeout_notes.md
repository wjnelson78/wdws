# Sprint A closeout notes — in progress

Running log of closeout items collected during Sprint A execution. This
file feeds into the final `/opt/wdws/reports/sprint_a_closeout.md` at
Sprint A completion per work order v2.2 §11.

Each entry: short name, date surfaced, what happened, why it mattered,
where the lesson applies going forward.

---

## Task 4 closeouts

### 1. Per-process TokenBucket design incorrect for multi-worker scenarios
**Surfaced:** 2026-04-23, during parallel legal bulk + medical reclassification run.

**What happened:** The first implementation of `TokenBucket` stored timestamps in an in-memory list per process, initialized from the DB on startup but NOT re-read between acquires. When two workers ran concurrently, each tracked only its own consumption. Combined trailing-60min consumption reached 174 against a 100/hr ceiling — 74% overshoot — before the monitor alerted.

**Why it mattered:** The 100/hr ceiling is a self-imposed conservative rate designed to stay well under any external rate-limit, preserve tier headroom, and give the monitor enough signal-to-noise to catch classifier anomalies. Overshooting by 74% defeats the rationale.

**Fix:** DB-authoritative `acquire()` — every call reads the DB for current 60-min count. Single source of truth. Bounded overshoot: worst case (N-1) tokens when N workers race between the SELECT and the staging-row INSERT. For 2 workers at 100/hr, ~1% transient overshoot. Safety cap: 60s max sleep for Ctrl-C responsiveness; cap grows with overshoot past ceiling+10 to reduce pointless re-polling.

**Verified:** Unit test at `/tmp/test_token_bucket.py` — 2 concurrent workers against a mocked shared-state bucket with scaled time. Max post-preload window count = ceiling exactly, both workers progressed.

**Where this applies going forward:** Any rate-limited operation with potentially-concurrent consumers must coordinate through shared state, not per-process state. Design default: DB-authoritative for Postgres-backed systems; Redis INCR/EXPIRE if latency matters; external service (e.g., actual API rate limiter) if strict.

### 2. Rate-ceiling violations need pre-launch verification, not post-launch monitoring
**Surfaced:** 2026-04-23, same incident as #1.

**What happened:** The per-process bucket passed unit-review because the single-worker case was correct. The multi-worker case was never tested before the parallel run. Monitoring caught the overshoot, but only after it happened.

**Why it mattered:** Monitoring detects problems; it doesn't prevent them. For safety-critical ceilings (rate limits, memory caps, query depth), the invariant must be tested under the actual operational concurrency pattern BEFORE deploying.

**Where this applies going forward:** Before launching a new operational mode (parallel workers, multi-process pipelines, concurrent API consumers), explicitly test the invariants under that concurrency pattern. Use scaled-time unit tests against shared-state mocks where possible; short real-time verification in shadow if mocks don't capture the interaction.

### 3. Approval discipline — verify implementation assumptions before authorizing new operational mode
**Surfaced:** 2026-04-23, parallel-run authorization chain.

**What happened:** The parallel-run mode was authorized on the assumption that the existing TokenBucket was multi-worker-safe. That assumption was wrong and could have been surfaced with a 60-second code read of `TokenBucket.from_db()` (it's in-memory per-process) before authorizing.

**Why it mattered:** Authorization-at-scale is cheap to grant and expensive to unwind once in flight. Verifying implementation assumptions before authorizing adds minutes; finding them out mid-run costs hours plus the closeout burden.

**Where this applies going forward:** When the directing-human is about to authorize a new operational mode (parallel run, scaled-up workload, new deployment target), the agent surfaces the key implementation assumptions that mode relies on AND names which of them have been verified vs. which are inherited from prior operation. The directing-human gets enough signal to catch inherited-but-wrong assumptions without having to request the audit explicitly.

### 4. Reporting discipline — surface-and-propose must label proposal as pending
**Surfaced:** 2026-04-23, during rate-overshoot response.

**What happened:** On detecting the 174/100 overshoot the agent halted the running workers (conservative action, correct) and in the same message began describing the Option 2 bucket fix. The framing slid toward "here's what I'm doing" before Will had confirmed the approach. Will's reply — "Option 2 is correct. Good call" — then also noted the agent had moved toward Option 2 before authorization. That's the reporting-discipline gap: the conservative action was clear, but the follow-up proposal was not clearly labeled as pending authorization.

*Note to Will 2026-04-23:* this closeout is framed from the agent-side reflection, not from an explicit correction you issued. You named the underlying pattern implicitly by flagging the sequencing. If the framing doesn't match your read of the incident, treat it as my reflection to carry forward and adjust or drop it at closeout review time.

**Why it mattered:** In surface-and-propose responses, the conservative action is already taken (and should be). The proposal part of the message needs to be clearly pending — the reader should be unable to mistake a proposed fix for a committed fix. Ambiguity here makes it harder for the directing-human to correct course, because correction requires them to first disambiguate whether a correction is even needed.

**Where this applies going forward:** When the agent takes a conservative action and proposes a follow-up, format as: "[ACTION TAKEN]: halted X. [PROPOSAL PENDING YOUR CALL]: options for Y are A/B/C." The "pending your call" label is explicit and unambiguous. Applies any time the agent is both reporting a completed action AND proposing a subsequent one in the same message.

### 5. 100/hr self-imposed ceiling — revisit for Sprint B based on Task 4 empirical data
**Surfaced:** 2026-04-23.

**What happened:** The 100/hr self-imposed ceiling was set during Sprint A planning as a conservative default. Task 4's actual Anthropic API consumption pattern (Sonnet 4.6 classification, 1 call per document) produces no anomalies, no tier friction, and — based on typical Anthropic tier limits — leaves substantial headroom.

**Why it mattered:** The ceiling limits Sprint A wall-clock by a factor of (actual-possible-rate ÷ 100). If the actual-possible-rate is 300-500/hr, the ceiling adds 3-5x to wall-clock on any classification-bound task.

**Where this applies going forward:** In the Sprint A closeout report, note the empirical Task 4 consumption shape and recommend a ceiling revision for Sprint B based on (a) measured actual consumption across Task 4 bulk, (b) current Anthropic tier limits and utilization, (c) whether any rate-related anomaly fired during Task 4 bulk. Do NOT revisit the ceiling mid-Sprint-A — the bulk run gets predictable behavior with the current ceiling; mid-stream changes add complexity without benefit.

### 6. CLASSIFIER_VERSION hardcoded constant — parameterize whenever prompt can be swapped
**Surfaced:** 2026-04-23, gate-launch abort.

**What happened:** The worker's `CLASSIFIER_VERSION` was a module-level constant (`'agent_athena_v1'`). When the medical classifier prompt was swapped from v1 to v2 for the mini-validation gate, the worker would have produced v2-prompt classifications tagged as `classifier_version='agent_athena_v1'` — breaking downstream analysis that distinguishes v1 vs v2 output. Caught at 0 classifications landed; halted and added `--classifier-version` CLI arg before restart.

**Why it mattered:** Audit-trail fidelity is load-bearing for any analysis that segments by classifier behavior — including the gate submission itself, which needed to identify v2-retained mental_health rationales by querying for `classifier_version='agent_athena_v2'`.

**Where this applies going forward:** Any field that identifies the source-of-truth for how a row was produced (prompt version, model version, pipeline stage) must be parameterizable at runtime if the system will EVER support swapping that source mid-lifecycle. Module-level constants are fine only if the constant genuinely never changes. When in doubt, parameterize — the cost is one CLI arg, the benefit is audit-trail resilience.

### 7. Evaluation methodology — compare classifier to known-population under same prompt, not fixed threshold
**Surfaced:** 2026-04-23, mini-validation gate framing.

**What happened:** The mini-validation gate was originally specified with fixed-band thresholds (<60% surface; 60-95% auto-continue; >95% halt) against strong priors about v1 being over-flagged. When the agent assembled the gate submission, it reframed the evaluation to compare the fresh-doc flag rate against the reclassification retention rate under the SAME v2 prompt (92% fresh vs 89% retained). Will confirmed the reframe was the better methodology.

**Why it mattered:** Fixed thresholds are useful when you have strong priors about what the correct outcome looks like. When the underlying population property is uncertain — as with v2's actual flag rate across the corpus — relative comparisons to a known-population-under-same-prompt are more informative. The expected signature of a correctly-calibrated classifier is that fresh-doc behavior ≈ known-population behavior under the same prompt. Divergence in either direction would indicate mis-calibration; convergence supports the "corpus property" reading.

**Where this applies going forward:** When specifying evaluation gates for classifier prompt changes, default to relative comparison against the same classifier's behavior on a known population (typically the reclassification set) rather than fixed bands against prior intuitions. Fixed bands remain useful when there's strong external ground truth OR when the evaluation is about absolute performance (e.g., "classifier must reach 95% accuracy on a gold-labeled set"). For calibration questions ("is this prompt flagging at the right rate?"), the same-prompt known-population comparison is the better anchor.

### 8. Multi-worker reveals code paths single-worker never exercised
**Surfaced:** 2026-04-23, relaunch of bulk workers post-bucket-fix.

**What happened:** The worker-mode halt check at the end of each loop iteration used `all(await fetch_next_document(conn, d) is None for d in domains_to_process)` — an `await` inside a generator expression passed to `all()`. In the single-worker sample_batch runs that had been the operational default up to this point, this halt check was never reached: the sample_size target was hit first, and the sample_batch return path exited before the worker-mode halt check. When the code path was finally reached in `--mode worker` for the bulk run, it crashed with `TypeError: 'async_generator' object is not iterable`. The prior-conversation summary incorrectly noted this as fixed; it had not been.

**Why it mattered:** Single-worker sequential operation exercises a subset of a pipeline's code paths. When the operational mode shifts (parallel workers, bulk mode vs sample_batch, production vs test workload), previously-unreached code gets exercised and latent bugs surface. The cost here was ~10 minutes of agent time to diagnose and fix, caught by both workers crashing early rather than producing wrong output — but it didn't have to be benign. A latent data-corruption bug in that same code path would have landed on production data before being caught.

**Where this applies going forward:** When a new operational mode is being deployed (even if the underlying code is "already in production"), test the code paths that mode will exercise. For mode-switch moments like "single worker → parallel," "sample_batch → worker," "test workload → full corpus," explicitly list the code paths the new mode newly reaches and verify each. Where possible, add integration tests that exercise those paths before deployment; where that's not feasible, at minimum do a short real-time verification with abort-on-first-error.

**Related:** This is the code-correctness sibling of closeout #2 (rate-ceiling verification). Both are "the invariant held in mode X, but mode Y was never tested." Different failure modes, same root cause — scale-up-mode concurrency and scale-up-mode code paths both need explicit verification, not inheritance from single-mode success.

---

## Pre-Task-4 closeouts (carried forward from earlier sprint stages)

*None yet — add as they come up.*
