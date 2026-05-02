# Litigation-Priority Addendum

**Phase 0 deliverable.** Author: Athena RCW-Ingest agent. Date: 2026-04-28.
Pre-loads RCW citation anchors from operator memory and active-docket context
so Phase 5 canary suites don't need to discover them at canary-time.

---

## 1. Active docket (12 cases)

Pulled from `Athena.list_cases`. Used to drive priority-title weighting.

| Case # | Court | Title | Doc count | Priority titles implicated |
|---|---|---|---|---|
| `325CV05551DGE` | US WDWA | Nelson v BIIA et al | 259 | **51** (Industrial Insurance), 49 (Labor) |
| `26-2-00762-31` | Snohomish | IAFF v SSCFR Nelson | 653 | **42** (PRA via 42.56), **2** (Courts), 41 (Public Empl.) |
| `2:26-cv-00776-JCC` (linked, federal companion to records-access work) | US WDWA | (Title II ADA / federal civil-rights matter; per Athena memory) | n/a | 42 (PRA), 4 (Civil Procedure analogues) |
| `19-2-03310-31` | Snohomish | FTCU v Nelson | 304 | 4 (Civil Procedure), 19 (Business Regulations) |
| `24-2-01031-31` | Snohomish | Nelson v Starbucks | 337 | **49** (Labor; **49.60** WLAD), 4 |
| `23-2-09435-31` | Snohomish | Nelson v Unum Group | 207 | (federal-preempted ERISA primarily; collateral state issues 48 Insurance, 49) |
| `226CV00337KKE` | US WDWA | Nelson v Silver Lake et al | 257 | (state-action / civil-rights — 42, 49) |
| `25-2-00238-31` | Snohomish | Nelson vs FTCU et al | 115 | 4, 19 |
| `24-2-07413-31` | Snohomish | Nelson v Trevor Haward et al | 219 | (likely 4, 7 special proceedings; verify with operator) |
| `23-2-07759-31` | Snohomish | Nelson v Prisma Wellness | 177 | 49 (employment/leave?), 18 (professions) |
| `24-2-01034-31` | Snohomish | Nelson v Brian Baseggio | 72 | (likely 7, 4) |
| `24-2-01033-31` | Snohomish | Nelson v Stuart Gibson | 74 | (likely 7, 4) |
| `24-2-01032-31` | Snohomish | Nelson v Olivia Kuker | 72 | (likely 7, 4) |

**Title implication weights (across the docket):**

```
Title 4  — implicated in 8+ cases (Civil Procedure is universal)
Title 42 — implicated in 4+ cases (PRA + records-access work is central)
Title 49 — implicated in 4+ cases (employment / WLAD / Starbucks / Prisma)
Title 51 — implicated in 1 high-stakes case (BIIA, 259 docs)
Title 2  — implicated in court-administration / records-access work
Title 41 — implicated in IAFF / SSCFR public-employment dispute
Title 48 — collateral to Unum (insurance) and Silver Lake (insurance/state-action)
Title 19 — collateral to FTCU and FTCU-related (business regulations)
Title 7  — likely implicated in several Snohomish writs / special proceedings
Title 11 — possible probate-context references
Title 26 — referenced in Sara Murray work (RCW 26.44 abuse reporting; RCW 26.09.191 parenting plan)
```

Priority-core (from kickoff doc) of `51 + 42 + 4 + 49` is correct and matches the docket weighting exactly.

---

## 2. Pre-loaded mandatory canary anchors (Phase 5 §7.1)

Operator-supplied anchors merged with anchors surfaced from `recall_personal_memories` and `search_agent_notes`. These are the sections Phase 5 canary suites must hit on cited litigation patterns. Each anchor is paired with the `legal-framework` topical tag it should carry post-Phase-4 ingest.

### Title 42 — Public Officers and Agencies

| Citation | Subject | Topical tag | Why it matters in Will's docket |
|---|---|---|---|
| RCW 42.56.030 | PRA — Construction (broad pro-disclosure mandate) | `pra` | Foundation for every PRA claim Athena makes; framing in Snohomish records-access work |
| RCW 42.56.520 | PRA — Prompt responses required (5-business-day window) | `pra` | Active timing-violation theory in Galvin/Romero records work (per Athena memory 4/24 Romero decision analysis) |
| RCW 42.56.550 | PRA — Judicial review of agency action | `pra` | Authority for GR 31.1(d)(4) writ track-1 if Snohomish stalls past May 1, 2026 (per memory 4/19 GR 31.1 Petition + Title II Grievance) |
| RCW 42.56.580 | PRA — open public records officer (recodified from 42.17 in 2006) | `pra` | Recodification-chain test: must resolve via `rcw.cross_references` |
| RCW 42.30 (entire chapter) | Open Public Meetings Act | `oma` | Different structural pattern from 42.56; per Phase 0 sample dissection |

### Title 49 — Labor Regulations

| Citation | Subject | Topical tag | Why |
|---|---|---|---|
| RCW 49.60.030 | WLAD — Right of equality, freedom from discrimination | `wlad` | Core WLAD framing in Starbucks, Prisma, Silver Lake work |
| RCW 49.60.180 | WLAD — Unfair employment practices (the workhorse provision) | `wlad`, `employment-discrimination` | **Cross-jurisdiction canary Test A anchor** — must rank alongside 42 U.S.C. § 12112 |
| RCW 49.60.215 | WLAD — Unfair practices in places of public accommodation | `wlad` | Public-accommodation parallel to ADA Title III |
| RCW 49.60.040 | WLAD — Definitions | `wlad` | Definitional hook for "disability," "employer" — protected-class boundary |

### Title 51 — Industrial Insurance

| Citation | Subject | Topical tag | Why |
|---|---|---|---|
| RCW 51.32.040 | Industrial Insurance — assignment of compensation, attachment | `bii` | Operator-listed anchor (kickoff) |
| RCW 51.52.060 | BIIA — appeals procedure | `bii` | Direct procedural anchor for Nelson v BIIA |
| RCW 51.52.115 | BIIA — judicial review (the hook into superior court) | `bii` | Phase 0 dissection sample; the §50 escape valve in workers' comp matters |

### Title 4 — Civil Procedure

| Citation | Subject | Topical tag | Why |
|---|---|---|---|
| RCW 4.16.080 | Statutes of limitations — 3 years for personal injury, fraud, etc. | `wa-civil-procedure`, `statute-of-limitations` | Phase 0 dissection sample; relevant to most active Snohomish litigation timing analysis |
| RCW 4.96.010 | Tortious conduct of governmental entities — claims procedure | `wa-civil-procedure`, `tort-claims-act` | Required pre-suit notice for any state/municipal defendant (Snohomish entities, AOC, etc.) |
| RCW 4.96 (entire chapter) | Claims against governmental entities — full chapter | `wa-civil-procedure`, `tort-claims-act` | Operator-listed full chapter; central to civil-rights against state actors |
| RCW 4.84.185 | Frivolous-action attorney fees | `wa-civil-procedure` | Pro se litigation discipline marker; defensive against opposing counsel motions |

### Title 41 — Public Employment, Civil Service, Pensions

| Citation | Subject | Topical tag | Why |
|---|---|---|---|
| RCW 41.06 (entire chapter) | State Civil Service Law | `wa-public-employment` | IAFF v SSCFR and any state-employee discipline issues |
| RCW 41.40 (entire chapter) | PERS — Public Employees' Retirement System | `wa-public-employment` | Deferred-comp / pension-collateral aspects of state-employee disputes |

### Title 2 — Courts of Record

| Citation | Subject | Topical tag | Why |
|---|---|---|---|
| RCW 2.04 (entire chapter) | Supreme Court rule-making power | `wa-courts` | Statutory authority under which GR 31 / GR 31.1 are promulgated; relevant to "GR 31 is the floor not the ceiling" doctrinal framing per Athena memory 4/22 |
| RCW 2.36 (entire chapter) | Juries | `wa-courts` | Jury procedure for civil cases — relevant to TRO and trial-readiness work |

### Title 9, 9A, 10 — Criminal

| Citation | Subject | Topical tag | Why |
|---|---|---|---|
| RCW 9A.82 (entire chapter) | Washington Criminal Profiteering Act (state-RICO analogue) | `wa-rico` | **Critical operator-relevant gap**: per Athena memory 4/22, RCW 9A.82.010(4)(i) lists ONLY public-servant bribery (9A.68.010/.020/.040/.050) as state-RICO predicates — commercial bribery 9A.68.060 is NOT a listed predicate. This gap is why the Cat F bribery analysis in Sara Murray work routes through federal RICO, not state. The corpus must accurately preserve 9A.82.010(4) so this analysis can be re-derived. |
| RCW 9A.68.010, .020, .040, .050, .060 | Bribery (public-servant + commercial) | `wa-criminal` | Same memory; predicate-act mapping for civil-RICO theory work |
| RCW 9A.04.020 (current) and 9A.04 (chapter) | Washington Criminal Code definitions and general provisions | `wa-criminal` | Phase 0 dissection sample |
| RCW 10.01.180(3)(a) | Post-*Bearden* indigency provision (legal-financial obligations) | `wa-criminal-procedure`, `indigency` | Operator-listed; central to "WA indigency doctrine" research thread per Athena note 17 (HANDOFF: WA Records Access / Galvin Dual-Role Theory, 4/28) |

### Title 26 — Domestic Relations

| Citation | Subject | Topical tag | Why |
|---|---|---|---|
| RCW 26.44 (entire chapter) | Child abuse — mandatory reporting, investigations, GAL framework | `wa-domestic-relations`, `child-abuse-reporting` | Operator-listed; central to Sara Murray Branch B work (DCYF / forensic interview / RCW 26.44.186-.188 records) per memory 4/20 |
| RCW 26.09.191 | Parenting plan — restrictions on residential time, decision-making | `wa-domestic-relations` | "Frequently misread by parties, attorneys, and judges" per ESHB 1620 chart (Athena memory 4/20); central to Sara Murray strategic axis |

### Title 36 — Counties

| Citation | Subject | Topical tag | Why |
|---|---|---|---|
| RCW 36.18.016 | Court fees | `wa-courts`, `court-fees` | Operator-listed; "May not Shall" argument in Athena note 12 (GR 31.1(h)(4) discretion) interacts with RCW 36.18.016 fee discretion analysis |

### Title 71 + 71A + 74 — Mental Health / Disabilities / Public Assistance

| Citation | Subject | Topical tag | Why |
|---|---|---|---|
| RCW 71 (chapter index) | Mental Illness — civil commitment, treatment | `wa-mental-health` | Collateral framework for ADA Title II / OLC integration mandate analyses |
| RCW 71A (chapter index) | Developmental Disabilities | `wa-mental-health` | DDA-touching cases |
| RCW 74.34 | Vulnerable Adults — civil cause of action | `wa-public-assistance`, `vulnerable-adults` | Memory 4/22 noted "RCW 74.34 vulnerable-adult civil cause of action does NOT cleanly fit 2014 hospital scenario" — corpus must preserve text accurately so this analysis is reproducible |
| RCW 11.88.010(1) | Common-law incapacitated-person doctrine | `wa-probate`, `incapacitated-person` | Memory 4/22 — fallback theory when RCW 74.34 doesn't fit |

---

## 3. Cross-jurisdiction parallels (for §M of retrieval guide)

Pairings that the priority-core canary should be able to surface together when called with `jurisdiction_filter=None`:

| Federal anchor | WA state anchor | Doctrinal parallel |
|---|---|---|
| 5 U.S.C. § 552 (FOIA) | RCW 42.56.030 / .520 / .550 (PRA) | Records-access frameworks |
| 42 U.S.C. § 12112 (ADA Title I employment) | RCW 49.60.180 (WLAD employment) | Employment disability discrimination |
| 42 U.S.C. § 12132 (ADA Title II services/programs) | (no clean WA-statutory parallel; WA Const. Art. I § 12 + WLAD § 215 partial) | Public-services disability access — FORE: WA has no clean parallel; Phase 6 §M should explicitly note this asymmetry |
| 42 U.S.C. § 1983 | (WA constitutional torts via RCW 4.96.010 + state common-law tort) | Civil-rights remedy |
| 18 U.S.C. § 1961 (federal RICO predicates) | RCW 9A.82.010(4) (state criminal-profiteering predicates) | Predicate-act analysis — note the 9A.68.060 commercial-bribery gap |
| 29 U.S.C. § 1132(a)(1)(B) (ERISA benefits action) | (federal-preempted; WA insurance code 48.* survives only at margins) | LTD / health benefits — Athena memory 4/24 Beth Davis correction is canonical |

The Phase 5 cross-jurisdiction canary (Test A + Test B from `cross_jurisdiction_design.md`) covers two of these pairings; the rest become organic test material for Phase 5 spot-checks across the priority-core.

---

## 4. Items the operator may want to add (verify before Phase 4)

Surfaced from Athena memory but not on the operator's pre-loaded list — confirm before committing them as mandatory canaries:

1. **RCW 42.17A** — Campaign Disclosure (former 42.17 contents recodified to 42.17A in 2010, then to Title 29B in 2024). Recodification chain is canonical test material for `rcw.cross_references` recodification logic.
2. **RCW 29B (entire title)** — Campaign Disclosure (the latest landing place after the 42.17 → 42.17A → 29B recodification). Currently absent from the priority list but recodification-chain canaries imply this title should at least be reachable.
3. **RCW 7.16** — Special Proceedings: certiorari, mandamus, prohibition (writs). Memory 4/21 (Sara Murray work) flagged "RAP 16.2 and RCW 7.16 structure mandamus against state officers in a separate court, not against the Supreme Court itself filed in that same court" — relevant doctrinal framework for any state-mandamus track.
4. **RCW 19.86** — Consumer Protection Act (CPA). Often paired with WLAD or PRA in WA civil-rights claims. Touched in FTCU and Starbucks contexts.
5. **RCW 4.92** — Claims and actions against the state (parallels 4.96 for state-vs-municipal entities). Operator listed RCW 4.96 (governmental entities) — RCW 4.92 (state) is the sibling; verify.
6. **RCW 28A.642** — Discrimination in public schools (referenced in WA Constitution Art. IX context).
7. **RCW 70.02** — Medical records / health-care information privacy. Touched in Sara Murray medical-records work and ADA reasonable-accommodation analyses.
8. **RCW 18.71** — Physicians (medical practice act). Touched in any medical-records or DCYF physician-witness context.

These are flagged for operator consideration, not added to §7.1 mandatory canaries without authorization.

---

## 5. GR 31 / GR 31.1 — confirmed OUT OF SCOPE per kickoff

GR 31 (Access to Court Records) and GR 31.1 (Records Access — Administrative Records) are court rules promulgated by the WA Supreme Court under authority of RCW 2.04, NOT statutes in the RCW. Per kickoff §10, court rules are Phase 8 backlog. This Phase 4 ingest will NOT include GR 31 / GR 31.1 source text.

The corpus will include RCW 2.04 (the rule-making power), so an agent asking "where does GR 31 come from?" can be pointed to RCW 2.04 even though GR 31 itself isn't in the corpus. Document this in Phase 6 §M as the canonical "court rule out-of-corpus" pattern.

---

## 6. Open questions for operator confirmation

1. **Title 26 inclusion**: kickoff §2.3 marked Title 26 as "only if relevant to GAL matters". Active docket has Sara Murray work (Branch B, RCW 26.44, RCW 26.09.191) which clearly implicates Title 26. Recommend full inclusion of Title 26 in priority-core. **Confirm.**
2. **Title 19** (Business Regulations Misc, 111 chapters): touched by FTCU work but mostly long-tail. Recommend NOT priority-core; let it land in long-tail. **Confirm.**
3. **Title 48** (Insurance, 101 chapters): touched by Unum and Silver Lake work but most insurance disputes route through federal preemption. Recommend NOT priority-core. **Confirm.**
4. **RCW 19.86** (CPA): touched in multiple cases. Recommend ADD to priority-core anchors. **Confirm.**
5. **RCW 7.16** (writs): recommended for inclusion in priority-core anchors. **Confirm.**
6. **Phase 4 wall-time revision**: per `rcw_source_findings.md` §6.4, full corpus likely 24–48+ hours of embedding wall-time, not 7–10. Priority-core (51+42+4+49) is ~2.5 hr at the throughput baseline. Pause-checkpoint cadence in kickoff doc §6 should be re-paced around section milestones. **Confirm.**

End of litigation-priority addendum.
