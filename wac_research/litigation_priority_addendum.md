# WAC Litigation-Priority Addendum

**Phase 0 deliverable.** Author: Athena WAC-Ingest agent. Date: 2026-04-30 (revised from preliminary).
Companion to `wac_source_findings.md` and `authority_chain_design.md`.
Pre-loads WAC citation anchors from operator memory and active-docket context
so Phase 5 canary suites don't need to discover them at canary-time.

Parallels `/opt/wdws/rcw_research/litigation_priority_addendum.md`.

---

## 1. ⚠ Surfaced anomaly — Title 222 in operator-locked priority list

The kickoff briefing §4.5 (priority sequence, slot 6) reads:

> 6. Title 222 (Health Care Authority — parallel to RCW 70 health care)

**Verified factual delta:** WAC Title 222 is the **Forest Practices Board** (15 chapters). The Health Care Authority is **WAC Title 182** (41 chapters), administered under chapter 41.05 RCW.

This is an off-axis anomaly per kickoff §5.3 halt-and-investigate protocol. Two interpretations:

- (a) Operator typo: meant Title 182, wrote 222. Most likely — the parenthetical "Health Care Authority — parallel to RCW 70 health care" describes Title 182's actual scope.
- (b) Operator intentionally wanted Forest Practices in slot 6. Unlikely — Forest Practices is irrelevant to active docket and has no clear health-care / RCW 70 parallel.

**Recommendation:** swap Title 182 into slot 6, drop Title 222 from priority sequence (or relegate to long tail). **Halt before Phase 4 priority-priority block (Pause-Checkpoint A) until operator confirms.**

Title 222 (Forest Practices Board) is genuinely irrelevant to the active docket — keep in long tail. RCW 70 is administered by Department of Health (Title 246, already in slot 5) and HCA (Title 182). Slot 5 + corrected slot 6 give complete RCW Title 70 coverage.

---

## 2. Active docket — WAC implication mapping

Pulled from `Athena.list_cases` and the RCW litigation-priority addendum. WAC implication = administrative regulations that operationalize each implicated RCW.

| Case # | RCW titles already mapped | Implicated WAC titles |
|---|---|---|
| `325CV05551DGE` Nelson v BIIA | RCW 51, 49 | **WAC 296** (L&I — BIIA appeals procedures, ch 296-15 / 296-17 / 296-22) |
| `26-2-00762-31` IAFF v SSCFR Nelson | RCW 42 (PRA), 2, 41 | **WAC 357** (state HR + civil service procedures), **WAC 44** (Code Reviser / agency rulemaking; chapter count not yet sampled) |
| `2:26-cv-00776-JCC` (federal records-access companion) | RCW 42, 4 | (each agency promulgates its own PRA WAC; centralized in WAC 44) |
| `19-2-03310-31` FTCU v Nelson | RCW 4, 19 | **WAC 208** (Department of Financial Institutions — credit union framework), **WAC 458** (revenue/excise as collateral) |
| `24-2-01031-31` Nelson v Starbucks | RCW 49.60, 4 | **WAC 162** (Human Rights — WLAD employment regulations, ch 162-16, 162-22, 162-26) |
| `23-2-09435-31` Nelson v Unum Group | RCW 48 (preempted by ERISA at federal level) | **WAC 284** (Insurance Commissioner — collateral state insurance reg) |
| `226CV00337KKE` Nelson v Silver Lake | civil rights | **WAC 162** (WLAD enforcement parallels) |
| `25-2-00238-31` Nelson v FTCU et al | RCW 4, 19 | **WAC 208** |
| `24-2-07413-31`, `24-2-01034-31`, `24-2-01033-31`, `24-2-01032-31` | RCW 7, 4 | (Civil-procedure analogues live in CR / GR — Phase 8 court rules; not WAC-implicated) |
| `23-2-07759-31` Nelson v Prisma Wellness | RCW 49, 18 (medical practice) | **WAC 246** (Department of Health — RCW 18 implementation for medical practitioners) |
| Sara Murray Branch B work (collateral) | RCW 26.44, 26.09 | **WAC 110** (DCYF — CPS, GAL, foster care, kinship care procedures) |

**Title implication weights (across the docket):**

```
WAC Title 296  — implicated in 1 high-stakes case (BIIA, 259 docs); 71 chapters
WAC Title 162  — implicated in 2+ employment/civil-rights cases; 14 chapters (small but high-density)
WAC Title 110  — implicated in Sara Murray Branch B work (CPS / GAL); 28 chapters
WAC Title 246  — implicated in Prisma medical context, collateral; 86 chapters
WAC Title 182  — HCA (the Title-222 typo correction); 41 chapters; cross-cutting health-care administration
WAC Title 388  — DSHS; 88 chapters; RCW 74 implementation
WAC Title 357  — IAFF v SSCFR civil service; 12 chapters
WAC Title 192  — Employment Security; 44 chapters
WAC Title 308  — Department of Licensing; 71 chapters
WAC Title 458  — Department of Revenue; 21 chapters
WAC Title 44   — Code Reviser / agency rulemaking; chapter count not yet sampled
```

---

## 3. Operator-locked priority sequence (with Title 222→182 correction proposed)

Per kickoff §4.5, with §1 anomaly resolved:

**Pause-Checkpoint A (priority core):**

1. Title 296 (L&I — BIIA workers' comp regulations) — 71 chapters
2. Title 162 (Human Rights Commission — WLAD enforcement) — 14 chapters
3. Title 388 (DSHS — public assistance, child welfare) — 88 chapters
4. Title 110 (DCYF — child welfare framework) — 28 chapters

Subtotal: 201 chapters.

**Pause-Checkpoint B (priority-priority):**

5. Title 246 (Department of Health) — 86 chapters
6. **Title 182 (Health Care Authority)** — 41 chapters [**corrected from Title 222 per §1**]
7. Title 357 (state HR/civil service) — 12 chapters
8. Title 192 (Employment Security Department) — 44 chapters
9. Title 308 (Department of Licensing) — 71 chapters
10. Title 458 (Department of Revenue) — 21 chapters

Subtotal: 275 chapters.

**Long tail:** 106 remaining titles (116 populated − 10 priority titles). Estimated ~1,800 chapters at long-tail mean of ~17 chapters/title.

**Total project estimate:** ~2,300 chapters, ~30,000 active sections.

---

## 4. Pre-loaded mandatory canary anchors (Phase 5 §7.1 equivalent)

Each anchor paired with its authorizing RCW (the regulatory authority chain — both should appear in `wac.authority_chain` post-Phase-4-resolve) and the topical tag the WAC chunk should carry.

### Title 296 — Department of Labor and Industries (BIIA + workers' comp)

| Citation | Subject | Authorizing RCW (expected) | Topical tag | Why it matters |
|---|---|---|---|---|
| WAC 296-14A (chapter) | Structured settlement procedures | RCW 51.04.020, 51.04.030, 2011 1st sp.s. c 37 § 305 | `bii-regulations-wac` | Settlement framework for state-fund claims; verbatim-sampled in Phase 0 |
| WAC 296-15 (chapter) | Self-insurer claims management | RCW 51.14, 51.16 | `bii-regulations-wac` | Verbatim-sampled in Phase 0; large dispo table |
| WAC 296-17 (chapter) | Worker classification + employer reporting | RCW 51.16 | `bii-regulations-wac` | Premium-rate framework |
| WAC 296-20 (chapter) | Medical aid rules | RCW 51.04, 51.32, 51.36 | `bii-regulations-wac` | Treating-provider / MMI / impairment-rating procedures |
| WAC 296-22 (chapter) | Health care services — fees | RCW 51.04, 51.36 | `bii-regulations-wac` | Fee schedule implementation |

### Title 162 — Human Rights Commission (WLAD enforcement)

| Citation | Subject | Authorizing RCW (expected) | Topical tag | Why |
|---|---|---|---|---|
| WAC 162-16 (chapter) | Employment discrimination — general | RCW 49.60.120(3) | `wlad-enforcement-wac`, `employment-discrimination` | **Cross-jurisdiction Test C anchor** — must rank alongside RCW 49.60.180 + 42 U.S.C. § 12112 |
| WAC 162-22 (chapter) | Employment — handicapped persons | RCW 49.60.120(3), 49.60.180 | `wlad-enforcement-wac`, `disability-discrimination` | Direct disability-discrimination implementation |
| WAC 162-26 (chapter) | Public accommodations — disability discrimination | RCW 49.60.120(3) | `wlad-enforcement-wac` | Public-accommodation parallel to ADA Title III |
| WAC 162-30 (chapter) | Sex discrimination | RCW 49.60.120(3) | `wlad-enforcement-wac` | |
| WAC 162-32 (chapter) | Sexual orientation and gender identity | RCW 49.60.120(3) | `wlad-enforcement-wac` | |
| WAC 162-08 (chapter) | Practice and procedure | RCW 49.60.120(3) | `wlad-enforcement-wac`, `wac-procedure` | HRC complaint processing — relevant to WSHRC pre-suit administrative track |

### Title 110 — Department of Children, Youth & Families (Sara Murray axis)

| Citation | Subject | Authorizing RCW (expected) | Topical tag | Why |
|---|---|---|---|---|
| WAC 110-30 (chapter) | Child Protective Services | RCW 26.44 | `dcyf-wac`, `child-abuse-reporting` | CPS investigation procedures; central to Sara Murray Branch B work |
| WAC 110-50 (chapter) | Child Welfare | RCW 74.13, 26.44 | `dcyf-wac` | Foster-care framework |
| WAC 110-148 (chapter) | Licensing requirements for child foster homes | RCW 74.15 | `dcyf-wac` | |
| WAC 110-149 (chapter) | Licensing requirements for kinship homes | RCW 74.15 | `dcyf-wac` | |

### Title 388 — DSHS

| Citation | Subject | Authorizing RCW (expected) | Topical tag | Why |
|---|---|---|---|---|
| WAC 388-71 (chapter) | Long-Term Care services (per memory 4/22 vulnerable-adult work) | RCW 74.34, 74.39A | `dshs-wac` | Vulnerable-adult civil cause of action analysis hook |
| Chapters in 800s range (DDA / mental health / vocational rehab) | RCW 71A, 71.05 | `dshs-wac` | Collateral to ADA Title II / OLC integration mandate |

DSHS Title 388 is large (88 chapters) and most content is service-administration procedure rather than litigation-relevant doctrine. Recommend ingesting for completeness; pre-load specific anchors only for DDA/MH/long-term-care chapters.

### Title 182 — Health Care Authority (Title 222→182 correction)

| Citation | Subject | Authorizing RCW (expected) | Topical tag | Why |
|---|---|---|---|---|
| WAC 182 (chapter list to confirm in Phase 4) | HCA programs — Apple Health, employee benefits, behavioral health | RCW 41.05, 71.24, 74.09 | `hca-wac` | Cross-cutting health-care administrative framework |

### Title 246 — Department of Health (RCW 18 implementation)

| Citation | Subject | Authorizing RCW (expected) | Topical tag | Why |
|---|---|---|---|---|
| WAC 246-808 area (Naturopathy, etc. medical practice) | RCW 18.36A | `health-wac` | Prisma Wellness collateral |
| WAC 246-852 area (Pharmacy) | RCW 18.64 | `health-wac` | |
| WAC 246-300 area (Vital records) | RCW 70.58A | `health-wac` | Birth/death record procedure — collateral to Sara Murray work |

### Title 44 — Code Reviser's Office + agency rulemaking

| Citation | Subject | Authorizing RCW (expected) | Topical tag | Why |
|---|---|---|---|---|
| (chapter count not yet sampled) | APA implementation; rule-making procedures | RCW 34.05 (Administrative Procedure Act) | `wa-apa-implementation` | Procedural framework for any administrative challenge |

### Title 357 — State Human Resources / civil service

| Citation | Subject | Authorizing RCW (expected) | Topical tag | Why |
|---|---|---|---|---|
| WAC 357 (entire title — 12 chapters, full inclusion cheap) | Civil service rules — director's review, classification, compensation | RCW 41.06 | `wa-public-employment-wac` | IAFF v SSCFR collateral |

---

## 5. Authority-chain canary anchors (Phase 5 §6 equivalent)

Cross-jurisdiction queries that must surface the WAC + RCW pair when called against the post-Phase-4 corpus. Extends `cross_jurisdiction_design.md` Tests A/B/C from RCW Phase 0 to add the WAC layer:

**Test C (NEW for WAC) — three-jurisdiction discrimination:**

```python
rag_query("disability discrimination state and federal employment with implementing regulations",
          domain='legal', top_k=15)
```

PASS criterion: top-15 results MUST include all three of:
- 42 U.S.C. § 12112 (federal ADA Title I)
- RCW 49.60.180 (WLAD employment)
- WAC 162-22-XXX or WAC 162-16-XXX (HRC enforcement)

Diagnostic if WAC layer fails: most likely cause is missing topical tags (`wlad-enforcement-wac` or `disability-discrimination`). Mitigation: re-tag and re-rank.

**Test D (NEW for WAC) — workers' comp procedure:**

```python
rag_query("workers compensation appeal procedure state insurance fund claim",
          domain='legal', top_k=10)
```

PASS criterion: top-10 MUST include
- RCW 51.52.060 / .115 (BIIA appeal procedure)
- WAC 296-15-XXX (self-insurer claim procedures) OR WAC 296-22-XXX (medical fee schedule)

**Test E (NEW for WAC) — authority-chain reverse query (SQL canary):**

```sql
SELECT COUNT(*) AS wlad_implementation_section_count
FROM wac.authority_chain
WHERE authorizing_rcw LIKE 'RCW 49.60%';
```

PASS criterion: count > 50 (Title 162 has 14 chapters; even at 5 sections/chapter avg that's 70+; clearing 50 easily).

If count < 50: parser is dropping authority brackets or routing them to `wac.cross_references` instead of `wac.authority_chain`. Halt and audit.

**Test F (NEW for WAC) — child-welfare authority chain:**

```sql
SELECT COUNT(*) FROM wac.authority_chain WHERE authorizing_rcw LIKE 'RCW 26.44%';
SELECT COUNT(*) FROM wac.authority_chain WHERE authorizing_rcw LIKE 'RCW 74.13%';
```

PASS criterion: each count > 10.

**Test G (NEW for WAC) — overall resolution rate:**

```sql
SELECT
  COUNT(*) FILTER (WHERE NOT is_chapter_level AND NOT is_session_law)                  AS total,
  COUNT(*) FILTER (WHERE NOT is_chapter_level AND NOT is_session_law AND rcw_section_id IS NOT NULL) AS resolved
FROM wac.authority_chain;
```

PASS criterion: `resolved / total > 0.70`. Below 70% halts Phase 5 (per `authority_chain_design.md` §5).

---

## 6. Cross-jurisdiction parallels — three-jurisdiction extension

Adds a third column to RCW Phase 0 §3 cross-jurisdiction table:

| Federal anchor | WA RCW anchor | WAC implementation anchor | Doctrinal triple |
|---|---|---|---|
| 5 U.S.C. § 552 (FOIA) | RCW 42.56.030 / .520 / .550 (PRA) | (each agency's own PRA WAC; centralized in WAC 44) | Records-access frameworks |
| 42 U.S.C. § 12112 (ADA Title I employment) | RCW 49.60.180 (WLAD employment) | WAC 162-22-XXX (HRC handicapped-persons employment) | Disability-discrimination employment |
| 42 U.S.C. § 12132 (ADA Title II services/programs) | (no clean RCW parallel) | WAC 388-XXX (DSHS service delivery, partial) | Public-services disability access |
| 42 U.S.C. § 1983 | RCW 4.96.010 (state torts) + state common law | (none — § 1983 enforcement is federal) | Civil-rights remedy |
| 18 U.S.C. § 1961 (federal RICO) | RCW 9A.82.010(4) (state criminal-profiteering) | (none — criminal predicates) | Predicate-act analysis |
| Workers' Compensation (federal LHWCA, FECA) | RCW 51 (state industrial insurance) | WAC 296-XX (L&I implementation) | Workers' comp framework |
| 45 C.F.R. § 1340 (federal CAPTA child-abuse procedures) | RCW 26.44 (WA CAPTA implementation) | WAC 110-30 (DCYF CPS procedures) | Child-abuse mandatory-reporting + investigation |

Phase 5 cross-jurisdiction canary covers Tests C (employment disability), D (workers' comp), E/F/G (authority chain SQL). Remaining triples become organic test material for Phase 5 spot-checks.

---

## 7. Items the operator may want to add (verify before Phase 4)

Surfaced from Athena memory + post-2026-04-24 privilege filter and Sprint A context. Not on operator's pre-loaded list — confirm before committing as mandatory canaries.

1. **WAC 137 (Department of Corrections)** — cross-cuts with RCW 9.94A (sentencing) and post-conviction relief framing. No active criminal-procedure case but corpus should be reachable.
2. **WAC 388-71 (DSHS Long-Term Care)** — RCW 74.34 vulnerable-adult civil cause of action analysis hook (memory 4/22). Pre-loaded above.
3. **WAC 458-20 (Excise Taxes — Tax Topics)** — relevance depends on operator's REET-related work.
4. **WAC 192-330 (Employment Security — unemployment insurance)** — collateral to post-employment / unemployment-claim work.
5. **WAC 484 (small agencies' own PRA WAC)** — many small agencies. Recommend not pre-loading individually; rely on WAC 44 + per-agency dispositions.
6. **WAC 230 (Gambling Commission)** — irrelevant to active docket; long-tail.
7. **WAC 51 (State Building Code Council)** — irrelevant to active docket; long-tail.

These are flagged for operator consideration, not added to §4 mandatory canaries without authorization.

---

## 8. Out-of-scope confirmation

GR / CR / RAP / ER and other WA court rules are confirmed Phase 8 backlog. Court rules themselves are not WAC and not in this corpus.

WSR (Washington State Register) is not in scope — WSR citations stored as strings in `wac.section_history` and `wac.sections.wsr_register_number` but WSR rule-making text is not fetched.

Pre-2004 WAC archive vintages NOT included (current corpus only — the 2025 archive serves as fidelity dictionary, not a separate ingest).

---

## 9. Open questions for operator confirmation

1. **§1 anomaly resolution**: confirm Title 222 → Title 182 swap in priority slot 6.
2. **Title 388 ingestion approach**: full ingest with selective topical tagging (DDA/MH/long-term-care chapters)? Or full ingest with universal `dshs-wac` tag and skip topical refinement?
3. **Title 44 priority slot**: chapter count not yet sampled. If small (likely), include in priority block. If large, defer to long tail. **Sample in Phase 0 final pass or Phase 2 acquisition?**
4. **Title 208 (Financial Institutions)**: FTCU litigation collateral. Recommend long-tail. **Confirm.**
5. **Title 284 (Insurance Commissioner)**: Unum litigation collateral. Recommend long-tail. **Confirm.**
6. **Topical tag pre-creation**: should Phase 1 schema-apply create all `legal-framework` topical tags now (`wlad-enforcement-wac`, `bii-regulations-wac`, etc.) or defer to Phase 4 ingest? RCW Phase 1 pre-created 17 new legal-framework tags as part of schema apply — recommend same pattern.
7. **Phase 4 wall-time revision**: per `wac_source_findings.md` §6.4, Phase 4 Stage 2 is ~20 hours with HF stable. Priority core (slots 1–4) ~3–4 hours. **Confirm cadence assumption.**

End of WAC litigation-priority addendum.
