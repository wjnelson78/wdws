# Litigation Priority Addendum — WA Court Rules

**Date:** 2026-04-30
**Method:** Rule-existence verified by parsing list pages (`cached_html/list_<group>_<set>.html`); titles extracted from list-page anchor text or PDF first lines.

## Reconciled priority sequence (operator-approved 2026-04-30)

**Tier 1 — high-value civil-litigation core (Pause-Checkpoint A):**
```
GR → RPC → CR → ER → RAP → CJC → GALR
```

**Tier 2 — long tail (Pause-Checkpoint B):**
```
CrR → CrRLJ → CRLJ → JuCR → SCCAR → RALJ → APR → ELC
```

**Briefing reconciliation:**
- `MAR` removed from sequence (does not exist on courts.wa.gov; the modern equivalent is `SCCAR` and is folded in at Tier 2).
- `DR` removed entirely (does not exist as a standalone rule set on courts.wa.gov; lawyer-discipline procedural framework is `ELC`, also folded in at Tier 2).
- `GALR` (Guardian ad Litem Rules, 7 rules) added to Tier 1: load-bearing for GAL strategy in active Snohomish 26-2-00762-31 / 26-2-02783-31 matters.
- `ELC` (Rules for Enforcement of Lawyer Conduct, 133 rules) added to Tier 2: load-bearing for Carlisle's WSBA RPC consultation about Athena AI compliance and procedural framework for any lawyer-conduct enforcement question.

Within-tier ordering for Tier 2 doesn't materially affect outcome since all rule sets get ingested in this run. ELC at end of Tier 2 is acceptable — Carlisle-relevant canaries fire during ELC ingest before Pause-Checkpoint B closes.

## Phase 8 deferred (out of scope for this run)

These sets are **deferred to Phase 8** but **kept in the citation regex's `SET_CODES` alternation** for cross-reference recognition. When in-scope rule bodies cite a Phase-8-deferred set (e.g., a GR rule citing `CCR 1.5`), the cross-reference parser will recognize it as a court-rule citation; the resulting `wa_court_rules.cross_references` row will have `target_jurisdiction='state-wa-court-rule'` and `target_rule_id=NULL`. Phase 8 ingest of the deferred set later resolves the FK retroactively.

| code | reason |
|---|---|
| `CCR` (34) | No civil-commitment matters in active operator docket |
| `IRLJ` (26) | No traffic-court angle in active docket |
| `AR` (6) | Administrative |
| `ARLJ` (16) | Administrative |
| `BJAR` (5) | Governance |
| `DRJ` (14) | Judicial discipline (governance) |
| `JISCR` (18) | Information-system governance |
| `SPR` (9) | Special proceedings — narrow scope |
| `SPRC` (7) | Special proceedings — criminal narrow scope |
| `LLLT RPC` (0) | LLLT program sunset 2020; landing page only |
| `ELLLTC` (1) | LLLT enforcement; mono-PDF, defunct program |
| `ELPOC` (1) | Limited Practice Officer enforcement; mono-PDF |
| `LPORPC` (1) | Limited Practice Officer RPC; mono-PDF |
| `BBP` (0) | Bench-Bar-Press narrative; no rule structure |
| `ATJ` (0/2) | Access to Justice principles; mono-PDF |
| `BJA` (0/1) | Time standards; mono-PDF |
| `SCAR` (0/1) | Supplemental admin; mono-PDF |

## Anchor-citation harvest by priority rule set

### GR — General Rules (58 rules; index URL https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=ga&set=GR)

| rule | title | PDF |
|---|---|---|
| GR 31 | Access to Court Records | `pdf/GR/GA_GR_31_00_00.pdf` |
| GR 31.1 | Access to Administrative Records | `pdf/GR/GA_GR_31_01_00.pdf` |
| GR 33 | Requests for Accommodation by Persons with Disabilities | `pdf/GR/GA_GR_33_00_00.pdf` |
| GR 34 | Waiver of Court and Clerk's Fees and Charges in Civil Matters on the Basis of Indigency | `pdf/GR/GA_GR_34_00_00.pdf` |

All four operator-priority GR rules confirmed present and cached as samples. GR 8 is `[Reserved, Chapter 136, Laws 2002]`. GR 14 has both a base PDF and an `Appendix` companion (`GA_GR_14_Appendix.pdf`). GR 25 and GR 26 also have `_Appendix` / `_Standards` companions.

### RPC — Rules of Professional Conduct (64 rules; index URL https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=ga&set=RPC)

| rule | title | PDF |
|---|---|---|
| RPC 1.1 | Competence | `pdf/RPC/GA_RPC_01_01_00.pdf` |
| RPC 1.5 | Fees | `pdf/RPC/GA_RPC_01_05_00.pdf` |
| RPC 5.5 | Unauthorized Practice of Law; Multijurisdictional Practice of Law | `pdf/RPC/GA_RPC_05_05_00.pdf` |

All three operator-priority RPC rules confirmed and sampled. Comments use `[N]` numbered convention; "Additional Washington Comments" section appended on rules with WA-specific commentary; `[Washington revision]` inline tag on revised comments. RPC 1.0A and 1.0B are alpha-suffix variants (definitional rules); RPC 7.2/7.4/7.5 are `[RESERVED.]`.

### CR — Superior Court Civil Rules (96 rules; index URL https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=sup&set=CR)

| rule | title | PDF |
|---|---|---|
| CR 12 | Defenses and Objections | `pdf/CR/SUP_CR_12_00_00.pdf` |
| CR 26 | General Provisions Governing Discovery | `pdf/CR/SUP_CR_26_00_00.pdf` |
| CR 56 | Summary Judgment | `pdf/CR/SUP_CR_56_00_00.pdf` |

All three operator-priority CR rules confirmed and sampled. Note CR 45 in the list page has no PDF link (multi-line title incl. "Includes Form — Subpoena"); needs manual handling. CR 53 = "Masters (Reserved)". CR 61 = "Harmless Error Reserved". CR 72-76 = range-reserved.

### ER — Rules of Evidence (67 rules; index URL https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=ga&set=ER)

| rule | title | PDF |
|---|---|---|
| ER 401 | Definition of "Relevant Evidence" | `pdf/ER/GA_ER_04_01_00.pdf` |
| ER 403 | Exclusion of Relevant Evidence on Grounds of Prejudice, Confusion, or Waste of Time | `pdf/ER/GA_ER_04_03_00.pdf` |
| ER 802 | Hearsay Rule | `pdf/ER/GA_ER_08_02_00.pdf` (URL pattern verified) |

All three operator-priority ER rules confirmed. ER uses 3-digit citation form (`401`, `403`, `802`) but PDF filename uses Article+Rule split (`04_01_00` for `401`). ER 301/302 in titles include literal "(Reserved)"; ER 807 = "Child Victims or Witnesses (Reserved)"; ER 1102 = "Amendments (Reserved)".

### RAP — Rules of Appellate Procedure (180 rules; index URL https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=app&set=RAP)

| rule | title | PDF |
|---|---|---|
| RAP 18.13A | Accelerated Review of Juvenile Dependency Disposition Orders, Orders Terminating Parental Rights, and Orders Establishing or Changing Permanent Plans of Care | `pdf/RAP/APP_RAP_18_13A_00.pdf` |

Confirmed and sampled. RAP is the largest rule set (180 entries). Includes alpha-suffix rules (`12A`, `13.5A`, `15A`-`15D`, `18.13A`, `7.0A` on inspection of full rule list). Range entries `RAP 18.18 - 18.20` are reserved.

### CJC — Code of Judicial Conduct (4 Canon entries; ~30 inner rules)

Confirmed all 4 Canons present:

| Canon | title | PDF |
|---|---|---|
| Canon 1 | A Judge Shall Uphold and Promote the Independence, Integrity, and Impartiality of the Judiciary, and Shall Avoid Impropriety and the Appearance of Impropriety | `pdf/CJC/GA_CJC_canon1.pdf` |
| Canon 2 | A Judge Should Perform the Duties of Judicial Office Impartially, Competently, and Diligently | `pdf/CJC/GA_CJC_canon2.pdf` |
| Canon 3 | A Judge Shall Conduct the Judge's Personal and Extrajudicial Activities to Minimize the Risk of Conflict with the Obligations of Judicial Office | `pdf/CJC/GA_CJC_canon3.pdf` |
| Canon 4 | A Judge or Candidate for Judicial Office Shall Not Engage in Political or Campaign Activity that Is Inconsistent with the Independence, Integrity, or Impartiality of the Judiciary | `pdf/CJC/GA_CJC_canon4.pdf` |

**Structural anomaly:** Each Canon PDF contains nested `RULE 1.1`, `RULE 1.2`, ... `RULE 1.3` (and similarly for Canon 2, 3, 4). Sample Canon 1 inspection shows `RULE 1.1 — Compliance with the Law`, `RULE 1.2 — Promoting Confidence in the Judiciary`, `RULE 1.3 — Avoiding Abuse of the Prestige of Judicial Office` — total ~3 rules per Canon. Rule-level CJC cite (e.g., "CJC 2.11" for disqualification) requires unpacking the Canon PDFs in Phase 2. **Citation regex must support both `CJC Canon N` and `CJC N.M` forms.**

### GALR — Superior Court Guardian ad Litem Rules (7 rules; index URL https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=sup&set=GALR)

| rule | title |
|---|---|
| GALR 1 | Scope and Definitions |
| GALR 2 | General Responsibilities of Guardian ad Litem |
| GALR 3 | Roles and Responsibilities of Guardian ad Litem in Title 13 RCW Juvenile Cases |
| GALR 4 | Authority of Guardian ad Litem |
| GALR 5 | Appointments of Guardian ad Litem |
| GALR 6 | Limited Appointments |
| GALR 7 | Grievance Procedures |

All 7 GALR rules are load-bearing for GAL strategy work (Paul Gill / Faye Wong context in active Snohomish matters). Small set — full ingest is straightforward.

### ELC — Rules for Enforcement of Lawyer Conduct (133 rules; index URL https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=ga&set=ELC)

| rule | title | role |
|---|---|---|
| ELC 1.1 | Scope of Rules | foundational scope |
| ELC 1.2 | Disciplinary Authority | foundational authority |
| ELC 1.3 | Definitions | foundational |
| ELC 3.1 | Open Meetings and Public Disciplinary Information | public-info procedural anchor |
| ELC 5.1 | Grievants | grievance-procedure entry |
| ELC 7.1 | Interim Suspension for Conviction of A Crime | enforcement procedural |
| ELC 9.1 | Stipulations | stipulation procedural |
| ELC 10.1 | General Procedure | core enforcement procedure |
| ELC 13.1 | Sanctions and Remedies | sanctions framework |
| ELC 15.1 | Random Examination of Books and Records | trust-account enforcement |

Title-level distribution: 16 titles, ~3-16 rules per title. ELC 15.6 is `[RESERVED]` (singleton). The procedural rules in titles 5/7/9/10/13 are the load-bearing block for Carlisle's WSBA consultation about Athena AI compliance — they describe the framework Carlisle is consulting about. ELC parser must handle three-segment numbering only if it appears (none observed in 133-rule sample).

## Pause A / Pause B sequencing notes

After Tier 1 (`GR → RPC → CR → ER → RAP → CJC → GALR`) reach **Pause-Checkpoint A** for operator review. Pause-Checkpoint B closes after Tier 2 (`CrR → CrRLJ → CRLJ → JuCR → SCCAR → RALJ → APR → ELC`).

Within-tier ordering inside Tier 2 doesn't materially affect outcome since all sets get ingested in this run. ELC at end of Tier 2 means Carlisle-relevant canaries fire during ELC ingest before Pause-Checkpoint B closes.

## Spot-check anchor citations for Phase 4 ingest

Each of the following should resolve to a single ingested rule with the indicated title once Phase 4 runs:

1. `GR 31` → "Access to Court Records"
2. `GR 31.1` → "Access to Administrative Records"
3. `GR 33` → "Requests for Accommodation by Persons with Disabilities"
4. `GR 34` → "Waiver of Court and Clerk's Fees and Charges in Civil Matters on the Basis of Indigency" (Jafar v. Webb anchor)
5. `RPC 1.1` → "Competence" (Carlisle WSBA/Athena AI compliance anchor)
6. `RPC 1.5` → "Fees"
7. `RPC 5.5` → "Unauthorized Practice of Law; Multijurisdictional Practice of Law"
8. `CR 12` → "Defenses and Objections"
9. `CR 26` → "General Provisions Governing Discovery"
10. `CR 56` → "Summary Judgment"
11. `ER 401` → 'Definition of "Relevant Evidence"'
12. `ER 403` → "Exclusion of Relevant Evidence on Grounds of Prejudice, Confusion, or Waste of Time"
13. `ER 802` → "Hearsay Rule"
14. `RAP 18.13A` → "Accelerated Review of Juvenile Dependency Disposition Orders..."
15. `CJC Canon 1` → "A Judge Shall Uphold and Promote the Independence, Integrity, and Impartiality..."
16. `GALR 1` → "Scope and Definitions"
17. `GALR 5` → "Appointments of Guardian ad Litem" (active Snohomish matters anchor)
18. `ELC 1.1` → "Scope of Rules"
19. `ELC 5.1` → "Grievants" (Carlisle WSBA framework anchor)
20. `ELC 13.1` → "Sanctions and Remedies"

These should serve as Phase 4 acceptance gates: if all 20 retrieve correctly, the corpus is ingest-good for the operator's primary use cases.
