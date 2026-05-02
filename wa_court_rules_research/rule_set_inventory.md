# WA Court Rules — Rule Set Inventory

**Source:** `https://www.courts.wa.gov/court_rules/`
**Method:** Programmatic parse of cached `list_<group>_<set>.html` index pages (32 files in `cached_html/`).
**Date captured:** 2026-04-30
**Parser:** Regex over `<td nowrap>NUM</td><td>...<a href="..pdf">TITLE</a></td>` table rows. Source: `cached_html/inventory.json`.

## Inventory table

| group | set_code | rule_count | set_name | list URL |
|---|---|---:|---|---|
| aca | `SCAR` | 0 | Supplemental Court of Appeals Administrative Rule | https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=aca&set=SCAR |
| am | `ATJ` | 0 | Access to Justice | https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=am&set=ATJ |
| am | `BBP` | 0 | Bench-Bar-Press Committee Statement of Principles | https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=am&set=BBP |
| am | `BJA` | 0 | Advisory Case Processing Time Standards | https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=am&set=BJA |
| app | `RAP` | 180 | Rules of Appellate Procedure | https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=app&set=RAP |
| clj | `ARLJ` | 16 | Administrative Rules for Courts of Limited Jurisdiction | https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=clj&set=ARLJ |
| clj | `CRLJ` | 82 | Civil Rules for Courts of Limited Jurisdiction | https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=clj&set=CRLJ |
| clj | `CrRLJ` | 76 | Criminal Rules for Courts of Limited Jurisdiction | https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=clj&set=CrRLJ |
| clj | `IRLJ` | 26 | Infraction Rules for Courts of Limited Jurisdiction | https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=clj&set=IRLJ |
| clj | `RALJ` | 45 | Rules for Appeal of Decisions of Courts of Limited Jurisdiction | https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=clj&set=RALJ |
| ga | `APR` | 54 | Admission and Practice Rules | https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=ga&set=APR |
| ga | `BJAR` | 5 | Board for Judicial Administration Rules | https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=ga&set=BJAR |
| ga | `CJC` | 4 | Code of Judicial Conduct (Canon-level; rule-level nesting inside) | https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=ga&set=CJC |
| ga | `DRJ` | 14 | Discipline Rules for Judges | https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=ga&set=DRJ |
| ga | `ELC` | 133 | Rules for Enforcement of Lawyer Conduct | https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=ga&set=ELC |
| ga | `ELLLTC` | 1 | Rules for Enforcement of Limited License Legal Technician Conduct | https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=ga&set=ELLLTC |
| ga | `ELPOC` | 1 | Rules for Enforcement of Limited Practice Officer Conduct | https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=ga&set=ELPOC |
| ga | `ER` | 67 | Rules of Evidence | https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=ga&set=ER |
| ga | `GR` | 58 | General Rules | https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=ga&set=GR |
| ga | `JISCR` | 18 | Judicial Information System Committee Rules | https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=ga&set=JISCR |
| ga | `LLLT RPC` | 0 | Limited License Legal Technician Rules of Professional Conduct (no live link; LLLT program retired) | https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=ga&set=LLLT%20RPC |
| ga | `LPORPC` | 1 | Limited Practice Officer Rules of Professional Conduct | https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=ga&set=LPORPC |
| ga | `RPC` | 64 | Rules of Professional Conduct | https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=ga&set=RPC |
| sup | `AR` | 6 | Superior Court Administrative Rules | https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=sup&set=AR |
| sup | `CCR` | 34 | Superior Court Civil Commitment Rules | https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=sup&set=CCR |
| sup | `CR` | 96 | Superior Court Civil Rules | https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=sup&set=CR |
| sup | `CrR` | 65 | Superior Court Criminal Rules | https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=sup&set=CrR |
| sup | `GALR` | 7 | Superior Court Guardian ad Litem Rules | https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=sup&set=GALR |
| sup | `JuCR` | 82 | Juvenile Court Rules | https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=sup&set=JuCR |
| sup | `SCCAR` | 27 | Superior Court Civil Arbitration Rules | https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=sup&set=SCCAR |
| sup | `SPR` | 9 | Superior Court Special Proceedings | https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=sup&set=SPR |
| sup | `SPRC` | 7 | Superior Court Special Proceedings Rules — Criminal | https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=sup&set=SPRC |

**Total: 1,178 rules across 32 rule sets** (raw count from list-page row enumeration).

## Reconciled scope (operator-approved 2026-04-30)

**In-scope for this run (15 rule sets, ~960 rules):**

| code | rules | tier | notes |
|---|---:|---|---|
| GR | 58 | 1 | General Rules |
| RPC | 64 | 1 | Rules of Professional Conduct |
| CR | 96 | 1 | Superior Court Civil Rules |
| ER | 67 | 1 | Rules of Evidence |
| RAP | 180 | 1 | Rules of Appellate Procedure |
| CJC | 4 Canons → ~30 inner | 1 | Code of Judicial Conduct (per-RULE unpacking required) |
| GALR | 7 | 1 | Guardian ad Litem Rules (folded in 2026-04-30) |
| CrR | 65 | 2 | Superior Court Criminal Rules |
| CrRLJ | 76 | 2 | Criminal Rules for Limited Jurisdiction |
| CRLJ | 82 | 2 | Civil Rules for Limited Jurisdiction |
| JuCR | 82 | 2 | Juvenile Court Rules |
| SCCAR | 27 | 2 | Superior Court Civil Arbitration Rules (substituted for briefing's "MAR") |
| RALJ | 45 | 2 | Rules of Appeal of Decisions of Limited Jurisdiction Courts |
| APR | 54 | 2 | Admission and Practice Rules |
| ELC | 133 | 2 | Rules for Enforcement of Lawyer Conduct (folded in 2026-04-30) |

**Phase 8 deferred (17 rule sets, ~218 rules):** CCR, IRLJ, AR, ARLJ, BJAR, DRJ, JISCR, SPR, SPRC, LLLT RPC, ELLLTC, ELPOC, LPORPC, BBP, ATJ, BJA, SCAR.

**Phase-8-deferred sets are kept in `SET_CODES` regex alternation** for cross-reference recognition. When in-scope rule bodies cite a Phase-8-deferred set (e.g., a GR rule citing `CCR 1.5`), the cross-ref parser recognizes it as a court-rule citation; the resulting `wa_court_rules.cross_references` row will have `target_jurisdiction='state-wa-court-rule'` and `target_rule_id=NULL`. Phase 8 ingest of the deferred set later resolves the FK retroactively.

**Briefing reconciliation:** MAR → SCCAR (briefing was wrong); DR removed (does not exist standalone); RFE = ELC (folded in, not deferred).

### Group keys

| group | meaning |
|---|---|
| `sup` | Rules for Superior Court |
| `ga` | Rules of General Application |
| `clj` | Rules for Courts of Limited Jurisdiction |
| `app` | Rules of Appeal |
| `aca` | Appellate-court administrative |
| `am` | "Administrative materials" — Access to Justice principles, BBP, BJA standards |

## Caveats and known counting deviations

1. **Mono-PDF rule sets** — `ELLLTC` (1), `ELPOC` (1), `LPORPC` (1), `BJA` (≈1, single PDF "TIME standards"), `SCAR` (1), `ATJ` (≈2 PDFs), `BBP` (0 — landing-page text only) — these are not enumerated rule sets but consolidated single-document rules. The "1" in the table reflects a single PDF/document hyperlink, not an inflated rule count. For Phase 4 ingest planning, treat these as document-level units, not rule-level.
2. **`LLLT RPC` (0 rules)** — landing page exists with no body content or PDF links. The LLLT (Limited License Legal Technician) program was sunset by the WA Supreme Court in 2020 (program wound down 2022); the rules are likely deprecated. **Defer to operator: include as historical or skip.**
3. **`BBP` (0 rules)** — narrative text only, no rule structure. Not an "enumerated" rule set.
4. **`ATJ` (counted as 0 above)** — has 2 linked PDFs (Technology Principles + Order Approving). Body counts as documents, not numbered rules.
5. **CJC (4 entries)** — list-page reports 4 Canons. Each Canon PDF contains nested `RULE 1.1`, `RULE 1.2`, ... `RULE 4.5` sub-rules. Real "rule-level" CJC count is ~30+ once Canons are unpacked. **The 4 in the table is the index-level count; Phase 2 parser must descend into the PDF to enumerate the inner RULE entries.**
6. **Range entries** — 8 rows in the inventory are ranges (e.g., `RAP 18.18 - 18.20`, `CRLJ 27-37`, `CRLJ 65-67`, `CRLJ 69-70`, `CRLJ 78-80`, `CRLJ 86.04--99.04`, `JuCR 11.4-11.20`). These represent multiple reserved/repealed rule numbers in a single index row. If unpacked, they add ~50-70 placeholder rule numbers, but they are all `[RESERVED]` so ingest may legitimately collapse them.
7. **Rule numbers with `Stds` / `Appx` / `Standards` suffixes** — `GR 14_Appx`, `GR 25_Appx`, `GR 26_Stds`, `ARLJ 14_Stds`, `CrR 3.1 Stds`, `CrRLJ 3.1 Stds`, `CCR 2.1 Stds`, `JuCR 9.2 Stds`, `GR 26_Standards`. These are appendices/standards companions to a parent rule, not standalone rules.
8. **APR irregular IDs** — `APR Regs`, `APR LP_REGS`, `APR LP_DIS_REGS` are regulation-document handles, not numbered rules.

## Reconciliation against briefing list

| briefing code | present? | notes |
|---|---|---|
| GR | yes (58) | Includes target GR 31, 31.1, 33, 34 |
| CR | yes (96) | Superior Court Civil Rules |
| CrR | yes (65) | Includes RESCINDED rules (e.g., CrR 3.2A) |
| CrRLJ | yes (76) | |
| CRLJ | yes (82) | |
| ER | yes (67) | Includes ER 401/403/802 |
| RAP | yes (180) | Includes RAP 18.13A |
| RPC | yes (64) | Includes RPC 1.1, 1.5, 5.5 |
| CJC | yes (4 Canons → ~30 inner rules) | Structural anomaly — see caveat #5 |
| JuCR | yes (82) | |
| **MAR** | **NOT FOUND** | Mandatory Arbitration Rules superseded; modern equivalent is `SCCAR` (Superior Court Civil Arbitration Rules, 27 rules). |
| RALJ | yes (45) | |
| APR | yes (54) | Plus Regs/LP_REGS/LP_DIS_REGS variants |
| **DR** | **NOT FOUND** | "DR" (Divorce/Dissolution Rules) — modern WA equivalents are `CR` chapters and `CCR` (Civil Commitment), not a standalone DR set. |

## Rule sets present beyond the briefing list (16 sets)

These are **not listed** in the briefing's primary or "check for" lists:

| code | count | name | notes |
|---|---:|---|---|
| `AR` | 6 | Superior Court Administrative Rules | distinct from `ARLJ` |
| `ARLJ` | 16 | Admin Rules for Courts of Limited Jurisdiction | |
| `ATJ` | 0/2 | Access to Justice technology principles | mono-PDF |
| `BBP` | 0 | Bench-Bar-Press principles | narrative |
| `BJA` | 0/1 | Advisory Case Processing Time Standards | mono-PDF |
| `BJAR` | 5 | Board for Judicial Administration Rules | |
| `CCR` | 34 | Superior Court Civil Commitment Rules | high-volume |
| `DRJ` | 14 | Discipline Rules for Judges | |
| `ELC` | 133 | Rules for Enforcement of Lawyer Conduct | matches briefing's "RFE" check-for item |
| `ELLLTC` | 1 | Enforcement of LLLT Conduct | mono-PDF; LLLT-program-related |
| `ELPOC` | 1 | Enforcement of Limited Practice Officer Conduct | mono-PDF |
| `GALR` | 7 | Superior Court Guardian ad Litem Rules | |
| `IRLJ` | 26 | Infraction Rules for Courts of Limited Jurisdiction | traffic-court |
| `JISCR` | 18 | Judicial Information System Committee Rules | |
| `LLLT RPC` | 0 | LLLT RPC | landing page; no rule body |
| `LPORPC` | 1 | Limited Practice Officer RPC | mono-PDF |
| `SCAR` | 0/1 | Supplemental Court of Appeals Admin Rule | mono-PDF |
| `SCCAR` | 27 | Superior Court Civil Arbitration Rules | likely modern MAR replacement |
| `SPR` | 9 | Superior Court Special Proceedings | matches briefing's "Special Proceedings" check-for item |
| `SPRC` | 7 | Superior Court Special Proceedings — Criminal | |

Several of these match briefing "check for" items (`SPR`/`SPRC` = Special Proceedings; `ELC` = RFE; `SCAR` partially = Supreme Court Supp Rules), but the volume and breadth of additional sets exceeds what the briefing anticipated. **Halt criterion #2 triggered — see HALT.md.**
