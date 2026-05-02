# USC Full Ingestion — PAUSED 2026-04-28

## Status

Phase 4 paused at HARD STOP after Strategy B work block completion. Litigation-priority core is operational and verified clean. Pause is for priority pivot to RCW (Revised Code of Washington) full ingestion, not because of any USC corpus issue.

## What's Complete

**Titles ingested + verified clean:**

- Title 5 (Government Organization & Employees) — 1,221 sections
- Title 28 (Judiciary) — 846 sections + 230 court rules (FRCP/FRAP/FRE in 28a)
- Title 29 (Labor — including Rehab Act, ERISA) — 861 sections
- Title 42 (Public Health & Welfare — including ADA, §1983, Civil Rights, Medicare/Medicaid) — 8,520 sections

**Aggregate state at pause:**

- 11,678 statute documents
- 22,789 chunks (100% embedded with BGE-M3)
- 271,590 cross-references
- 26,218 section history rows + 58,250 section notes
- 230 court rules with rule_set discriminator (FRCP, FRAP, FRE)
- All twelve core litigation anchors content-verified against OLRC + Cornell LII + GovInfo

**Architecture/methodology assets in place (carry over to RCW):**

- Six bug-class fixes shipped (Stage-2 cursor invalidation; appendix-row metadata clobber; chapter-tag-map + full_text scope; LOWER() planner opacity; postgresql.conf session_preload_libraries=vector; cross-domain canary restructure)
- Three corruption bug classes closed at the parser level (empty-identifier embedded sections; phantom sections persisting in DB; duplicate-URI rule collision via Strategy B first-occurrence-wins)
- Phase 6 retrieval guide §G2, §H1, §H2, §I, §J, §K complete
- canary_lib v3 with three-pattern methodology (statutory phrasing scoped to statute, citation lookup via SQL exact match, popular-name retrieval via tag_filter + iterative_scan)
- documents_backfill_staging FK pre-delete pattern documented
- Known-rule dictionary methodology proven (caught fully-substituted corruption invisible to LIKE-audit)

## What Remains

**Titles NOT yet ingested:**

- Title 5 Appendix (5A) — Reorganization Plans + APA appendix material
- Title 11 (Bankruptcy) + 11a (FRBP) — 282 sections + 229 court rules
- Title 18 (Crimes & Criminal Procedure) + 18a (FRCrP) — 1,428 sections + 77 court rules
- Title 26 (Internal Revenue Code) — 2,276 sections (longest in USC)
- Title 38 (Veterans' Benefits) — 1,389 sections
- Plus ~40 smaller non-litigation-priority titles (Titles 1-4, 6-10, 12-17, 19-25, 27, 30-37, 39-41, 43-49, 51-52, 54)

**Estimated wall time to complete:**

- Litigation-priority remainder (5A → 18 → 18a → 11 → 11a): ~30-45 min execution + canaries
- Title 26 + Title 38: ~3-4 hours
- Long tail (~40 small titles): ~12-16 hours unattended overnight
- Total to corpus completion: roughly one full work session + one overnight run

## Resumption Procedure

When work resumes:

1. Verify postgresql.conf still has `session_preload_libraries='vector'` in `/etc/postgresql/17/main/conf.d/10-pgvector-session-preload.conf`
2. Verify parser at `/opt/wdws/usc_research/usc_parser.py` still has the three corruption-class fixes (identifier-guard, first-occurrence-wins on duplicate URIs, etc.)
3. Check OLRC for newer release point than PL 119-84 (April 18 2026); if newer, re-evaluate whether to upgrade litigation-priority titles before continuing long tail
4. Resume sequence: T5A → T18 → T18a → T11 → T11a → cumulative pause-checkpoint → T26 + T38 + long tail
5. canary_lib v3 methodology applies unchanged
6. HARD STOP / off-axis halt protocol applies unchanged

## Operator-Facing Status

**Litigation anchors that ARE retrievable right now:**

- 5 U.S.C. § 552 (FOIA), § 552a (Privacy Act), § 2302 (Whistleblower / Prohibited Personnel Practices)
- 28 U.S.C. § 1331 (federal question jurisdiction)
- 29 U.S.C. § 794 (Rehab Act § 504), § 1104 (ERISA fiduciary duties)
- 42 U.S.C. § 12101, § 12102, § 12132 (ADA Findings, Definition, Title II Discrimination), § 1983 (civil rights deprivation)
- Fed. R. Civ. P. 1-86 (all clean post-Strategy-B), with FRCP 17 verified for active FRCP 17(c)(2) work
- Fed. R. App. P. 1-48 (all clean)
- Fed. R. Evid. 101-1103 (all clean)

**Litigation anchors NOT yet retrievable (require remaining ingest):**

- 18 U.S.C. § 241 (Conspiracy against rights), § 242 (Deprivation of rights under color of law — criminal counterpart to § 1983), § 1001 (False statements)
- 11 U.S.C. § 362 (Automatic stay), § 523 (Exceptions to discharge), § 727 (Discharge)
- Fed. R. Crim. P. (all 77 rules)
- Fed. R. Bankr. P. (all 229 rules)
- All IRC sections (Title 26)
- All Veterans benefits sections (Title 38)

## Backlog Items (deferred from this run)

- Phase 7: Differential update path for new OLRC release points
- Phase 7: Proper OLRC content-fidelity round-trip methodology (JOIN sections + notes + history, body-only diff vs OLRC `<section>` element, not full `<main>`)
- Phase 7: `alt_citations` JSONB column on `usc.sections` for popular-name retrieval (e.g., "Section 504" → 29 U.S.C. § 794, "Section 1983" → 42 U.S.C. § 1983)
- Phase 7: Future ingest project for Social Security Supplemental Rules and Tax Court / COFC Rules from authoritative non-OLRC sources (8 rules deliberately excluded under Strategy B; not in current docket)
- Future: Production `rag_query` (`mcp_server_v2.py:5440-5630`) doesn't currently use `tag_filter`; if/when extended to do so, the `iterative_scan` + slug-equality patterns from this run apply

## Cross-Reference

See also: Phase 6 retrieval guide notes at `/opt/wdws/usc_research/phase6_retrieval_guide_notes.md` (sections G2, H1, H2, I, J, K) — covers all retrieval patterns and methodology lessons from this run.
