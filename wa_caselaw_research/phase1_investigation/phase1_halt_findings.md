---
title: WA Appellate Case Law — Phase 1 HALT Findings
date: 2026-05-01
status: HALTED — operator scope reset required before any further work
---

# WA Appellate Case Law Ingest — Phase 1 HALT (operator surface)

**Project:** /opt/wdws/wa_caselaw_research/
**Phase:** 1 — Pre-flight investigation
**Disposition:** HALTED at Phase 1.1 (source URL verification) before substantive scope work
**Halt reason:** Source authorization in kickoff is structurally insufficient to satisfy
the kickoff's stated coverage goals. Multiple §K corroborations reinforced;
this is an operator-disposition decision point, not an agent decision point.

---

## Summary

The kickoff authorizes "courts.wa.gov exclusively" with date range "1950-present"
and 11 operationally-relevant authority spot-checks. Phase 1 source verification
established that:

1. The kickoff's URL pattern returns 404. The endpoint moved from
   `/opinions/?fa=opinions.allopinions&courtId=AXX` to
   `/opinions/index.cfm?fa=opinions.byYear&fileYear=YYYY&crtLevel=X&pubStatus=Y`.
2. The `courtId=A08/A01/A02/A03` parameter scheme is dead. The current scheme is
   `crtLevel=S` (Supreme) | `crtLevel=C` (Court of Appeals, all divisions). There is
   no per-Division (Div I / II / III) filter at the index level.
3. **courts.wa.gov hosts only slip opinions from 2013-02-22 onward** (Court of
   Appeals Published archive starts 2012). The site itself states this explicitly.
4. The historical archive (back to "territorial days") is at
   `www.lexisnexis.com/clients/wareports/` — the LexisNexis-hosted free-access
   mirror that the WA Supreme Court has officially designated as the public access
   point. It is an interactive Lexis Advance SPA with TOS clause "may not be used
   for commercial purposes" and is not amenable to bulk programmatic ingest.

Net effect on kickoff scope:

| Kickoff scope element | Source-verified state | Gap |
|---|---|---|
| Date range 1950-present | courts.wa.gov: 2013-present only | 63 of 76 calendar years out of reach via authorized source |
| WA Sup. Ct. ~30K-50K opinions | ~2,700-4,500 reachable (13 yrs × ~250-350/yr) | 6-19× shortfall |
| COA Div I/II/III ~130K-200K | ~13K-20K reachable (14 yrs × ~1K-1.5K/yr COA aggregate) | 6-15× shortfall; no division-level filter |
| Total ~160K-250K | ~15K-25K reachable | 6-17× shortfall |
| 11 operationally-relevant authorities | 4 reachable (Jafar 2013, Blazina 2015, Ramirez 2018, Koenig 2009 borderline) | **7 of 11 out of reach** |

The 11-authority spot-check exceeds the kickoff's own halt trigger
("Operationally-relevant authority spot-check (11 opinions) misses >2 → halt and
surface"). 7 misses including Marquis (1996, the WLAD foundation), Gunwall (1986,
the independent state doctrine anchor), Negron (1976), and Nast (1986).

Also: the parser stress-test target (Marquis, 130 Wn.2d 97, 1996) is not
acquirable from courts.wa.gov. The single-opinion structural-stress-test
discipline (§K #6) cannot proceed against the operator-named target without a
source change.

Per §K #36 (agent-scope boundary enforcement) and the kickoff's own halt
discipline, this is not an agent decision. Surfacing for operator disposition.

---

## Verification trace

### Independent baseline confirmation (case_law schema absent + 5 corpora at frozen baselines)

```
case_law_schema_exists                 0
rcw.sections                      58,529  (matches kickoff frozen baseline)
usc.sections                      61,133  (matches)
wac.sections                      73,180  (matches)
wa_court_rules.rules               1,286  (matches)
wa_constitution.sections             274  (matches)
wac.authority_chain @ RCW         83,752  (matches orthogonality witness)
core.documents (legal)           204,456  (matches)
db_size                            6,883 MB  (matches)
```

All 5 sibling-corpus baselines preserved. case_law schema clean and absent —
no namespace collision with prior agent state. No race condition.

### URL probe trace (kickoff URLs)

| URL | HTTP | Body fingerprint |
|---|---|---|
| `/opinions/?fa=opinions.allopinions&courtId=A08` | 200 | "Washington Courts Website Error" page (7,126 bytes — identical for all 4 courtIds) |
| `/opinions/?fa=opinions.allopinions&courtId=A01` | 200 | identical 7,126 bytes |
| `/opinions/?fa=opinions.allopinions&courtId=A02` | 200 | identical 7,126 bytes |
| `/opinions/?fa=opinions.allopinions&courtId=A03` | 200 | identical 7,126 bytes |

The error page text: "You have requested a page that does not exist or is no
longer available on our web site. ... documents such as court opinions are
removed from the site after a specified period of time. So it may be possible
that you followed a link to Washington Courts that is no longer valid."

### Discovered current URL pattern

| URL | Returns |
|---|---|
| `/opinions/index.cfm?fa=opinions.displayAll` | Year-indexed link list to `byYear` per crtLevel + pubStatus |
| `/opinions/index.cfm?fa=opinions.byYear&fileYear=YYYY&crtLevel=S&pubStatus=PUB` | Sup. Ct. published opinions by year |
| `/opinions/index.cfm?fa=opinions.byYear&fileYear=YYYY&crtLevel=C&pubStatus=PUB` | COA published opinions by year (no Division filter) |
| `/opinions/index.cfm?fa=opinions.byYear&fileYear=YYYY&crtLevel=C&pubStatus=PAR` | COA published-in-part by year |
| `/opinions/index.cfm?fa=opinions.byYear&fileYear=YYYY&crtLevel=C&pubStatus=UNP` | COA unpublished by year (out of scope per kickoff) |
| Sup. Ct. years available | 2013-2026 (14 years) |
| COA published years available | 2012-2026 (15 years) |
| COA published-in-part years | 2013-2026 (14 years) |

Per courts.wa.gov LexisOverview page (verbatim):

> "Courts.wa.gov website : This website has only slip opinions since
> February 2013."

> "The website provides 'no cost, no frills' access to the text of published
> appellate opinions. The opinions on the free-access website [LexisNexis-hosted
> mirror] are intended to mirror the text of the official opinions that are
> found in the Washington Reports and Washington Appellate Reports series of
> books"

> "A website has been created to provide free access to the full historical set
> of published opinions of the Supreme Court and the Court of Appeals, dating
> back through territorial days. The website is called the Washington State
> Judicial Opinions Website and is accessible at
> www.lexisnexis.com/clients/wareports/."

### LexisNexis mirror probe

`https://www.lexisnexis.com/clients/wareports/` returns a 3,696-byte
JavaScript-driven cookie-set + redirect into Lexis Advance's `/container?config=...`
endpoint at `advance.lexis.com`. Not amenable to programmatic bulk ingest;
designed for interactive lookup. TOS clause from courts.wa.gov LexisOverview:
"The opinions are provided free of charge but **may not be used for commercial
purposes**."

### Slip opinion vs official opinion — citation-form implication

courts.wa.gov serves SLIP opinions. Slip opinions:
- Are filed on the day of decision and are often not the court's final opinion
- Carry slip citation, not the official Wn.2d / Wn. App. citation
- Are "preceded with a cover sheet" and marked with red asterisk if no longer official

Operationally, citations the operator and opposing counsel use are the official
Wn.2d / Wn. App. forms (the kickoff's CHECK constraint
`chk_canonical_citation_format` requires these). Mapping slip opinions →
official citations requires an additional resolution step (the Lexis-hosted
mirror is the official-citation source per the WA Supreme Court's own
designation).

### MRSC alternative — explicitly disqualified by the WA Supreme Court

> "MRSC website : The website for the Municipal Research and Services Center
> has for many years provided free access to our published opinions, but these
> opinions vary significantly from the official published opinions, because
> (1) they have not been fully updated, (2) they have errors that have not
> been corrected, and (3) they vary over the years as to the quality of the
> source of the information from which they were taken. These opinions were
> never intended to be the official court opinions"

MRSC is not a defensible authoritative source per the issuing court's own
characterization.

---

## Decision tree (operator disposition required)

The kickoff is internally inconsistent: it authorizes a single source
(courts.wa.gov) that cannot satisfy the stated date range (1950-present) or the
operationally-relevant authority spot-checks (7 of 11 pre-date the source).
Surfaced as four resolution paths; agent does not select.

### Path A — Restrict scope to courts.wa.gov 2013-present

Honor the literal source authorization. Acquire ~15K-25K slip opinions
from 2013-02-22 to 2026-present. Drop scope of 1950-2012 entirely.

- **Pro:** No source-authorization change. Cleanest §K #36 boundary.
  Acquisition pipeline simple (year × crtLevel × pubStatus index pages).
- **Con:** Loses 17 years of operationally-foundational doctrine. WLAD
  (Marquis 1996), Gunwall (1986), most BIIA / Title 51 framework. The
  case-law corpus would be useful for current-case research only, not
  for doctrinal-authority chain work.
- **Citation gap:** slip opinions only (resolution to Wn.2d / Wn. App.
  official citation requires separate per-opinion lookup or acceptance of
  slip-citation-only canonical form, breaking the kickoff's
  `chk_canonical_citation_format` constraint pattern).
- **Spot-check coverage:** 4 of 11 reachable (Jafar 2013, Blazina 2015,
  Ramirez 2018, Koenig 2009 borderline; Loeffelholz 2012 just out).

### Path B — Authorize CourtListener / Free Law Project as the source

CourtListener (courtlistener.com/api/) is Free Law Project's open-license
case-law corpus. Has WA Supreme Court back through territorial-era and Court
of Appeals back through ~1969. Built on the Caselaw Access Project (Harvard
Law's CAP, complete through 2018, with subsequent updates). REST API,
rate-limited but free, Cambridge Open License (permissive — explicitly
allows commercial use including litigation-research operations).

- **Pro:** Full 1950-present coverage. Already-built citation graph (Lexis
  parallel citations included). Stable bulk API. Permissive license. CAP
  text fidelity is academic-grade. Same source can extend to federal case
  law in a future project per §K #36 boundary respect (kickoff explicitly
  scopes out federal — that boundary holds; just opens the architectural
  door).
- **Con:** Source-authorization change vs kickoff's "courts.wa.gov
  exclusively" — operator decision. CAP coverage ends ~2018-2020 for some
  jurisdictions; CourtListener fills 2018-present from courts.wa.gov +
  other public sources, so coverage is multi-source upstream (operationally
  abstracted by the API). Rate limits would extend acquisition wall time.
- **Citation form:** CourtListener stores both slip and official Wn.2d /
  Wn. App. citations in the same record. `chk_canonical_citation_format`
  constraint satisfied without per-opinion resolution work.
- **Spot-check coverage:** 11 of 11 expected reachable.

### Path C — Multi-source: courts.wa.gov for 2013-present + CourtListener (or CAP bulk) for 1950-2012 backfill

- **Pro:** Authoritative-current (slip opinions immediately on filing) +
  authoritative-archive (CAP/CL official Wn.2d/Wn. App. text). 11/11
  spot-check coverage.
- **Con:** Multi-source ingest pipeline complexity. De-duplication where
  2013-2018 overlap. Two TOS regimes. Two parser dispatch paths (slip
  HTML/PDF on courts.wa.gov vs JSON/text on CourtListener API).

### Path D — Operator declines all + project deferred or cancelled

Defensible disposition: case-law ingest is large, operationally-relevant
authority is already accessible to operator via county law library
Westlaw/Lexis access, and the immediate ACE corpus (statutes + regulations +
court rules + constitution) covers the primary-authority frameworks operator
needs for active-matter research. Case law as a sixth corpus may be deferred
until a workflow trigger (e.g., a specific brief that demands large-scale
parallel-citation traversal).

---

## §K corroborations registered this Phase 1

To be incorporated in `/opt/wdws/wa_caselaw_research/phase6_retrieval_guide_notes.md`
once Phase 1 disposition is set and Phase 2+ proceeds (or, if project deferred,
preserved here).

### §K #25 reinforcement — 6th instance this session: kickoff URL doubly stale

**Failure mode (refined):** kickoff URLs may be stale at *both* the endpoint
level (path moved) AND the parameter-scheme level (query parameter renamed or
semantically repurposed). A kickoff URL that returns HTTP 200 is not enough to
confirm currency — the response body must also be parsed for "this is the page
I expected" markers.

**Concrete instance:** kickoff URL
`https://www.courts.wa.gov/opinions/?fa=opinions.allopinions&courtId=A08`
returns HTTP 200 (not 404 or 301) but the body is the courts.wa.gov error page
("You have requested a page that does not exist"). All four courtId values
return the identical error page (same MD5 7126 bytes). Without body inspection
the agent could have proceeded to "URLs work, scaling fine" misjudgment.

**Audit channel:** treat HTTP 200 as necessary but not sufficient; require
body-fingerprint or expected-content-marker check on each verified URL. The
endpoint moved to `index.cfm?fa=opinions.displayAll` and the parameter scheme
moved from `courtId=AXX` to `crtLevel=S/C` + `pubStatus=PUB/PAR/UNP` +
`fileYear=YYYY`. Both changes are silent at the HTTP-status level.

**Generalizes:** prior 5 instances of #25 (USC 5A recodification, USC long-tail
title currency, RCW Phase 8 ghost-row title-existence, WA Constitution kickoff
URL drift, WA Constitution Article XX status note) were content-level (the
described entity moved/changed). This 6th instance is endpoint-level (the URL
pattern itself moved). Same audit channel applies: verify the premise, don't
infer from prior project memory.

### §K #29 reinforcement — 6th instance this session: coordinator scope-cardinality inflation

**Concrete instance:** kickoff projected ~160K-250K opinions across 4 courts
back to 1950 from courts.wa.gov. Verified scope reachable from courts.wa.gov:
~15K-25K opinions (2013-present aggregate). Inflation factor ~6-17× — the
largest seen so far this session (prior largest was WCR Task 4 at 55%
inflation). The inflation here is not from session-memory aggregate but from a
source-currency assumption that didn't survive Phase 1 verification.

**Generalizes:** #29 inflation isn't only about session-memory aggregates vs
per-source counts; it can also surface as the boundary case where the source
itself does not contain the full set the coordinator believed it did. Same
mitigation: per-source pre-flight verification before any halt-band lock.

### §K #37 sharpening — provable-existence axis applies to source-authorization, not just per-row

**Failure mode:** §K #37 (provable-existence vs convenient-completeness) was
canonized at the per-row materialization level (e.g., RCW Phase 8 ghost-row
materialization for repealed sections — only synthesize stubs where evidentiary
basis exists). This Phase 1 finding extends the same principle to
source-authorization scope: an authorized source must provably contain the
declared scope's content, not be assumed to do so via the source's name or
historical reputation.

**Concrete instance:** "courts.wa.gov" has the institutional reputation of "WA
appellate court source" but in fact serves only 2013-present slip opinions. The
kickoff's "1950-present scope from courts.wa.gov" is provable-existence-failed
at Phase 1 source verification. Honoring the literal authorization (Path A)
preserves §K #37 discipline; expanding to additional sources (Paths B/C)
requires explicit operator authorization and re-asserts provable-existence at
the new source's coverage boundary.

**Generalizes:** `source name × declared scope × actual source coverage` is a
trinary verification that must occur at Phase 1 pre-flight, not at Phase 2
acquisition wall-time discovery.

---

## Halt protocol applied

Per the kickoff's halt-and-investigate discipline:

| Halt trigger (kickoff) | Status | Notes |
|---|---|---|
| Per-court HTTP acquisition failure rate >5% | TRIGGERED | 100% acquisition-shape failure (URLs return error page, not opinion lists) |
| Marquis stress-test parser fails on any structural element | N/A — could not run | Marquis (1996) not acquirable from authorized source |
| Aggregate opinion count outside [120K, 280K] | TRIGGERED | Reachable count ~15K-25K, ~6-17× below floor |
| Authority spot-check misses >2 of 11 | TRIGGERED | 7 of 11 pre-2013, out of reach from authorized source |
| Schema-affecting decisions surface beyond planned bounds | NOT TRIGGERED | Schema state clean; no schema work attempted yet |
| Storage growth exceeds 60 GB during ingest | N/A | No ingest started |
| Other corpus integrity invariants change | NOT TRIGGERED | All 5 baselines + authority-chain witness preserved |

Three of the kickoff's own halt triggers fired at Phase 1 source verification.
Per the kickoff: "**halt and surface** for operator scope confirmation before
Phase 2."

---

## Recommended one-line operator surface

> Kickoff source (courts.wa.gov) stops at Feb-2013 — only 4 of 11 op-relevant
> authorities reachable, total scope ~15K-25K vs projected 160K-250K. Source
> reset required: Path A (limit to 2013-present), Path B (authorize
> CourtListener), Path C (both), or Path D (defer/cancel)?

---

## What was NOT touched

Per §K #36 boundary enforcement, no work occurred outside the agent's scope:

- No schema definition (case_law schema absent — confirmed clean)
- No FK-additive ALTER TABLE on rcw/wac/wcr/usc/wa_constitution
  cross_references
- No source acquisition beyond preflight HTTP probes (4 × root + 2 × overview pages =
  6 small fetches, all <30 KB each, all rate-limited)
- No /var/cache/wa_caselaw/ population beyond directory creation
- No core.documents writes
- No other-corpus reads beyond the baseline-verification SQL above

Project directory: `/opt/wdws/wa_caselaw_research/phase1_investigation/`
contains this halt-findings file only.

---

## Resume conditions

Phase 1 resumes when operator selects a path (A/B/C/D). If A: proceed with
Marquis-equivalent stress-test substituted (a 2013+ Sup. Ct. opinion of similar
structural complexity), revised scope projections, revised 11-authority list
restricted to 2013+ matters. If B or C: proceed with CourtListener API
familiarization + revised acquisition plan (HTTP API rate limits, JSON parser
in lieu of HTML/PDF parser, Cambridge Open License attribution requirements).
If D: agent stands down, this halt-findings file is the disposition record.

---

**Phase 1 status: HALTED at 1.1 source verification. Awaiting operator scope reset.**
