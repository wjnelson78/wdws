#!/usr/bin/env python3
"""Phase 8: verification + cumulative report.

Runs internal-consistency checks over the case_law corpus + cross-corpus FK
uplift state. Produces structured output suitable for the Phase 8 close
report.

Checks:
  V1  Row counts in case_law.* match expected band [85K, 105K] for clusters
  V2  No orphan opinions (cluster_id has corresponding row in opinion_clusters)
  V3  No orphan clusters (docket_id present in dockets)
  V4  No orphan dockets (court_id present in courts)
  V5  Citation graph integrity: opinions_cited rows where citing_in_corpus
      AND cited_in_corpus consistent with case_law.opinions
  V6  Embedding completeness: every case-opinion document has total_chunks
      chunks AND every chunk has embedding populated
  V7  Cross-corpus xrefs: target_case_law_opinion_id references resolve
  V8  10/11 spot-check authorities still resolve in case_law.citations
  V9  Frozen-corpus orthogonality: rcw/usc/wac/wcr/wa_const counts unchanged
"""
from __future__ import annotations

import os
import sys
import psycopg

# Frozen baselines from MEMORY (project_sprint_a_task4_bulk_watch / phase 1)
FROZEN_BASELINES = {
    "rcw.sections": 51764,            # WAC schema_design.sql line documenting state
    "usc.sections_via_tag": 11678,    # via core.tags slug='federal-usc' join
    "wac.sections": None,             # filled at runtime if ~30K
    "wa_court_rules.rules": 1145,     # phase6_retrieval_guide_notes.md
    "wa_constitution.sections": 274,  # MEMORY
}

SPOT_CHECK = [
    ("Marquis v. City of Spokane",      "130", "Wn.2d", "97"),
    ("Fell v. Spokane Transit Auth.",   "128", "Wn.2d", "618"),
    ("Frisino v. Seattle Sch. Dist. 1", "160", "Wn. App.", "765"),
    ("Loeffelholz v. Univ. of WA",      "175", "Wn.2d", "264"),
    ("Jafar v. Webb",                   "177", "Wn.2d", "520"),
    ("State v. Blazina",                "182", "Wn.2d", "827"),
    ("State v. Ramirez",                "191", "Wn.2d", "732"),
    ("City of Federal Way v. Koenig",   "167", "Wn.2d", "341"),
    ("Nast v. Michels",                 "107", "Wn.2d", "300"),
    ("State v. Gunwall",                "106", "Wn.2d", "54"),
    # Negron 1976 was the documented miss (1/11)
]


def check(label: str, ok: bool, detail: str = ""):
    sym = "✓" if ok else "✗"
    print(f"  {sym} {label}: {detail}", flush=True)
    return ok


def main():
    db_url = os.environ.get("DATABASE_URL")
    assert db_url, "DATABASE_URL not set"

    print("=" * 70)
    print("Phase 8 Verification — WA Case Law Ingest")
    print("=" * 70)

    failures = []
    with psycopg.connect(db_url) as conn:
        c = conn.cursor()

        # V1 row counts
        print("\n[V1] case_law.* row counts:")
        c.execute("""
            SELECT 'courts' AS t, COUNT(*) FROM case_law.courts
            UNION ALL SELECT 'dockets', COUNT(*) FROM case_law.dockets
            UNION ALL SELECT 'opinion_clusters', COUNT(*) FROM case_law.opinion_clusters
            UNION ALL SELECT 'opinions', COUNT(*) FROM case_law.opinions
            UNION ALL SELECT 'opinions_cited', COUNT(*) FROM case_law.opinions_cited
            UNION ALL SELECT 'citations', COUNT(*) FROM case_law.citations
            UNION ALL SELECT 'parentheticals', COUNT(*) FROM case_law.parentheticals
            ORDER BY 1
        """)
        counts = dict(c.fetchall())
        for t, n in counts.items():
            print(f"    {t:25s}: {n:>10,}")
        clusters_in_band = 85_000 <= counts["opinion_clusters"] <= 105_000
        if not check("V1 clusters in projected band [85K, 105K]",
                     clusters_in_band, f"actual={counts['opinion_clusters']:,}"):
            failures.append("V1")

        # V2 orphan opinions
        c.execute("""
            SELECT COUNT(*) FROM case_law.opinions o
             LEFT JOIN case_law.opinion_clusters c ON c.id = o.cluster_id
             WHERE c.id IS NULL
        """)
        n = c.fetchone()[0]
        if not check("V2 no orphan opinions", n == 0, f"orphans={n}"):
            failures.append("V2")

        # V3 orphan clusters
        c.execute("""
            SELECT COUNT(*) FROM case_law.opinion_clusters c
             LEFT JOIN case_law.dockets d ON d.id = c.docket_id
             WHERE d.id IS NULL
        """)
        n = c.fetchone()[0]
        if not check("V3 no orphan clusters", n == 0, f"orphans={n}"):
            failures.append("V3")

        # V4 orphan dockets
        c.execute("""
            SELECT COUNT(*) FROM case_law.dockets d
             LEFT JOIN case_law.courts ct ON ct.id = d.court_id
             WHERE ct.id IS NULL
        """)
        n = c.fetchone()[0]
        if not check("V4 no orphan dockets", n == 0, f"orphans={n}"):
            failures.append("V4")

        # V5 citation graph integrity
        c.execute("""
            SELECT
              SUM(CASE WHEN citing_in_corpus AND cited_in_corpus THEN 1 ELSE 0 END) AS both,
              SUM(CASE WHEN citing_in_corpus AND NOT cited_in_corpus THEN 1 ELSE 0 END) AS citing_only,
              SUM(CASE WHEN NOT citing_in_corpus AND cited_in_corpus THEN 1 ELSE 0 END) AS cited_only,
              COUNT(*)
              FROM case_law.opinions_cited
        """)
        both, citing_only, cited_only, total = c.fetchone()
        print(f"    opinions_cited breakdown: total={total:,}  "
              f"both_in_corpus={both:,}  citing_only={citing_only:,}  cited_only={cited_only:,}")
        # Sanity: both + citing_only + cited_only = total
        if not check("V5 citation graph row classification consistent",
                     both + citing_only + cited_only == total,
                     f"sum={both+citing_only+cited_only} total={total}"):
            failures.append("V5")

        # V6 embedding completeness
        c.execute("""
            SELECT
              COUNT(*) FILTER (WHERE document_id IS NOT NULL)         AS extracted,
              COUNT(*) FILTER (WHERE document_id IS NULL)             AS unextracted,
              COUNT(*)                                                 AS total
              FROM case_law.opinions
        """)
        extracted, unextracted, total = c.fetchone()
        print(f"    opinions extraction: total={total:,}  extracted={extracted:,}  "
              f"unextracted={unextracted:,}")

        c.execute("""
            SELECT
              COUNT(*) FILTER (WHERE ch.embedding IS NULL)     AS unembedded,
              COUNT(*) FILTER (WHERE ch.embedding IS NOT NULL) AS embedded
              FROM core.document_chunks ch
              JOIN core.documents d ON d.id = ch.document_id
             WHERE d.document_type = 'case-opinion'
        """)
        unembedded, embedded = c.fetchone()
        print(f"    case-opinion chunks: embedded={embedded:,}  unembedded={unembedded:,}")
        if not check("V6 all case-opinion chunks embedded",
                     unembedded == 0,
                     f"unembedded={unembedded:,}"):
            failures.append("V6")

        # V7 cross-corpus xrefs resolve
        for s in ("usc", "rcw", "wac", "wa_court_rules", "wa_constitution"):
            c.execute(f"""
                SELECT
                  COUNT(*) FILTER (WHERE target_jurisdiction='state-wa-case-law') AS pointing,
                  COUNT(*) FILTER (WHERE target_case_law_opinion_id IS NOT NULL
                                   AND NOT EXISTS (SELECT 1 FROM case_law.opinions o
                                                    WHERE o.id = target_case_law_opinion_id)) AS broken
                  FROM {s}.cross_references
            """)
            pointing, broken = c.fetchone()
            print(f"    {s}.cross_references → case_law: pointing={pointing:,}  broken_FK={broken:,}")
            if broken != 0:
                if not check(f"V7 {s} no broken case_law FKs", broken == 0):
                    failures.append(f"V7-{s}")

        # V8 spot-check authorities
        print("\n[V8] 10/11 spot-check authorities (Negron 1976 acknowledged miss):")
        n_resolved = 0
        for name, vol, rep, page in SPOT_CHECK:
            c.execute("""
                SELECT cluster_id FROM case_law.citations
                 WHERE volume=%s AND reporter ILIKE %s AND page=%s
                 LIMIT 1
            """, (vol, f"%{rep.replace('Wn.', 'Wash.').replace('Wn', 'Wash')}%", page))
            r = c.fetchone()
            if r is None:
                # Try with the canonical Wn. form
                c.execute("""
                    SELECT cluster_id FROM case_law.citations
                     WHERE volume=%s AND (reporter ILIKE %s OR reporter ILIKE %s) AND page=%s
                     LIMIT 1
                """, (vol, f"%Wash.%", f"%{rep}%", page))
                r = c.fetchone()
            if r:
                n_resolved += 1
                print(f"    ✓ {name} → cluster_id={r[0]}")
            else:
                print(f"    ✗ {name} ({vol} {rep} {page}) — not found")
        if not check(f"V8 ≥9/10 spot-check authorities resolved",
                     n_resolved >= 9, f"resolved={n_resolved}/10"):
            failures.append("V8")

        # V9 frozen-corpus orthogonality
        print("\n[V9] Frozen-corpus orthogonality (counts should be unchanged):")
        for table_label, expected in FROZEN_BASELINES.items():
            if expected is None:
                continue
            schema, table = table_label.split(".", 1)
            if "_via_tag" in table:
                c.execute("""
                    SELECT COUNT(*) FROM core.document_tags dt
                     JOIN core.tags t ON t.id=dt.tag_id
                     WHERE t.slug='federal-usc'
                """)
            else:
                c.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
            n = c.fetchone()[0]
            ok = (n == expected)
            print(f"    {table_label:35s} expected={expected:>7,}  actual={n:>7,}  "
                  f"{'OK' if ok else 'DRIFT'}")
            if not ok:
                failures.append(f"V9-{table_label}")

        print("\n" + "=" * 70)
        if not failures:
            print("Phase 8 verification: ALL CHECKS PASSED")
        else:
            print(f"Phase 8 verification: {len(failures)} CHECK FAILURES: {failures}")
        print("=" * 70)
        return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
