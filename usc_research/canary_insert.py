"""
Phase 1 acceptance canary — inserts 42 U.S.C. § 12101 as a single sample USC
section to prove the pipeline end-to-end (DDL + ingestion + embedding + retrieval)
before Phase 2 download begins.

Cleanup is the caller's responsibility (DELETE the core.documents row to CASCADE
the usc.sections row away). This script does NOT clean up.
"""
import sys, os, json, hashlib
sys.path.insert(0, "/opt/wdws")
sys.path.insert(0, "/opt/wdws/mcp-server")

import psycopg2
import psycopg2.extras
from embedding_service import embed_query_sync, EMBEDDING_DIMENSIONS

assert EMBEDDING_DIMENSIONS == 1024, f"BGE-M3 dim mismatch: {EMBEDDING_DIMENSIONS}"

DSN = "postgresql://wdws:NEL2233obs@127.0.0.1:5432/wdws"
RELEASE_POINT = "PL 119-84"
SOURCE_PATH   = f"usc:PL_119-84:t42:s12101"
SECTION_TITLE = "42 U.S.C. § 12101 — Findings and purpose"

# Verbatim § 12101 text fetched from uscode.house.gov view.xhtml
# (release point: PL 119-84; text identical to ADAAA 2008 — unchanged since)
FULL_TEXT_MD = """# § 12101. Findings and purpose

## (a) Findings

The Congress finds that—

(1) physical or mental disabilities in no way diminish a person's right to fully participate in all aspects of society, yet many people with physical or mental disabilities have been precluded from doing so because of discrimination; others who have a record of a disability or are regarded as having a disability also have been subjected to discrimination;

(2) historically, society has tended to isolate and segregate individuals with disabilities, and, despite some improvements, such forms of discrimination against individuals with disabilities continue to be a serious and pervasive social problem;

(3) discrimination against individuals with disabilities persists in such critical areas as employment, housing, public accommodations, education, transportation, communication, recreation, institutionalization, health services, voting, and access to public services;

(4) unlike individuals who have experienced discrimination on the basis of race, color, sex, national origin, religion, or age, individuals who have experienced discrimination on the basis of disability have often had no legal recourse to redress such discrimination;

(5) individuals with disabilities continually encounter various forms of discrimination, including outright intentional exclusion, the discriminatory effects of architectural, transportation, and communication barriers, overprotective rules and policies, failure to make modifications to existing facilities and practices, exclusionary qualification standards and criteria, segregation, and relegation to lesser services, programs, activities, benefits, jobs, or other opportunities;

(6) census data, national polls, and other studies have documented that people with disabilities, as a group, occupy an inferior status in our society, and are severely disadvantaged socially, vocationally, economically, and educationally;

(7) the Nation's proper goals regarding individuals with disabilities are to assure equality of opportunity, full participation, independent living, and economic self-sufficiency for such individuals; and

(8) the continuing existence of unfair and unnecessary discrimination and prejudice denies people with disabilities the opportunity to compete on an equal basis and to pursue those opportunities for which our free society is justifiably famous, and costs the United States billions of dollars in unnecessary expenses resulting from dependency and nonproductivity.

## (b) Purpose

It is the purpose of this chapter—

(1) to provide a clear and comprehensive national mandate for the elimination of discrimination against individuals with disabilities;

(2) to provide clear, strong, consistent, enforceable standards addressing discrimination against individuals with disabilities;

(3) to ensure that the Federal Government plays a central role in enforcing the standards established in this chapter on behalf of individuals with disabilities; and

(4) to invoke the sweep of congressional authority, including the power to enforce the fourteenth amendment and to regulate commerce, in order to address the major areas of discrimination faced day-to-day by people with disabilities.

(Pub. L. 101–336, §2, July 26, 1990, 104 Stat. 328; Pub. L. 110–325, §3, Sept. 25, 2008, 122 Stat. 3554)
"""

# Stub source XML — Phase 4 will replace with the real <section> from the OLRC zip.
SOURCE_XML_STUB = (
    '<section xmlns="http://xml.house.gov/schemas/uslm/1.0" '
    'identifier="/us/usc/t42/s12101" status="operational">'
    '<num value="12101">§ 12101.</num>'
    '<heading>Findings and purpose</heading>'
    '<!-- canary stub — full XML loaded in Phase 4 -->'
    '</section>'
)

METADATA = {
    "type": "usc_section",
    "title_number": 42,
    "title_name": "The Public Health and Welfare",
    "is_positive_law": False,
    "chapter_number": "126",
    "chapter_heading": "Equal Opportunity for Individuals with Disabilities",
    "subchapter": None,
    "section_number": "12101",
    "section_heading": "Findings and purpose",
    "canonical_citation": "42 U.S.C. § 12101",
    "alt_citations": ["42 USC 12101"],
    "release_point": RELEASE_POINT,
    "current_through": "2026-04-18",
    "is_repealed": False,
    "is_transferred": False,
    "is_omitted": False,
    "effective_date": "1990-07-26",
    "enacted_by_pl": "Pub. L. 101-336",
    "statutes_at_large_ref": "104 Stat. 328",
    "amendment_count": 1,    # ADAAA 2008
    "olrc_url": "https://uscode.house.gov/view.xhtml?req=granuleid:USC-prelim-title42-section12101",
    "ingested_at": "2026-04-28T00:00:00Z",
    "canary": True,           # marks this row as Phase-1 smoke-test only
}

CHUNK_TEXT = (
    "Title 42 → Chapter 126 → § 12101 — Findings and purpose\n\n" + FULL_TEXT_MD
)
content_hash = hashlib.sha256(FULL_TEXT_MD.encode("utf-8")).hexdigest()

# ── 1. Embed (BGE-M3 1024-dim float32) ─────────────────────────────────────
print(f"[canary] Embedding chunk ({len(CHUNK_TEXT)} chars) with BAAI/bge-m3 …")
emb = embed_query_sync(CHUNK_TEXT)
assert len(emb) == 1024, f"unexpected dim {len(emb)}"
print(f"[canary] Embedding ready: dim={len(emb)}, sample[0:4]={emb[0:4]}")

# pgvector accepts the ARRAY-style literal '[v1,v2,...]' for halfvec — go that way
# to keep the canonical $N::halfvec(1024) cast in SQL (no Python-side fp16 quantization).
emb_literal = "[" + ",".join(f"{x:.7f}" for x in emb) + "]"

# ── 2. Insert (one transaction) ───────────────────────────────────────────
conn = psycopg2.connect(DSN)
conn.autocommit = False
try:
    with conn.cursor() as cur:
        # 2a. usc.titles — Title 42 (idempotent)
        cur.execute("""
            INSERT INTO usc.titles
                (title_number, title_name, is_positive_law, has_appendix,
                 release_point, current_through, notes)
            VALUES (42, 'The Public Health and Welfare', FALSE, FALSE,
                    %s, '2026-04-18', 'Phase 1 canary seed; Phase 4 will overwrite')
            ON CONFLICT (title_number) DO NOTHING
        """, (RELEASE_POINT,))

        # 2b. usc.chapters — Chapter 126 (Equal Opportunity for Individuals with Disabilities)
        cur.execute("""
            INSERT INTO usc.chapters
                (title_number, chapter_number, chapter_heading, sort_order)
            VALUES (42, '126', 'Equal Opportunity for Individuals with Disabilities', 126)
            ON CONFLICT (title_number, chapter_number) DO NOTHING
            RETURNING id
        """)
        row = cur.fetchone()
        if row is None:
            cur.execute("SELECT id FROM usc.chapters WHERE title_number=42 AND chapter_number='126'")
            row = cur.fetchone()
        chapter_id = row[0]

        # 2c. core.documents — the document body (governance defaults applied)
        cur.execute("""
            INSERT INTO core.documents
                (domain, document_type, title, source_path, filename,
                 full_content, content_hash, metadata,
                 classification, confidentiality, legal_hold,
                 privilege, privilege_classified_at, privilege_classified_by)
            VALUES
                ('legal', 'statute', %s, %s, 't42-s12101.xml',
                 %s, %s, %s::jsonb,
                 'public', 'public', FALSE,
                 'none', NOW(), 'rule:statute_is_public')
            ON CONFLICT (source_path)
                WHERE document_type = 'statute'
                DO UPDATE SET
                    full_content = EXCLUDED.full_content,
                    metadata     = EXCLUDED.metadata,
                    updated_at   = NOW()
            RETURNING id
        """, (SECTION_TITLE, SOURCE_PATH, FULL_TEXT_MD,
              content_hash, json.dumps(METADATA)))
        document_id = cur.fetchone()[0]
        print(f"[canary] core.documents.id = {document_id}")

        # 2d. usc.sections
        cur.execute("""
            INSERT INTO usc.sections
                (document_id, title_number, chapter_id, section_number,
                 section_heading, canonical_citation,
                 effective_date, enacted_by_pl, statutes_at_large_ref,
                 full_text, source_xml, release_point)
            VALUES (%s, 42, %s, '12101',
                    'Findings and purpose', '42 U.S.C. § 12101',
                    '1990-07-26', 'Pub. L. 101-336', '104 Stat. 328',
                    %s, %s, %s)
            ON CONFLICT (canonical_citation, release_point) DO NOTHING
            RETURNING id
        """, (document_id, chapter_id, FULL_TEXT_MD, SOURCE_XML_STUB, RELEASE_POINT))
        row = cur.fetchone()
        if row is None:
            cur.execute("""
                SELECT id FROM usc.sections
                WHERE canonical_citation=%s AND release_point=%s
            """, ('42 U.S.C. § 12101', RELEASE_POINT))
            row = cur.fetchone()
        section_id = row[0]
        print(f"[canary] usc.sections.id  = {section_id}")

        # 2e. usc.section_history (one row per Pub. L. segment)
        cur.execute("DELETE FROM usc.section_history WHERE section_id = %s", (section_id,))
        cur.executemany("""
            INSERT INTO usc.section_history
                (section_id, pub_law, pub_law_date, statutes_ref, action_type,
                 action_summary, sort_order)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, [
            (section_id, 'Pub. L. 101-336', '1990-07-26', '104 Stat. 328',
             'enacted', 'ADA enacted', 0),
            (section_id, 'Pub. L. 110-325', '2008-09-25', '122 Stat. 3554',
             'amended', 'ADAAA — broadened definitions', 1),
        ])

        # 2f. core.document_chunks — single chunk with halfvec cast in SQL
        chunk_id = f"{document_id}:0"
        cur.execute("""
            INSERT INTO core.document_chunks
                (id, document_id, chunk_index, total_chunks, content,
                 embedding, embedding_model_id, token_count,
                 embedded_content, embedded_at)
            VALUES
                (%s, %s, 0, 1, %s,
                 %s::halfvec(1024), 2, %s,
                 %s, NOW())
            ON CONFLICT (id) DO UPDATE SET
                embedding = EXCLUDED.embedding,
                embedded_content = EXCLUDED.embedded_content,
                embedded_at = NOW()
        """, (chunk_id, document_id, CHUNK_TEXT,
              emb_literal, len(CHUNK_TEXT.split()),
              CHUNK_TEXT))

        # 2g. core.documents.total_chunks
        cur.execute("UPDATE core.documents SET total_chunks=1 WHERE id=%s", (document_id,))

        # 2h. Tag attachments — usc, federal-statute, title-42, ada, civil-rights,
        # due-process, equal-protection, qualified-immunity is NOT applied (off-topic)
        TAG_SLUGS = ['usc', 'federal-statute', 'title-42', 'ada',
                     'civil-rights', 'due-process', 'equal-protection']
        cur.execute("SELECT slug, id FROM core.tags WHERE slug = ANY(%s)", (TAG_SLUGS,))
        slug_to_id = dict(cur.fetchall())
        missing = set(TAG_SLUGS) - set(slug_to_id)
        if missing:
            raise RuntimeError(f"Missing required tags: {missing}")
        cur.executemany("""
            INSERT INTO core.document_tags (document_id, tag_id)
            VALUES (%s, %s) ON CONFLICT DO NOTHING
        """, [(document_id, slug_to_id[s]) for s in TAG_SLUGS])

    conn.commit()
    print(f"[canary] COMMIT — document_id={document_id}, section_id={section_id}, "
          f"tags_attached={len(TAG_SLUGS)}, chunk_id={chunk_id}")
    print(f"[canary] To clean up after acceptance: "
          f"DELETE FROM core.documents WHERE id = '{document_id}';")
finally:
    conn.close()
