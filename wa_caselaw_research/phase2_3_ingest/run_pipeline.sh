#!/usr/bin/env bash
# =============================================================================
# Phase 2/3 streaming ingest pipeline — CL bulk-data → case_law.* (WA-filtered)
# =============================================================================
# Architecture: §K #43 — external streaming filter, no full-corpus on-disk
# staging. Pipeline order is dependency-driven (filter sets cascade).
#
# Each step pipes bzcat → stream_filter.py → psql \COPY.
# Filter id-sets are extracted via SELECT after each prerequisite step
# completes.
# =============================================================================
set -euo pipefail

CACHE=/var/cache/wa_caselaw
PYFILTER=/opt/wdws/wa_caselaw_research/phase2_3_ingest/stream_filter.py
WORK=/opt/wdws/wa_caselaw_research/phase2_3_ingest/_work
mkdir -p "$WORK"

# Load DATABASE_URL (only — full .env contains values with special chars)
export DATABASE_URL=$(grep -E '^DATABASE_URL=' /opt/wdws/.env | head -1 | sed 's/^DATABASE_URL=//')
[[ -z "${DATABASE_URL:-}" ]] && { echo "DATABASE_URL not loaded from /opt/wdws/.env"; exit 1; }

PSQL="psql ${DATABASE_URL} -v ON_ERROR_STOP=1 --quiet"

log()  { echo "[$(date +%H:%M:%S)] $*" >&2; }
phase() { echo; echo "==================================================================="; echo "  $*"; echo "==================================================================="; }

# -----------------------------------------------------------------------------
phase "Step 1: courts (full pass-through, all CL courts)"
log "loading all CL courts..."
bzcat "$CACHE/courts-2026-03-31.csv.bz2" | python3 "$PYFILTER" courts | \
    $PSQL -c "\\COPY case_law.courts(id,short_name,full_name,citation_string,jurisdiction,in_use,position,url,start_date,end_date,notes,parent_court_id,date_modified) FROM STDIN WITH (FORMAT csv, HEADER)"
COURTS_N=$($PSQL -t -A -c "SELECT COUNT(*) FROM case_law.courts;")
log "case_law.courts: $COURTS_N rows"

# -----------------------------------------------------------------------------
phase "Step 2: dockets (filter court_id IN WA set)"
bzcat "$CACHE/dockets-2026-03-31.csv.bz2" | python3 "$PYFILTER" dockets | \
    $PSQL -c "\\COPY case_law.dockets(id,court_id,case_name,case_name_short,case_name_full,docket_number,slug,date_filed,date_terminated,date_argued,nature_of_suit,cause,appeal_from_id,appeal_from_str,source,blocked,date_blocked,date_created,date_modified) FROM STDIN WITH (FORMAT csv, HEADER)"
DOCKETS_N=$($PSQL -t -A -c "SELECT COUNT(*) FROM case_law.dockets;")
log "case_law.dockets: $DOCKETS_N rows"

# Extract WA docket ids
$PSQL -t -A -c "SELECT id FROM case_law.dockets ORDER BY id" > "$WORK/wa_docket_ids.txt"
WA_DOCKETS_N=$(wc -l < "$WORK/wa_docket_ids.txt")
log "wa_docket_ids set: $WA_DOCKETS_N"

# -----------------------------------------------------------------------------
phase "Step 3: opinion-clusters (filter docket_id IN wa_docket_ids)"
bzcat "$CACHE/opinion-clusters-2026-03-31.csv.bz2" | \
    python3 "$PYFILTER" clusters --filter-file "$WORK/wa_docket_ids.txt" | \
    $PSQL -c "\\COPY case_law.opinion_clusters(id,docket_id,case_name,case_name_short,case_name_full,date_filed,date_filed_is_approximate,slug,judges,nature_of_suit,posture,procedural_history,syllabus,headnotes,summary,disposition,history,citation_count,precedential_status,source,blocked,date_blocked,cross_reference,correction,other_dates,attorneys,arguments,headmatter,date_created,date_modified) FROM STDIN WITH (FORMAT csv, HEADER)"
CLUSTERS_N=$($PSQL -t -A -c "SELECT COUNT(*) FROM case_law.opinion_clusters;")
log "case_law.opinion_clusters: $CLUSTERS_N rows"

# Extract WA cluster ids
$PSQL -t -A -c "SELECT id FROM case_law.opinion_clusters ORDER BY id" > "$WORK/wa_cluster_ids.txt"
WA_CLUSTERS_N=$(wc -l < "$WORK/wa_cluster_ids.txt")
log "wa_cluster_ids set: $WA_CLUSTERS_N"

# -----------------------------------------------------------------------------
phase "Step 4: opinions (filter cluster_id IN wa_cluster_ids) — large stream, ~10M rows scanned"
bzcat "$CACHE/opinions-2026-03-31.csv.bz2" | \
    python3 "$PYFILTER" opinions --filter-file "$WORK/wa_cluster_ids.txt" | \
    $PSQL -c "\\COPY case_law.opinions(id,cluster_id,type,author_str,author_id,joined_by_str,per_curiam,page_count,sha1,extracted_by_ocr,download_url,local_path,plain_text,html_with_citations,xml_harvard,date_created,date_modified) FROM STDIN WITH (FORMAT csv, HEADER)"
OPINIONS_N=$($PSQL -t -A -c "SELECT COUNT(*) FROM case_law.opinions;")
log "case_law.opinions: $OPINIONS_N rows"

# Extract WA opinion ids
$PSQL -t -A -c "SELECT id FROM case_law.opinions ORDER BY id" > "$WORK/wa_opinion_ids.txt"
WA_OPINIONS_N=$(wc -l < "$WORK/wa_opinion_ids.txt")
log "wa_opinion_ids set: $WA_OPINIONS_N"

# -----------------------------------------------------------------------------
phase "Step 5: opinions_cited (citation-map; filter EITHER side IN wa_opinion_ids)"
bzcat "$CACHE/citation-map-2026-03-31.csv.bz2" | \
    python3 "$PYFILTER" opinions_cited --filter-file "$WORK/wa_opinion_ids.txt" | \
    $PSQL -c "\\COPY case_law.opinions_cited(id,citing_opinion_id,cited_opinion_id,depth,citing_in_corpus,cited_in_corpus) FROM STDIN WITH (FORMAT csv, HEADER)"
CITED_N=$($PSQL -t -A -c "SELECT COUNT(*) FROM case_law.opinions_cited;")
log "case_law.opinions_cited: $CITED_N rows"

# -----------------------------------------------------------------------------
phase "Step 6: citations (parallel reporter citations; filter cluster_id IN wa_cluster_ids)"
bzcat "$CACHE/citations-2026-03-31.csv.bz2" | \
    python3 "$PYFILTER" citations --filter-file "$WORK/wa_cluster_ids.txt" | \
    $PSQL -c "\\COPY case_law.citations(id,cluster_id,volume,reporter,page,type,date_created,date_modified) FROM STDIN WITH (FORMAT csv, HEADER)"
CITS_N=$($PSQL -t -A -c "SELECT COUNT(*) FROM case_law.citations;")
log "case_law.citations: $CITS_N rows"

# -----------------------------------------------------------------------------
phase "Step 7: parentheticals (filter described_opinion_id IN wa_opinion_ids)"
bzcat "$CACHE/parentheticals-2026-03-31.csv.bz2" | \
    python3 "$PYFILTER" parentheticals --filter-file "$WORK/wa_opinion_ids.txt" | \
    $PSQL -c "\\COPY case_law.parentheticals(id,described_opinion_id,describing_opinion_id,text,score,group_id) FROM STDIN WITH (FORMAT csv, HEADER)"
PAREN_N=$($PSQL -t -A -c "SELECT COUNT(*) FROM case_law.parentheticals;")
log "case_law.parentheticals: $PAREN_N rows"

# -----------------------------------------------------------------------------
phase "Pipeline complete. Summary:"
$PSQL -c "
  SELECT 'courts' AS t, COUNT(*) FROM case_law.courts
  UNION ALL SELECT 'dockets', COUNT(*) FROM case_law.dockets
  UNION ALL SELECT 'opinion_clusters', COUNT(*) FROM case_law.opinion_clusters
  UNION ALL SELECT 'opinions', COUNT(*) FROM case_law.opinions
  UNION ALL SELECT 'opinions_cited', COUNT(*) FROM case_law.opinions_cited
  UNION ALL SELECT 'citations', COUNT(*) FROM case_law.citations
  UNION ALL SELECT 'parentheticals', COUNT(*) FROM case_law.parentheticals
  ORDER BY 1;
"
