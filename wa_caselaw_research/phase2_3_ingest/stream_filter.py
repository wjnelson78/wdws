#!/usr/bin/env python3
"""Streaming CSV filter for CourtListener bulk-data tables.

Reads a CL bulk CSV (HEADER + standard double-quote CSV) from stdin, applies
a per-table filter, projects to the target case_law.* schema's column subset
(as the SQL COPY consumer expects), and writes filtered CSV to stdout.

Designed to be the middle of a `bzcat | stream_filter.py | psql COPY FROM STDIN`
pipeline. Memory budget: O(filter set), no row-batching.

§K #43 architecture: external streaming filter prevents full-corpus on-disk
materialization in pg datadir. WA filter applied BEFORE COPY, so staging
tables hold only WA-scoped rows.

Usage:
    bzcat dockets.csv.bz2 | python3 stream_filter.py dockets | \\
      psql ... -c "\\COPY case_law.dockets(...) FROM STDIN WITH (FORMAT csv, HEADER)"

Filter modes:
    courts          — pass-through (small reference table; load all CL courts)
    dockets         — court_id IN ('wash','washctapp','washterr')
    clusters        — docket_id IN <wa_docket_ids> (loaded from --filter-file)
    opinions        — cluster_id IN <wa_cluster_ids> (--filter-file)
    opinions_cited  — citing OR cited opinion_id IN <wa_opinion_ids> (--filter-file)
                       + computes citing_in_corpus / cited_in_corpus flags
    citations       — cluster_id IN <wa_cluster_ids> (--filter-file)
    parentheticals  — described_opinion_id IN <wa_opinion_ids> (--filter-file)
"""
from __future__ import annotations

import argparse
import csv
import io
import sys

# ---------------------------------------------------------------------------
# CL CSV header column orders (from load-bulk-data-2026-03-31.sh)
# ---------------------------------------------------------------------------
CL_COURTS_HEADER = [
    "id","pacer_court_id","pacer_has_rss_feed","pacer_rss_entry_types",
    "date_last_pacer_contact","fjc_court_id","date_modified","in_use",
    "has_opinion_scraper","has_oral_argument_scraper","position",
    "citation_string","short_name","full_name","url","start_date","end_date",
    "jurisdiction","notes","parent_court_id",
]

CL_DOCKETS_HEADER = [
    "id","date_created","date_modified","source","appeal_from_str",
    "assigned_to_str","referred_to_str","panel_str","date_last_index",
    "date_cert_granted","date_cert_denied","date_argued","date_reargued",
    "date_reargument_denied","date_filed","date_terminated","date_last_filing",
    "case_name_short","case_name","case_name_full","slug","docket_number",
    "docket_number_core","pacer_case_id","cause","nature_of_suit","jury_demand",
    "jurisdiction_type","appellate_fee_status","appellate_case_type_information",
    "mdl_status","filepath_local","filepath_ia","filepath_ia_json",
    "ia_upload_failure_count","ia_needs_upload","ia_date_first_change",
    "view_count","date_blocked","blocked","appeal_from_id","assigned_to_id",
    "court_id","idb_data_id","originating_court_information_id","referred_to_id",
    "federal_dn_case_type","federal_dn_office_code",
    "federal_dn_judge_initials_assigned","federal_dn_judge_initials_referred",
    "federal_defendant_number","parent_docket_id","docket_number_raw",
    "docket_number_source",
]

CL_CLUSTERS_HEADER = [
    "id","date_created","date_modified","judges","date_filed",
    "date_filed_is_approximate","slug","case_name_short","case_name",
    "case_name_full","scdb_id","scdb_decision_direction","scdb_votes_majority",
    "scdb_votes_minority","source","procedural_history","attorneys",
    "nature_of_suit","posture","syllabus","headnotes","summary","disposition",
    "history","other_dates","cross_reference","correction","citation_count",
    "precedential_status","date_blocked","blocked","filepath_json_harvard",
    "filepath_pdf_harvard","docket_id","arguments","headmatter",
]

CL_OPINIONS_HEADER = [
    "id","date_created","date_modified","author_str","per_curiam","joined_by_str",
    "type","sha1","page_count","download_url","local_path","plain_text","html",
    "html_lawbox","html_columbia","html_anon_2020","xml_harvard","xml_scan",
    "html_with_citations","extracted_by_ocr","author_id","cluster_id",
]

CL_OPINIONS_CITED_HEADER = ["id","depth","cited_opinion_id","citing_opinion_id"]

CL_CITATIONS_HEADER = [
    "id","volume","reporter","page","type","cluster_id",
    "date_created","date_modified",
]

CL_PARENTHETICALS_HEADER = [
    "id","text","score","described_opinion_id","describing_opinion_id","group_id",
]

# ---------------------------------------------------------------------------
# Target case_law.* column orders for COPY (must match \COPY column lists)
# ---------------------------------------------------------------------------
TGT_COURTS = [
    "id","short_name","full_name","citation_string","jurisdiction","in_use",
    "position","url","start_date","end_date","notes","parent_court_id",
    "date_modified",
]

TGT_DOCKETS = [
    "id","court_id","case_name","case_name_short","case_name_full",
    "docket_number","slug","date_filed","date_terminated","date_argued",
    "nature_of_suit","cause","appeal_from_id","appeal_from_str","source",
    "blocked","date_blocked","date_created","date_modified",
]

TGT_CLUSTERS = [
    "id","docket_id","case_name","case_name_short","case_name_full",
    "date_filed","date_filed_is_approximate","slug","judges","nature_of_suit",
    "posture","procedural_history","syllabus","headnotes","summary",
    "disposition","history","citation_count","precedential_status","source",
    "blocked","date_blocked","cross_reference","correction","other_dates",
    "attorneys","arguments","headmatter","date_created","date_modified",
]

TGT_OPINIONS = [
    "id","cluster_id","type","author_str","author_id","joined_by_str",
    "per_curiam","page_count","sha1","extracted_by_ocr","download_url",
    "local_path","plain_text","html_with_citations","xml_harvard",
    "date_created","date_modified",
]

TGT_OPINIONS_CITED = [
    "id","citing_opinion_id","cited_opinion_id","depth",
    "citing_in_corpus","cited_in_corpus",
]

TGT_CITATIONS = [
    "id","cluster_id","volume","reporter","page","type",
    "date_created","date_modified",
]

TGT_PARENTHETICALS = [
    "id","described_opinion_id","describing_opinion_id","text","score","group_id",
]

# ---------------------------------------------------------------------------
WA_COURT_IDS = {"wash", "washctapp", "washterr"}


def load_int_set(path: str) -> set:
    """Load a set of integers from a file (one per line). Skips blanks."""
    s = set()
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                s.add(int(line))
    return s


def open_csv_streams():
    """Return (reader, writer) wired to stdin/stdout with utf8 + correct dialect.

    INPUT: CL bulk CSVs use MySQL-style escape: backslash escapes embedded
    quotes inside quoted fields (e.g. `\"`). Python's csv.reader requires
    escapechar='\\\\' to parse this correctly — without it, the reader treats
    `\\\"` as a closing quote and silently shifts subsequent column boundaries,
    eventually causing 'invalid input syntax' errors at COPY time when text
    lands in numeric columns.

    OUTPUT: csv.writer with quoting=QUOTE_MINIMAL emits standard CSV
    (doubled-quote escape, no backslash). Postgres COPY with FORMAT csv
    (default ESCAPE) accepts this — they match without needing to specify
    ESCAPE on the COPY command.
    """
    sys.stdin.reconfigure(encoding="utf-8", errors="replace")
    sys.stdout.reconfigure(encoding="utf-8", newline="")
    reader = csv.reader(sys.stdin, escapechar="\\", strict=False)
    writer = csv.writer(sys.stdout, quoting=csv.QUOTE_MINIMAL)
    return reader, writer


def project(row: list, header: list, target: list) -> list:
    """Reorder/subset a CL row to the target case_law.* column order."""
    idx = {col: i for i, col in enumerate(header)}
    return [row[idx[c]] for c in target]


def filter_courts(reader, writer):
    header = next(reader)
    if header != CL_COURTS_HEADER:
        sys.stderr.write(f"WARN: courts header mismatch. got={header}\n")
    writer.writerow(TGT_COURTS)
    for row in reader:
        writer.writerow(project(row, CL_COURTS_HEADER, TGT_COURTS))


def filter_dockets(reader, writer):
    header = next(reader)
    if header != CL_DOCKETS_HEADER:
        sys.stderr.write(f"WARN: dockets header mismatch len={len(header)} expected={len(CL_DOCKETS_HEADER)}\n")
    writer.writerow(TGT_DOCKETS)
    court_idx = CL_DOCKETS_HEADER.index("court_id")
    n_in, n_kept = 0, 0
    for row in reader:
        n_in += 1
        if row[court_idx] in WA_COURT_IDS:
            writer.writerow(project(row, CL_DOCKETS_HEADER, TGT_DOCKETS))
            n_kept += 1
    sys.stderr.write(f"dockets: scanned={n_in} kept={n_kept}\n")


def filter_clusters(reader, writer, wa_docket_ids: set):
    header = next(reader)
    if header != CL_CLUSTERS_HEADER:
        sys.stderr.write(f"WARN: clusters header mismatch len={len(header)} expected={len(CL_CLUSTERS_HEADER)}\n")
    writer.writerow(TGT_CLUSTERS)
    docket_idx = CL_CLUSTERS_HEADER.index("docket_id")
    n_in, n_kept = 0, 0
    for row in reader:
        n_in += 1
        try:
            if int(row[docket_idx]) in wa_docket_ids:
                writer.writerow(project(row, CL_CLUSTERS_HEADER, TGT_CLUSTERS))
                n_kept += 1
        except (ValueError, IndexError):
            continue
    sys.stderr.write(f"clusters: scanned={n_in} kept={n_kept}\n")


def filter_opinions(reader, writer, wa_cluster_ids: set):
    header = next(reader)
    if header != CL_OPINIONS_HEADER:
        sys.stderr.write(f"WARN: opinions header mismatch len={len(header)} expected={len(CL_OPINIONS_HEADER)}\n")
    writer.writerow(TGT_OPINIONS)
    cluster_idx = CL_OPINIONS_HEADER.index("cluster_id")
    n_in, n_kept = 0, 0
    for row in reader:
        n_in += 1
        if n_in % 500_000 == 0:
            sys.stderr.write(f"opinions progress: scanned={n_in:,} kept={n_kept:,}\n")
        try:
            if int(row[cluster_idx]) in wa_cluster_ids:
                writer.writerow(project(row, CL_OPINIONS_HEADER, TGT_OPINIONS))
                n_kept += 1
        except (ValueError, IndexError):
            continue
    sys.stderr.write(f"opinions: scanned={n_in:,} kept={n_kept:,}\n")


def filter_opinions_cited(reader, writer, wa_opinion_ids: set):
    header = next(reader)
    if header != CL_OPINIONS_CITED_HEADER:
        sys.stderr.write(f"WARN: opinions_cited header mismatch got={header}\n")
    writer.writerow(TGT_OPINIONS_CITED)
    n_in, n_kept = 0, 0
    for row in reader:
        n_in += 1
        if n_in % 5_000_000 == 0:
            sys.stderr.write(f"cited progress: scanned={n_in:,} kept={n_kept:,}\n")
        try:
            citing = int(row[3])  # citing_opinion_id
            cited = int(row[2])   # cited_opinion_id
        except (ValueError, IndexError):
            continue
        citing_in = citing in wa_opinion_ids
        cited_in = cited in wa_opinion_ids
        if citing_in or cited_in:
            writer.writerow([row[0], citing, cited, row[1],
                             "t" if citing_in else "f",
                             "t" if cited_in else "f"])
            n_kept += 1
    sys.stderr.write(f"opinions_cited: scanned={n_in:,} kept={n_kept:,}\n")


def filter_citations(reader, writer, wa_cluster_ids: set):
    header = next(reader)
    if header != CL_CITATIONS_HEADER:
        sys.stderr.write(f"WARN: citations header mismatch got={header}\n")
    writer.writerow(TGT_CITATIONS)
    cluster_idx = CL_CITATIONS_HEADER.index("cluster_id")
    n_in, n_kept = 0, 0
    for row in reader:
        n_in += 1
        try:
            if int(row[cluster_idx]) in wa_cluster_ids:
                writer.writerow(project(row, CL_CITATIONS_HEADER, TGT_CITATIONS))
                n_kept += 1
        except (ValueError, IndexError):
            continue
    sys.stderr.write(f"citations: scanned={n_in:,} kept={n_kept:,}\n")


def filter_parentheticals(reader, writer, wa_opinion_ids: set):
    header = next(reader)
    if header != CL_PARENTHETICALS_HEADER:
        sys.stderr.write(f"WARN: parentheticals header mismatch got={header}\n")
    writer.writerow(TGT_PARENTHETICALS)
    described_idx = CL_PARENTHETICALS_HEADER.index("described_opinion_id")
    n_in, n_kept = 0, 0
    for row in reader:
        n_in += 1
        try:
            if int(row[described_idx]) in wa_opinion_ids:
                writer.writerow(project(row, CL_PARENTHETICALS_HEADER, TGT_PARENTHETICALS))
                n_kept += 1
        except (ValueError, IndexError):
            continue
    sys.stderr.write(f"parentheticals: scanned={n_in:,} kept={n_kept:,}\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=[
        "courts", "dockets", "clusters", "opinions",
        "opinions_cited", "citations", "parentheticals",
    ])
    ap.add_argument("--filter-file", help="path to id-set file (one int/line)")
    args = ap.parse_args()

    # Increase CSV field size limit — opinions text can be very large
    csv.field_size_limit(sys.maxsize)

    reader, writer = open_csv_streams()

    if args.mode == "courts":
        filter_courts(reader, writer)
    elif args.mode == "dockets":
        filter_dockets(reader, writer)
    elif args.mode == "clusters":
        s = load_int_set(args.filter_file)
        sys.stderr.write(f"loaded {len(s):,} wa_docket_ids\n")
        filter_clusters(reader, writer, s)
    elif args.mode == "opinions":
        s = load_int_set(args.filter_file)
        sys.stderr.write(f"loaded {len(s):,} wa_cluster_ids\n")
        filter_opinions(reader, writer, s)
    elif args.mode == "opinions_cited":
        s = load_int_set(args.filter_file)
        sys.stderr.write(f"loaded {len(s):,} wa_opinion_ids\n")
        filter_opinions_cited(reader, writer, s)
    elif args.mode == "citations":
        s = load_int_set(args.filter_file)
        sys.stderr.write(f"loaded {len(s):,} wa_cluster_ids\n")
        filter_citations(reader, writer, s)
    elif args.mode == "parentheticals":
        s = load_int_set(args.filter_file)
        sys.stderr.write(f"loaded {len(s):,} wa_opinion_ids\n")
        filter_parentheticals(reader, writer, s)


if __name__ == "__main__":
    main()
