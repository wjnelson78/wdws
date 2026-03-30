#!/usr/bin/env python3
"""
Court Records Deduplication & Source Priority Cleanup
=====================================================
Identifies and removes duplicate court records, keeping the highest-priority
source copy. Also populates the document_availability table.

Source Priority (lower = better):
  1 = PACER (free)
  2 = CourtListener
  3 = CourtListener RECAP  (stored as courtlistener with raw_data source variant)
  4 = Google Scholar
  5 = Justia
  6 = Odyssey
  7 = Manual
  99 = Unknown

Logic:
  1. Within-source duplicates: same case_number + same source + same caseId/external_id
     → Keep the one with the most raw_data keys (richest data), delete others
  2. Cross-source duplicates: same case from multiple sources
     → Mark the highest-priority record as canonical (is_canonical=true)
     → Set canonical_id on lower-priority records pointing to the canonical
     → Create document_availability entries for all sources
  3. Note: "manual" records with docket_entries are NEVER deleted (they may
     have unique enrichment data)

Usage:
  python3 dedup_court_records.py [--dry-run] [--verbose]
"""

import argparse
import json
import sys
import uuid
from collections import defaultdict

import psycopg2
import psycopg2.extras

DB_DSN = "host=127.0.0.1 port=5432 dbname=osint-db user=root password=NEL2233obns"

SOURCE_PRIORITY = {
    'pacer': 1,
    'courtlistener': 2,
    'google_scholar': 4,
    'justia': 5,
    'odyssey': 6,
    'manual': 7,
}

# CourtListener sub-source variants get priority 2 or 3
COURTLISTENER_RECAP_SOURCES = {
    'CourtListener RECAP', 'courtlistener_recap', 'courtlistener-recap'
}


def get_effective_priority(record):
    """Return the effective priority for a record, considering CL sub-sources."""
    base = SOURCE_PRIORITY.get(record['source'], 99)
    if record['source'] == 'courtlistener':
        raw_source = (record.get('raw_data') or {}).get('source', '')
        if raw_source in COURTLISTENER_RECAP_SOURCES:
            return 3  # RECAP is slightly lower than direct CL
    return base


def richness_score(record):
    """Score how much useful data a record has (higher = richer)."""
    score = 0
    for col in ['case_name', 'court_name', 'filing_date', 'case_status',
                'plaintiff', 'defendant', 'judge', 'disposition', 'amount',
                'external_id']:
        if record.get(col):
            score += 1
    raw = record.get('raw_data') or {}
    score += len(raw)  # more keys = more data
    return score


def fetch_all_records(cur):
    """Fetch all court records with essential fields."""
    cur.execute("""
        SELECT cr.id, cr.case_number, cr.case_name, cr.court_name, cr.court_type,
               cr.source, cr.source_priority, cr.filing_date, cr.case_status,
               cr.plaintiff, cr.defendant, cr.judge, cr.disposition, cr.amount,
               cr.external_id, cr.raw_data, cr.is_canonical,
               EXISTS(SELECT 1 FROM court_docket_entries cde WHERE cde.court_record_id = cr.id) AS has_docket_entries,
               EXISTS(SELECT 1 FROM person_court_records pcr WHERE pcr.court_record_id = cr.id) AS has_person_links
        FROM court_records cr
        ORDER BY cr.case_number, cr.source_priority, cr.created_at
    """)
    columns = [desc[0] for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def group_by_case(records):
    """Group records by case_number."""
    groups = defaultdict(list)
    for r in records:
        key = r['case_number'] or str(r['id'])  # fallback to id if no case_number
        groups[key].append(r)
    return groups


def find_within_source_dupes(records):
    """
    Within a single case_number group, find duplicate records from the same source.
    For PACER: same source + same raw_data.caseId
    For CourtListener: same source + same external_id
    For others: same source + same filing_date + same case_name (fuzzy)
    Returns list of (keep_record, delete_records) tuples.
    """
    # Sub-group by source
    by_source = defaultdict(list)
    for r in records:
        by_source[r['source']].append(r)

    actions = []
    for source, source_records in by_source.items():
        if len(source_records) <= 1:
            continue

        # Build dedup key
        sub_groups = defaultdict(list)
        for r in source_records:
            raw = r.get('raw_data') or {}
            if source == 'pacer':
                key = str(raw.get('caseId', '') or raw.get('courtCase', {}).get('caseId', ''))
            elif source == 'courtlistener':
                key = str(r.get('external_id') or raw.get('cluster_id') or raw.get('docket_id') or '')
            else:
                key = f"{r.get('filing_date', '')}|{(r.get('case_name') or '')[:50]}"
            sub_groups[key].append(r)

        for key, group in sub_groups.items():
            if len(group) <= 1:
                continue
            # Sort by richness (descending), keep richest
            group.sort(key=lambda x: (
                1 if x['has_docket_entries'] else 0,  # prefer ones with docket entries
                1 if x['has_person_links'] else 0,    # prefer ones with person links
                richness_score(x)
            ), reverse=True)
            keep = group[0]
            delete = group[1:]
            actions.append((keep, delete))

    return actions


def dedup_pass(cur, records, dry_run=False, verbose=False):
    """
    Main deduplication pass:
    1. Remove within-source duplicates
    2. Mark canonical records for cross-source duplicates
    3. Populate document_availability
    """
    case_groups = group_by_case(records)
    
    total_deleted = 0
    total_relinked = 0
    canonical_set = 0
    availability_created = 0

    for case_number, group in case_groups.items():
        if not case_number:
            continue

        # --- Pass 1: Within-source duplicate removal ---
        within_dupes = find_within_source_dupes(group)
        for keep, deletes in within_dupes:
            for d in deletes:
                # Safety: never delete records with docket entries
                if d['has_docket_entries']:
                    if verbose:
                        print(f"  SKIP delete {d['id']} (has docket entries)")
                    continue

                # Re-link person_court_records from deleted to kept
                if d['has_person_links']:
                    if not dry_run:
                        # Get person links from record to delete
                        cur.execute("""
                            SELECT person_id, role, source_id FROM person_court_records
                            WHERE court_record_id = %s
                        """, (str(d['id']),))
                        links = cur.fetchall()
                        for person_id, role, source_id in links:
                            cur.execute("""
                                INSERT INTO person_court_records (person_id, court_record_id, role, source_id)
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT (person_id, court_record_id) DO NOTHING
                            """, (person_id, str(keep['id']), role, source_id))
                        total_relinked += len(links)

                if verbose:
                    print(f"  DELETE within-source dupe: {d['id']} [{d['source']}] "
                          f"case={case_number} (keeping {keep['id']})")
                if not dry_run:
                    # CASCADE will remove docket_entries and person_court_records
                    cur.execute("DELETE FROM court_records WHERE id = %s", (str(d['id']),))
                total_deleted += 1

        # Refresh group after deletions
        if not dry_run and within_dupes:
            remaining_ids = {r['id'] for r in group}
            deleted_ids = set()
            for _, deletes in within_dupes:
                for d in deletes:
                    if not d['has_docket_entries']:
                        deleted_ids.add(d['id'])
            group = [r for r in group if r['id'] not in deleted_ids]

        # --- Pass 2: Cross-source canonical marking ---
        sources = {r['source'] for r in group}
        if len(sources) > 1 or len(group) > 0:
            # Sort by priority (ascending) then richness (descending)
            group.sort(key=lambda x: (get_effective_priority(x), -richness_score(x)))
            canonical = group[0]

            if not dry_run:
                cur.execute("""
                    UPDATE court_records SET is_canonical = true WHERE id = %s
                """, (str(canonical['id']),))
                canonical_set += 1

                # Point non-canonical records to canonical
                for r in group[1:]:
                    cur.execute("""
                        UPDATE court_records SET is_canonical = false, canonical_id = %s
                        WHERE id = %s
                    """, (str(canonical['id']), str(r['id'])))

            if verbose and len(sources) > 1:
                print(f"  CANONICAL: {canonical['id']} [{canonical['source']}] "
                      f"for case {case_number} (sources: {sorted(sources)})")

        # --- Pass 3: Populate document_availability ---
        for r in group:
            raw = r.get('raw_data') or {}
            doc_url = raw.get('caseLink') or raw.get('api_url') or raw.get('absolute_url') or ''
            ext_id = r.get('external_id') or str(raw.get('caseId', '') or raw.get('cluster_id', '') or '')

            if not dry_run:
                cur.execute("""
                    INSERT INTO document_availability 
                    (case_number, court_record_id, source, source_priority, 
                     is_available, is_free, document_url, external_id)
                    VALUES (%s, %s, %s, %s, true, true, %s, %s)
                    ON CONFLICT (case_number, source, external_id) DO UPDATE SET
                        court_record_id = EXCLUDED.court_record_id,
                        is_available = true,
                        last_checked_at = now()
                """, (
                    case_number,
                    str(r['id']),
                    r['source'],
                    get_effective_priority(r),
                    doc_url[:1000] if doc_url else None,
                    ext_id[:255] if ext_id else None
                ))
                availability_created += 1

    return {
        'total_deleted': total_deleted,
        'total_relinked': total_relinked,
        'canonical_set': canonical_set,
        'availability_created': availability_created,
    }


def cross_source_cleanup(cur, dry_run=False, verbose=False):
    """
    For cross-source duplicates, delete the lower-priority record if the
    canonical (higher-priority) record has at least as much data.
    
    We're more conservative here — only delete if:
    - The record has NO docket entries
    - The record has NO unique person links
    - The canonical record exists and has all the fields
    """
    cur.execute("""
        SELECT cr.id, cr.case_number, cr.source, cr.source_priority, 
               cr.canonical_id, cr.is_canonical,
               EXISTS(SELECT 1 FROM court_docket_entries cde WHERE cde.court_record_id = cr.id) AS has_docket,
               EXISTS(SELECT 1 FROM person_court_records pcr WHERE pcr.court_record_id = cr.id) AS has_links
        FROM court_records cr
        WHERE cr.is_canonical = false AND cr.canonical_id IS NOT NULL
        ORDER BY cr.case_number, cr.source_priority
    """)
    
    non_canonical = cur.fetchall()
    deleted = 0
    relinked = 0
    
    for row in non_canonical:
        rec_id, case_num, source, priority, canonical_id, _, has_docket, has_links = row
        
        if has_docket:
            if verbose:
                print(f"  SKIP cross-source delete {rec_id} (has docket entries)")
            continue

        # Re-link person records to canonical
        if has_links:
            if not dry_run:
                cur.execute("""
                    SELECT person_id, role, source_id FROM person_court_records
                    WHERE court_record_id = %s
                """, (str(rec_id),))
                links = cur.fetchall()
                for person_id, role, source_id in links:
                    cur.execute("""
                        INSERT INTO person_court_records (person_id, court_record_id, role, source_id)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (person_id, court_record_id) DO NOTHING
                    """, (person_id, str(canonical_id), role, source_id))
                relinked += len(links)

        if verbose:
            print(f"  DELETE cross-source: {rec_id} [{source}] → canonical {canonical_id} case={case_num}")
        if not dry_run:
            cur.execute("DELETE FROM court_records WHERE id = %s", (str(rec_id),))
        deleted += 1

    return {'cross_deleted': deleted, 'cross_relinked': relinked}


def print_summary(stats):
    """Print human-readable summary."""
    print("\n" + "=" * 60)
    print("DEDUPLICATION SUMMARY")
    print("=" * 60)
    print(f"  Within-source duplicates deleted:    {stats['total_deleted']}")
    print(f"  Person links re-assigned:            {stats['total_relinked']}")
    print(f"  Canonical records marked:            {stats['canonical_set']}")
    print(f"  Availability entries created:        {stats['availability_created']}")
    if 'cross_deleted' in stats:
        print(f"  Cross-source duplicates deleted:     {stats['cross_deleted']}")
        print(f"  Cross-source links re-assigned:      {stats['cross_relinked']}")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description='Deduplicate court records')
    parser.add_argument('--dry-run', action='store_true', help='Preview changes without modifying')
    parser.add_argument('--verbose', action='store_true', help='Show details of each action')
    parser.add_argument('--no-cross-source-delete', action='store_true',
                        help='Skip deleting cross-source duplicates (only mark canonical)')
    args = parser.parse_args()

    conn = psycopg2.connect(DB_DSN)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        print("Fetching all court records...")
        records = fetch_all_records(cur)
        print(f"  Found {len(records)} records")

        print("\nRunning deduplication pass...")
        stats = dedup_pass(cur, records, dry_run=args.dry_run, verbose=args.verbose)
        
        if not args.no_cross_source_delete:
            print("\nRunning cross-source cleanup...")
            cross_stats = cross_source_cleanup(cur, dry_run=args.dry_run, verbose=args.verbose)
            stats.update(cross_stats)

        print_summary(stats)

        if args.dry_run:
            print("\n*** DRY RUN — no changes committed ***")
            conn.rollback()
        else:
            conn.commit()
            print("\nChanges committed successfully.")

            # Final count
            cur.execute("SELECT count(*) FROM court_records")
            final = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM document_availability")
            avail = cur.fetchone()[0]
            print(f"\nFinal court_records: {final}")
            print(f"Document availability entries: {avail}")

    except Exception as e:
        conn.rollback()
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    main()
