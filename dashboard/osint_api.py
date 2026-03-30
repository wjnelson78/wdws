"""
OSINT Investigation Database API Module for Athena Cognitive Platform.

Provides REST endpoints for the osint-db PostgreSQL database containing:
  - Investigations
  - Persons (subjects)
  - Court Records
  - Documents (filings, opinions, PDFs)
  - Properties & Deeds
  - Timeline Events
  - Entity Relationships

All endpoints require dashboard authentication (cookies or Basic auth).
"""

import json, os, uuid
from typing import Optional

import asyncpg
from starlette.requests import Request
from starlette.responses import JSONResponse

# ── OSINT-DB Connection Pool ─────────────────────────────────
OSINT_DATABASE_URL = os.getenv(
    "OSINT_DATABASE_URL",
    "postgresql://root:NEL2233obns@127.0.0.1:5432/osint-db",
)

_osint_pool: Optional[asyncpg.Pool] = None


async def get_osint_pool() -> asyncpg.Pool:
    global _osint_pool
    if _osint_pool is None:
        _osint_pool = await asyncpg.create_pool(
            OSINT_DATABASE_URL, min_size=2, max_size=6, command_timeout=30
        )
    return _osint_pool


def _ser(val):
    if val is None:
        return None
    if hasattr(val, "isoformat"):
        return val.isoformat()
    if isinstance(val, (dict, list)):
        return val
    return val


def _row(r):
    from decimal import Decimal
    d = dict(r)
    for k, v in d.items():
        if isinstance(v, uuid.UUID):
            d[k] = str(v)
        elif hasattr(v, "isoformat"):
            d[k] = v.isoformat()
        elif isinstance(v, Decimal):
            d[k] = float(v)
        elif isinstance(v, bytes):
            d[k] = None
    return d


# ── Stats / Overview ─────────────────────────────────────────

async def api_osint_stats(request: Request):
    """High-level statistics for the OSINT database."""
    p = await get_osint_pool()
    row = await p.fetchrow("""
        SELECT
            (SELECT count(*) FROM investigations) AS investigations,
            (SELECT count(*) FROM persons) AS persons,
            (SELECT count(*) FROM court_records) AS court_records,
            (SELECT count(DISTINCT case_number) FROM court_records) AS unique_cases,
            (SELECT count(*) FROM documents) AS documents,
            (SELECT count(*) FROM properties) AS properties,
            (SELECT count(*) FROM person_court_records) AS person_court_links,
            (SELECT count(*) FROM person_properties) AS person_property_links,
            (SELECT count(*) FROM timeline_events) AS timeline_events,
            (SELECT count(*) FROM entity_relationships) AS entity_relationships,
            (SELECT count(*) FROM notes) AS notes,
            (SELECT count(*) FROM addresses) AS addresses,
            (SELECT count(*) FROM email_addresses) AS emails,
            (SELECT count(*) FROM phone_numbers) AS phones,
            (SELECT count(*) FROM social_media_accounts) AS social_media,
            (SELECT count(*) FROM vehicles) AS vehicles
    """)
    return JSONResponse(_row(row))


async def api_osint_sources(request: Request):
    """Breakdown of court records by source and document types."""
    p = await get_osint_pool()
    by_source = await p.fetch("""
        SELECT COALESCE(source, raw_data->>'source', 'unknown') AS source, count(*) AS cnt
        FROM court_records GROUP BY 1 ORDER BY cnt DESC
    """)
    by_doc_type = await p.fetch("""
        SELECT COALESCE(document_type, 'unknown') AS doc_type, count(*) AS cnt
        FROM documents GROUP BY 1 ORDER BY cnt DESC
    """)
    by_court_type = await p.fetch("""
        SELECT COALESCE(court_type, 'unknown') AS court_type, count(*) AS cnt
        FROM court_records GROUP BY 1 ORDER BY cnt DESC
    """)
    return JSONResponse({
        "by_source": [_row(r) for r in by_source],
        "by_doc_type": [_row(r) for r in by_doc_type],
        "by_court_type": [_row(r) for r in by_court_type],
    })


# ── Investigations ───────────────────────────────────────────

async def api_osint_investigations(request: Request):
    """List all investigations with summary counts."""
    p = await get_osint_pool()
    rows = await p.fetch("""
        SELECT i.*,
               (SELECT count(*) FROM documents d WHERE d.investigation_id = i.id) AS doc_count,
               (SELECT count(*) FROM court_records cr WHERE cr.investigation_id = i.id) AS record_count,
               (SELECT count(*) FROM investigation_entities ie WHERE ie.investigation_id = i.id) AS entity_count,
               (SELECT count(*) FROM notes n WHERE n.investigation_id = i.id) AS note_count,
               (SELECT count(*) FROM timeline_events te WHERE te.investigation_id = i.id) AS event_count
        FROM investigations i
        ORDER BY i.created_at DESC
    """)
    return JSONResponse([_row(r) for r in rows])


async def api_osint_investigation_detail(request: Request):
    """Detailed investigation view with all linked entities."""
    inv_id = request.path_params["inv_id"]
    p = await get_osint_pool()

    inv = await p.fetchrow("SELECT * FROM investigations WHERE id = $1::uuid", inv_id)
    if not inv:
        return JSONResponse({"error": "not found"}, status_code=404)

    # Get all linked persons through investigation_entities
    persons = await p.fetch("""
        SELECT p.*, ie.role, ie.added_at
        FROM persons p
        JOIN investigation_entities ie ON ie.entity_type = 'person' AND ie.entity_id = p.id
        WHERE ie.investigation_id = $1::uuid
        ORDER BY p.last_name, p.first_name
    """, inv_id)

    # Recent docs
    docs = await p.fetch("""
        SELECT id, title, document_type, mime_type, file_size_bytes, created_at
        FROM documents WHERE investigation_id = $1::uuid
        ORDER BY created_at DESC LIMIT 50
    """, inv_id)

    # Court record stats
    record_stats = await p.fetch("""
        SELECT COALESCE(source, raw_data->>'source', 'unknown') AS source, count(*) AS cnt
        FROM court_records WHERE investigation_id = $1::uuid GROUP BY 1 ORDER BY cnt DESC
    """, inv_id)

    doc_stats = await p.fetch("""
        SELECT document_type, count(*) AS cnt
        FROM documents WHERE investigation_id = $1::uuid GROUP BY 1 ORDER BY cnt DESC
    """, inv_id)

    # Timeline
    timeline = await p.fetch("""
        SELECT * FROM timeline_events
        WHERE investigation_id = $1::uuid ORDER BY event_date DESC LIMIT 30
    """, inv_id)

    # Notes
    notes = await p.fetch("""
        SELECT * FROM notes WHERE investigation_id = $1::uuid ORDER BY created_at DESC
    """, inv_id)

    return JSONResponse({
        "investigation": _row(inv),
        "persons": [_row(r) for r in persons],
        "recent_documents": [_row(r) for r in docs],
        "record_stats": [_row(r) for r in record_stats],
        "doc_stats": [_row(r) for r in doc_stats],
        "timeline": [_row(r) for r in timeline],
        "notes": [_row(r) for r in notes],
    })


# ── Persons ──────────────────────────────────────────────────

async def api_osint_persons(request: Request):
    """List persons with optional investigation filter."""
    p = await get_osint_pool()
    inv_id = request.query_params.get("investigation_id")
    q = request.query_params.get("q", "").strip()

    if inv_id:
        rows = await p.fetch("""
            SELECT p.*,
                   (SELECT count(*) FROM person_court_records pcr WHERE pcr.person_id = p.id) AS court_count,
                   (SELECT count(*) FROM person_properties pp WHERE pp.person_id = p.id) AS property_count,
                   ie.role
            FROM persons p
            JOIN investigation_entities ie ON ie.entity_type = 'person' AND ie.entity_id = p.id
            WHERE ie.investigation_id = $1::uuid
            ORDER BY p.last_name, p.first_name
        """, inv_id)
    elif q:
        rows = await p.fetch("""
            SELECT p.*,
                   (SELECT count(*) FROM person_court_records pcr WHERE pcr.person_id = p.id) AS court_count,
                   (SELECT count(*) FROM person_properties pp WHERE pp.person_id = p.id) AS property_count
            FROM persons p
            WHERE p.full_name ILIKE '%' || $1 || '%'
               OR p.last_name ILIKE '%' || $1 || '%'
               OR p.first_name ILIKE '%' || $1 || '%'
            ORDER BY p.last_name, p.first_name
            LIMIT 100
        """, q)
    else:
        rows = await p.fetch("""
            SELECT p.*,
                   (SELECT count(*) FROM person_court_records pcr WHERE pcr.person_id = p.id) AS court_count,
                   (SELECT count(*) FROM person_properties pp WHERE pp.person_id = p.id) AS property_count
            FROM persons p
            ORDER BY p.last_name, p.first_name
            LIMIT 200
        """)
    return JSONResponse([_row(r) for r in rows])


async def api_osint_person_detail(request: Request):
    """Full person profile with all linked data."""
    person_id = request.path_params["person_id"]
    p = await get_osint_pool()

    person = await p.fetchrow("SELECT * FROM persons WHERE id = $1::uuid", person_id)
    if not person:
        return JSONResponse({"error": "not found"}, status_code=404)

    # Court records (deduplicated by case_number, pick best row per case)
    court_records = await p.fetch("""
        SELECT sub.*, rc.record_count FROM (
            SELECT DISTINCT ON (cr.case_number) cr.*, pcr.role
            FROM court_records cr
            JOIN person_court_records pcr ON pcr.court_record_id = cr.id
            WHERE pcr.person_id = $1::uuid
            ORDER BY cr.case_number,
                     (cr.case_name IS NOT NULL AND cr.case_name != '') DESC,
                     cr.filing_date DESC NULLS LAST,
                     cr.created_at DESC
        ) sub
        LEFT JOIN (
            SELECT cr.case_number, count(*) AS record_count
            FROM court_records cr
            JOIN person_court_records pcr ON pcr.court_record_id = cr.id
            WHERE pcr.person_id = $1::uuid
            GROUP BY cr.case_number
        ) rc ON rc.case_number = sub.case_number
        ORDER BY sub.filing_date DESC NULLS LAST
    """, person_id)

    # Properties
    properties = await p.fetch("""
        SELECT prop.*, pp.relationship, pp.is_current
        FROM properties prop
        JOIN person_properties pp ON pp.property_id = prop.id
        WHERE pp.person_id = $1::uuid
        ORDER BY pp.is_current DESC NULLS LAST
    """, person_id)

    # Addresses (include junction table data: is_current, address_type, dates)
    addresses = await p.fetch("""
        SELECT a.*,
               pa.is_current AS pa_is_current,
               pa.address_type AS pa_address_type,
               pa.date_first_seen AS pa_date_first_seen,
               pa.date_last_seen AS pa_date_last_seen
        FROM addresses a
        JOIN person_addresses pa ON pa.address_id = a.id
        WHERE pa.person_id = $1::uuid
        ORDER BY pa.is_current DESC NULLS LAST, pa.date_last_seen DESC NULLS LAST
    """, person_id)

    # Emails (include is_current/best_match from junction)
    emails = await p.fetch("""
        SELECT e.*,
               e.raw_data->>'email_type' AS email_type,
               COALESCE((e.raw_data->>'is_best_match')::boolean, false) AS is_best_match,
               pe.is_current AS pe_is_current
        FROM email_addresses e
        JOIN person_emails pe ON pe.email_id = e.id
        WHERE pe.person_id = $1::uuid
        ORDER BY pe.is_current DESC NULLS LAST
    """, person_id)

    # Phones (include is_current from junction)
    phones = await p.fetch("""
        SELECT ph.*, pp.is_current AS pp_is_current
        FROM phone_numbers ph
        JOIN person_phones pp ON pp.phone_id = ph.id
        WHERE pp.person_id = $1::uuid
        ORDER BY pp.is_current DESC NULLS LAST
    """, person_id)

    # Employment
    employment = await p.fetch("""
        SELECT er.* FROM employment_records er
        JOIN person_employment pe ON pe.employment_id = er.id
        WHERE pe.person_id = $1::uuid
        ORDER BY er.start_date DESC NULLS LAST
    """, person_id)

    # Education
    education = await p.fetch("""
        SELECT ed.* FROM education_records ed
        JOIN person_education pe ON pe.education_id = ed.id
        WHERE pe.person_id = $1::uuid
    """, person_id)

    # Social media
    social = await p.fetch("""
        SELECT sm.* FROM social_media_accounts sm
        JOIN person_social_media psm ON psm.social_media_id = sm.id
        WHERE psm.person_id = $1::uuid
    """, person_id)

    # Vehicles
    vehicles = await p.fetch("""
        SELECT v.* FROM vehicles v
        JOIN person_vehicles pv ON pv.vehicle_id = v.id
        WHERE pv.person_id = $1::uuid
    """, person_id)

    # Identifiers (SSN last4, DL, etc)
    identifiers = await p.fetch("""
        SELECT * FROM person_identifiers WHERE person_id = $1::uuid
    """, person_id)

    # Relationships
    relationships = await p.fetch("""
        SELECT er.*,
               CASE WHEN er.source_entity_id = $1::uuid THEN er.target_entity_id
                    ELSE er.source_entity_id END AS other_id,
               CASE WHEN er.source_entity_id = $1::uuid THEN er.target_entity_type
                    ELSE er.source_entity_type END AS other_type,
               p_other.full_name AS other_name
        FROM entity_relationships er
        LEFT JOIN persons p_other ON p_other.id = CASE
            WHEN er.source_entity_id = $1::uuid THEN er.target_entity_id
            ELSE er.source_entity_id END
        WHERE (er.source_entity_id = $1::uuid AND er.source_entity_type = 'person')
           OR (er.target_entity_id = $1::uuid AND er.target_entity_type = 'person')
    """, person_id)

    # Investigations this person is part of
    investigations = await p.fetch("""
        SELECT i.id, i.case_number, i.title, i.status, ie.role
        FROM investigations i
        JOIN investigation_entities ie ON ie.investigation_id = i.id
        WHERE ie.entity_type = 'person' AND ie.entity_id = $1::uuid
    """, person_id)

    return JSONResponse({
        "person": _row(person),
        "court_records": [_row(r) for r in court_records],
        "properties": [_row(r) for r in properties],
        "addresses": [_row(r) for r in addresses],
        "emails": [_row(r) for r in emails],
        "phones": [_row(r) for r in phones],
        "employment": [_row(r) for r in employment],
        "education": [_row(r) for r in education],
        "social_media": [_row(r) for r in social],
        "vehicles": [_row(r) for r in vehicles],
        "identifiers": [_row(r) for r in identifiers],
        "relationships": [_row(r) for r in relationships],
        "investigations": [_row(r) for r in investigations],
    })


# ── Court Records ────────────────────────────────────────────

async def api_osint_court_records(request: Request):
    """Browse court records with filters."""
    p = await get_osint_pool()
    inv_id = request.query_params.get("investigation_id")
    person_id = request.query_params.get("person_id")
    source = request.query_params.get("source")
    court_type = request.query_params.get("court_type")
    q = request.query_params.get("q", "").strip()
    offset = int(request.query_params.get("offset", 0))
    limit = min(int(request.query_params.get("limit", 50)), 200)

    conditions = []
    params = []
    idx = 1

    if inv_id:
        conditions.append(f"cr.investigation_id = ${idx}::uuid")
        params.append(inv_id)
        idx += 1
    if person_id:
        conditions.append(f"cr.id IN (SELECT court_record_id FROM person_court_records WHERE person_id = ${idx}::uuid)")
        params.append(person_id)
        idx += 1
    if source:
        conditions.append(f"(cr.source = ${idx} OR cr.raw_data->>'source' = ${idx})")
        params.append(source)
        idx += 1
    if court_type:
        conditions.append(f"cr.court_type = ${idx}")
        params.append(court_type)
        idx += 1
    if q:
        conditions.append(f"""(
            cr.case_number ILIKE '%' || ${idx} || '%'
            OR cr.case_name ILIKE '%' || ${idx} || '%'
            OR cr.plaintiff ILIKE '%' || ${idx} || '%'
            OR cr.defendant ILIKE '%' || ${idx} || '%'
            OR cr.attorney ILIKE '%' || ${idx} || '%'
        )""")
        params.append(q)
        idx += 1

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    count_row = await p.fetchrow(f"SELECT count(DISTINCT cr.case_number) AS total FROM court_records cr {where}", *params)

    rows = await p.fetch(f"""
        SELECT sub.*, rc.record_count FROM (
            SELECT DISTINCT ON (cr.case_number) cr.*
            FROM court_records cr
            {where}
            ORDER BY cr.case_number,
                     (cr.case_name IS NOT NULL AND cr.case_name != '') DESC,
                     cr.filing_date DESC NULLS LAST,
                     cr.created_at DESC
        ) sub
        LEFT JOIN (
            SELECT case_number, count(*) AS record_count
            FROM court_records cr {where}
            GROUP BY case_number
        ) rc ON rc.case_number = sub.case_number
        ORDER BY sub.filing_date DESC NULLS LAST, sub.created_at DESC
        LIMIT {limit} OFFSET {offset}
    """, *params + params)

    return JSONResponse({
        "total": count_row["total"],
        "offset": offset,
        "limit": limit,
        "records": [_row(r) for r in rows],
    })


async def api_osint_court_record_detail(request: Request):
    """Single court record with docket entries and linked persons."""
    rec_id = request.path_params["rec_id"]
    p = await get_osint_pool()

    rec = await p.fetchrow("SELECT * FROM court_records WHERE id = $1::uuid", rec_id)
    if not rec:
        return JSONResponse({"error": "not found"}, status_code=404)

    docket = await p.fetch("""
        SELECT * FROM court_docket_entries
        WHERE court_record_id = $1::uuid ORDER BY date_filed ASC NULLS LAST, entry_number ASC
    """, rec_id)

    persons = await p.fetch("""
        SELECT p.id, p.full_name, p.first_name, p.last_name, pcr.role
        FROM persons p
        JOIN person_court_records pcr ON pcr.person_id = p.id
        WHERE pcr.court_record_id = $1::uuid
    """, rec_id)

    docs = await p.fetch("""
        SELECT d.id, d.title, d.document_type, d.mime_type, d.file_size_bytes, d.created_at
        FROM documents d
        WHERE d.investigation_id = (SELECT investigation_id FROM court_records WHERE id = $1::uuid)
          AND (d.title ILIKE '%' || $2 || '%' OR d.file_path ILIKE '%' || $2 || '%')
        LIMIT 20
    """, rec_id, dict(rec).get("case_number", "ZZZZNOCASE"))

    # Sibling records (same case_number, different ingestion)
    siblings = await p.fetch("""
        SELECT id, case_number, case_name, court_name, court_type, source, filing_date,
               raw_data->>'source' AS raw_source
        FROM court_records
        WHERE case_number = $1 AND id != $2::uuid
        ORDER BY created_at DESC
    """, dict(rec).get("case_number", ""), rec_id)

    # Document availability across sources
    availability = await p.fetch("""
        SELECT da.id, da.source, da.source_priority, da.is_available, da.is_free,
               da.estimated_cost, da.document_url, da.external_id, da.fetched,
               da.fetched_at, da.last_checked_at, da.notes
        FROM document_availability da
        WHERE da.case_number = $1
        ORDER BY da.source_priority ASC NULLS LAST
    """, dict(rec).get("case_number", ""))

    avail_list = []
    for a in availability:
        avail_list.append({
            "id": str(a["id"]),
            "source": a["source"],
            "source_priority": a["source_priority"],
            "is_available": a["is_available"],
            "is_free": a["is_free"],
            "estimated_cost": float(a["estimated_cost"]) if a["estimated_cost"] else None,
            "document_url": a["document_url"],
            "fetched": a["fetched"],
            "notes": a["notes"],
        })

    return JSONResponse({
        "record": _row(rec),
        "docket_entries": [_row(r) for r in docket],
        "persons": [_row(r) for r in persons],
        "related_documents": [_row(r) for r in docs],
        "sibling_records": [_row(r) for r in siblings],
        "document_availability": avail_list,
    })


# ── Documents ─────────────────────────────────────────────────

async def api_osint_documents(request: Request):
    """Browse OSINT documents with filters."""
    p = await get_osint_pool()
    inv_id = request.query_params.get("investigation_id")
    doc_type = request.query_params.get("document_type")
    q = request.query_params.get("q", "").strip()
    offset = int(request.query_params.get("offset", 0))
    limit = min(int(request.query_params.get("limit", 50)), 200)

    conditions = []
    params = []
    idx = 1

    if inv_id:
        conditions.append(f"d.investigation_id = ${idx}::uuid")
        params.append(inv_id)
        idx += 1
    if doc_type:
        conditions.append(f"d.document_type = ${idx}")
        params.append(doc_type)
        idx += 1
    if q:
        conditions.append(f"(d.title ILIKE '%' || ${idx} || '%' OR d.content_text ILIKE '%' || ${idx} || '%')")
        params.append(q)
        idx += 1

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    count_row = await p.fetchrow(f"SELECT count(*) AS total FROM documents d {where}", *params)

    rows = await p.fetch(f"""
        SELECT d.id, d.investigation_id, d.title, d.document_type, d.mime_type,
               d.file_size_bytes, d.file_path, d.uploaded_by, d.created_at,
               i.case_number AS inv_case_number
        FROM documents d
        LEFT JOIN investigations i ON i.id = d.investigation_id
        {where}
        ORDER BY d.created_at DESC
        LIMIT {limit} OFFSET {offset}
    """, *params)

    return JSONResponse({
        "total": count_row["total"],
        "offset": offset,
        "limit": limit,
        "documents": [_row(r) for r in rows],
    })


async def api_osint_document_detail(request: Request):
    """Single document with content preview."""
    doc_id = request.path_params["doc_id"]
    p = await get_osint_pool()

    doc = await p.fetchrow("SELECT * FROM documents WHERE id = $1::uuid", doc_id)
    if not doc:
        return JSONResponse({"error": "not found"}, status_code=404)

    d = _row(doc)
    # Truncate huge content for API response
    if d.get("content_text") and len(d["content_text"]) > 50000:
        d["content_text"] = d["content_text"][:50000] + "\n\n[... truncated ...]"
    if d.get("ocr_text") and len(d["ocr_text"]) > 50000:
        d["ocr_text"] = d["ocr_text"][:50000] + "\n\n[... truncated ...]"

    return JSONResponse(d)


# ── Properties ────────────────────────────────────────────────

async def api_osint_properties(request: Request):
    """List properties with optional person filter."""
    p = await get_osint_pool()
    person_id = request.query_params.get("person_id")

    if person_id:
        rows = await p.fetch("""
            SELECT prop.*, pp.relationship, pp.is_current,
                   per.full_name AS owner_name
            FROM properties prop
            JOIN person_properties pp ON pp.property_id = prop.id
            JOIN persons per ON per.id = pp.person_id
            WHERE pp.person_id = $1::uuid
            ORDER BY pp.is_current DESC NULLS LAST
        """, person_id)
    else:
        rows = await p.fetch("""
            SELECT prop.*,
                   (SELECT string_agg(per.full_name, ', ')
                    FROM person_properties pp
                    JOIN persons per ON per.id = pp.person_id
                    WHERE pp.property_id = prop.id) AS owners
            FROM properties prop
            ORDER BY prop.created_at DESC
            LIMIT 100
        """)
    return JSONResponse([_row(r) for r in rows])


async def api_osint_property_detail(request: Request):
    """Full property detail with deeds, valuations, permits, loans, residents."""
    prop_id = request.path_params["prop_id"]
    p = await get_osint_pool()

    prop = await p.fetchrow("SELECT * FROM properties WHERE id = $1::uuid", prop_id)
    if not prop:
        return JSONResponse({"error": "not found"}, status_code=404)

    deeds = await p.fetch(
        "SELECT * FROM property_deeds WHERE property_id = $1::uuid ORDER BY recorded_date DESC NULLS LAST", prop_id)
    valuations = await p.fetch(
        "SELECT * FROM property_valuations WHERE property_id = $1::uuid ORDER BY valuation_date DESC", prop_id)
    loans = await p.fetch(
        "SELECT * FROM property_loans WHERE property_id = $1::uuid ORDER BY loan_date DESC NULLS LAST", prop_id)
    permits = await p.fetch(
        "SELECT * FROM property_permits WHERE property_id = $1::uuid ORDER BY permit_date DESC NULLS LAST", prop_id)
    residents = await p.fetch("""
        SELECT pr.*, per.full_name
        FROM property_residents pr
        LEFT JOIN persons per ON per.id = pr.person_id
        WHERE pr.property_id = $1::uuid
        ORDER BY pr.first_seen DESC NULLS LAST
    """, prop_id)
    owners = await p.fetch("""
        SELECT per.id, per.full_name, pp.relationship, pp.is_current
        FROM person_properties pp
        JOIN persons per ON per.id = pp.person_id
        WHERE pp.property_id = $1::uuid
    """, prop_id)

    return JSONResponse({
        "property": _row(prop),
        "deeds": [_row(r) for r in deeds],
        "valuations": [_row(r) for r in valuations],
        "loans": [_row(r) for r in loans],
        "permits": [_row(r) for r in permits],
        "residents": [_row(r) for r in residents],
        "owners": [_row(r) for r in owners],
    })


# ── Search ────────────────────────────────────────────────────

async def api_osint_search(request: Request):
    """Cross-entity search across OSINT data."""
    data = await request.json()
    q = data.get("q", "").strip()
    if not q or len(q) < 2:
        return JSONResponse({"error": "query too short"}, status_code=400)

    p = await get_osint_pool()
    results = {"persons": [], "court_records": [], "documents": [], "properties": []}

    # Search persons
    persons = await p.fetch("""
        SELECT id, full_name, first_name, last_name, occupation, date_of_birth
        FROM persons
        WHERE full_name ILIKE '%' || $1 || '%'
           OR last_name ILIKE '%' || $1 || '%'
           OR $1 = ANY(aliases)
        LIMIT 20
    """, q)
    results["persons"] = [_row(r) for r in persons]

    # Search court records
    records = await p.fetch("""
        SELECT id, case_number, case_name, court_name, court_type, filing_date, source
        FROM court_records
        WHERE case_number ILIKE '%' || $1 || '%'
           OR case_name ILIKE '%' || $1 || '%'
           OR plaintiff ILIKE '%' || $1 || '%'
           OR defendant ILIKE '%' || $1 || '%'
           OR attorney ILIKE '%' || $1 || '%'
        LIMIT 30
    """, q)
    results["court_records"] = [_row(r) for r in records]

    # Search documents
    docs = await p.fetch("""
        SELECT id, title, document_type, mime_type, file_size_bytes, created_at
        FROM documents
        WHERE title ILIKE '%' || $1 || '%'
           OR content_text ILIKE '%' || $1 || '%'
        LIMIT 20
    """, q)
    results["documents"] = [_row(r) for r in docs]

    # Search properties
    props = await p.fetch("""
        SELECT id, full_address, city, state, property_type, raw_data
        FROM properties
        WHERE full_address ILIKE '%' || $1 || '%'
           OR city ILIKE '%' || $1 || '%'
        LIMIT 10
    """, q)
    results["properties"] = [_row(r) for r in props]

    total = sum(len(v) for v in results.values())
    return JSONResponse({"query": q, "total": total, **results})


# ── Timeline ──────────────────────────────────────────────────

async def api_osint_timeline(request: Request):
    """Timeline events for an investigation."""
    inv_id = request.query_params.get("investigation_id")
    limit = min(int(request.query_params.get("limit", 50)), 200)

    p = await get_osint_pool()
    if inv_id:
        rows = await p.fetch("""
            SELECT te.*, i.case_number
            FROM timeline_events te
            LEFT JOIN investigations i ON i.id = te.investigation_id
            WHERE te.investigation_id = $1::uuid
            ORDER BY te.event_date DESC NULLS LAST
            LIMIT $2
        """, inv_id, limit)
    else:
        rows = await p.fetch("""
            SELECT te.*, i.case_number
            FROM timeline_events te
            LEFT JOIN investigations i ON i.id = te.investigation_id
            ORDER BY te.event_date DESC NULLS LAST
            LIMIT $1
        """, limit)
    return JSONResponse([_row(r) for r in rows])


# ── Notes ─────────────────────────────────────────────────────

async def api_osint_notes(request: Request):
    """Get/create notes for an investigation."""
    p = await get_osint_pool()
    if request.method == "GET":
        inv_id = request.query_params.get("investigation_id")
        if not inv_id:
            return JSONResponse({"error": "investigation_id required"}, status_code=400)
        rows = await p.fetch("""
            SELECT * FROM notes WHERE investigation_id = $1::uuid ORDER BY created_at DESC
        """, inv_id)

        return JSONResponse([_row(r) for r in rows])
    else:
        data = await request.json()
        inv_id = data.get("investigation_id")
        title = data.get("title", "")
        content = data.get("content", "")
        author = data.get("author", "dashboard")
        row = await p.fetchrow("""
            INSERT INTO notes (investigation_id, title, content, created_by)
            VALUES ($1::uuid, $2, $3, $4) RETURNING *
        """, inv_id, title, content, author)
        return JSONResponse(_row(row))




# ── Document Availability ─────────────────────────────────────

async def api_osint_doc_availability(request: Request):
    """Get document availability across sources for a case."""
    case_number = request.query_params.get("case_number", "")
    rec_id = request.query_params.get("court_record_id", "")
    p = await get_osint_pool()

    if rec_id:
        # Look up case_number from court_record_id
        row = await p.fetchrow("SELECT case_number FROM court_records WHERE id = $1::uuid", rec_id)
        if row:
            case_number = row["case_number"] or case_number

    if not case_number:
        return JSONResponse({"error": "case_number or court_record_id required"}, status_code=400)

    rows = await p.fetch("""
        SELECT da.id, da.case_number, da.source, da.source_priority,
               da.is_available, da.is_free, da.estimated_cost,
               da.document_url, da.external_id, da.fetched, da.fetched_at,
               da.last_checked_at, da.notes,
               cr.case_name, cr.court_name, cr.source AS record_source,
               cr.is_canonical
        FROM document_availability da
        LEFT JOIN court_records cr ON cr.id = da.court_record_id
        WHERE da.case_number = $1
        ORDER BY da.source_priority ASC NULLS LAST
    """, case_number)

    sources = []
    for r in rows:
        sources.append({
            "id": str(r["id"]),
            "source": r["source"],
            "source_priority": r["source_priority"],
            "is_available": r["is_available"],
            "is_free": r["is_free"],
            "estimated_cost": float(r["estimated_cost"]) if r["estimated_cost"] else None,
            "document_url": r["document_url"],
            "external_id": r["external_id"],
            "fetched": r["fetched"],
            "fetched_at": r["fetched_at"].isoformat() if r["fetched_at"] else None,
            "last_checked_at": r["last_checked_at"].isoformat() if r["last_checked_at"] else None,
            "notes": r["notes"],
            "case_name": r["case_name"],
            "record_source": r["record_source"],
            "is_canonical": r["is_canonical"],
        })

    # Also check if PACER availability is known but not yet in the table
    pacer_known = any(s["source"] == "pacer" for s in sources)
    free_available = any(s["is_free"] and s["is_available"] for s in sources)

    return JSONResponse({
        "case_number": case_number,
        "sources": sources,
        "has_free_source": free_available,
        "has_pacer": pacer_known,
        "recommendation": _availability_recommendation(sources),
    })


def _availability_recommendation(sources):
    """Recommend best action for fetching this document."""
    if not sources:
        return {"action": "search", "message": "No sources found. Search PACER/CourtListener."}

    free_sources = [s for s in sources if s["is_free"] and s["is_available"]]
    if free_sources:
        best = free_sources[0]
        if best["fetched"]:
            return {"action": "view", "message": f"Document already fetched from {best['source']}."}
        return {"action": "fetch_free", "source": best["source"],
                "message": f"Available free from {best['source']}."}

    paid_sources = [s for s in sources if s["is_available"] and not s["is_free"]]
    if paid_sources:
        best = paid_sources[0]
        cost_str = f"${best['estimated_cost']:.2f}" if best["estimated_cost"] else "unknown cost"
        return {"action": "fetch_paid", "source": best["source"],
                "estimated_cost": best["estimated_cost"],
                "message": f"Available from {best['source']} ({cost_str})."}

    return {"action": "unavailable", "message": "Document not currently available from any source."}


async def api_osint_doc_availability_update(request: Request):
    """Mark a document as fetched or update availability info."""
    body = await request.json()
    avail_id = body.get("id")
    action = body.get("action")  # "mark_fetched", "update_cost", "mark_unavailable"
    p = await get_osint_pool()

    if not avail_id:
        return JSONResponse({"error": "id required"}, status_code=400)

    if action == "mark_fetched":
        await p.execute("""
            UPDATE document_availability SET fetched = true, fetched_at = now(), updated_at = now()
            WHERE id = $1::uuid
        """, avail_id)
    elif action == "update_cost":
        cost = body.get("estimated_cost")
        await p.execute("""
            UPDATE document_availability SET estimated_cost = $2, is_free = false, updated_at = now()
            WHERE id = $1::uuid
        """, avail_id, cost)
    elif action == "mark_unavailable":
        await p.execute("""
            UPDATE document_availability SET is_available = false, updated_at = now()
            WHERE id = $1::uuid
        """, avail_id)
    elif action == "add_pacer":
        # Add a PACER availability entry for a case that only has free sources
        case_number = body.get("case_number")
        court_record_id = body.get("court_record_id")
        cost = body.get("estimated_cost", 0.10)
        if not case_number:
            return JSONResponse({"error": "case_number required for add_pacer"}, status_code=400)
        await p.execute("""
            INSERT INTO document_availability
            (case_number, court_record_id, source, source_priority, is_available, is_free, estimated_cost)
            VALUES ($1, $2::uuid, 'pacer', 1, true, false, $3)
            ON CONFLICT (case_number, source, external_id) DO UPDATE SET
              estimated_cost = EXCLUDED.estimated_cost, is_available = true, updated_at = now()
        """, case_number, court_record_id, cost)
    else:
        return JSONResponse({"error": f"unknown action: {action}"}, status_code=400)

    return JSONResponse({"ok": True})

# ── Route Definitions ────────────────────────────────────────
# Import these in app.py and add to the routes list

def get_osint_routes():
    """Return Starlette Route objects for all OSINT endpoints."""
    from starlette.routing import Route
    return [
        Route("/api/osint/stats", api_osint_stats),
        Route("/api/osint/sources", api_osint_sources),
        Route("/api/osint/investigations", api_osint_investigations),
        Route("/api/osint/investigations/{inv_id}", api_osint_investigation_detail),
        Route("/api/osint/persons", api_osint_persons),
        Route("/api/osint/persons/{person_id}", api_osint_person_detail),
        Route("/api/osint/court-records", api_osint_court_records),
        Route("/api/osint/court-records/{rec_id}", api_osint_court_record_detail),
        Route("/api/osint/documents", api_osint_documents),
        Route("/api/osint/documents/{doc_id}", api_osint_document_detail),
        Route("/api/osint/properties", api_osint_properties),
        Route("/api/osint/properties/{prop_id}", api_osint_property_detail),
        Route("/api/osint/search", api_osint_search, methods=["POST"]),
        Route("/api/osint/timeline", api_osint_timeline),
        Route("/api/osint/notes", api_osint_notes, methods=["GET", "POST"]),
        Route("/api/osint/doc-availability", api_osint_doc_availability),
        Route("/api/osint/doc-availability/update", api_osint_doc_availability_update, methods=["POST"]),
    ]