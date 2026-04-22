#!/usr/bin/env python3
"""
Split Providence Swedish HealthSummary PDFs into one PDF per visit.

Input:
  data/medical/records/records/swedish_providence_2026_04_20/
    1 of 2 - My Health Summary.PDF  (1508 pages, 100 visit-sections)
    2 of 2 - My Health Summary.PDF  (  29 pages,   7 visit-sections)

Output:
  data/medical/records/records/swedish_providence_2026_04_20/visits/
    NNN_YYYY-MM-DD_<Type>_<Facility>.pdf
    visits_manifest.csv
"""

import csv
import re
from pathlib import Path
from typing import Dict

import pypdf

_SWEDISH_APR20_DIR = Path(
    "/opt/wdws/data/medical/records/records/swedish_providence_2026_04_20"
)
_UW_APR21_DIR = Path(
    "/opt/wdws/data/medical/records/records/uw_medicine_2026_04_21"
)
_EVERGREEN_APR21_DIR = Path(
    "/opt/wdws/data/medical/records/records/evergreenhealth_2026_04_21"
)

# Registry of exports this splitter handles. Each entry is:
#   parts:     ordered list of (stem, PDF path). Stem identifies the part in
#              filenames/manifest (e.g. "1", "2", or a longer label).
#   out_dir:   where per-visit PDFs + manifest are written.
#   overrides: {(stem, visit_idx): {date, enc_type, facility}} hand-fixes for
#              visits where Epic's column-wrap defeated the auto-parser.
EXPORTS: Dict[str, dict] = {
    "swedish_providence_2026_04_20": {
        "parts": [
            ("1", _SWEDISH_APR20_DIR / "1 of 2 - My Health Summary.PDF"),
            ("2", _SWEDISH_APR20_DIR / "2 of 2 - My Health Summary.PDF"),
        ],
        "out_dir": _SWEDISH_APR20_DIR / "visits",
        "overrides": {
            ("1", 7):  {"date": "12/09/2024", "enc_type": "Surgery",          "facility": "Swedish Edmonds MP"},
            ("1", 8):  {"date": "12/09/2024", "enc_type": "Anesthesia",       "facility": "Swedish Edmonds MP"},
            ("1", 39): {"date": "04/30/2024", "enc_type": "Nurse Triage",     "facility": "Swedish Primary Care Mill Creek Campus"},
            ("1", 69): {"date": "09/23/2020", "enc_type": "Clinical Support", "facility": "Swedish Primary Care Mill Creek Campus"},
            ("1", 83): {"date": "01/30/2020", "enc_type": "Clinic Outreach",  "facility": "Swedish Primary Care Case Management"},
        },
    },
    "evergreenhealth_2026_04_21": {
        "parts": [
            ("1", _EVERGREEN_APR21_DIR / "1 of 2 - My Health Summary.PDF"),
            ("2", _EVERGREEN_APR21_DIR / "2 of 2 - My Health Summary.PDF"),
        ],
        "out_dir": _EVERGREEN_APR21_DIR / "visits",
        "overrides": {},
    },
    "uw_medicine_2026_04_21": {
        "parts": [
            ("1", _UW_APR21_DIR / "1 of 2 - My Health Summary.PDF"),
            ("2", _UW_APR21_DIR / "2 of 2 - My Health Summary.PDF"),
        ],
        "out_dir": _UW_APR21_DIR / "visits",
        "overrides": {
            # Visits where Epic wrapped facility name onto multiple lines,
            # so the parser latched onto an address/department fragment.
            # part-2 visit 11 (seq 111): 2018-06-27 Hospital Encounter at
            #   Northwest Hospital (NWH 2EAST, 1536 N 115th St).
            ("2", 11): {"enc_type": "Hospital Encounter", "facility": "UW Medical Center - Northwest"},
            # part-2 visit 13 (seq 113): 2016-02-22 Hospital Encounter at
            #   UWMC (4NE Surgery/Neurology/ENT ward).
            ("2", 13): {"enc_type": "Hospital Encounter", "facility": "UW Medical Center - 4NE"},
        },
    },
}

SECTION_HEADERS = {
    "Allergies", "Medications", "Active Problems", "Resolved Problems", "Immunizations",
    "Social History", "Last Filed Vital Signs", "Plan of Treatment", "Medical Devices",
    "Procedures", "Results", "Visit Diagnoses", "Care Teams", "Patient Contacts",
    "Document Information", "Reason for Visit", "Functional Status", "Mental Status",
    "Discharge Instructions", "Discharge Summaries", "ED Notes", "Progress Notes",
    "Administered Medications", "Insurance", "Advance Directives", "Anesthesia Record",
    "OR Notes", "Ordered Prescriptions", "Medications at Time of Discharge", "Encounters",
}
DATE_RE = re.compile(r"^\s*(\d{2}/\d{2}/\d{4})\b")
DATE_ONLY_RE = re.compile(r"^\s*\d{2}/\d{2}/\d{4}\s*$")
TIME_ONLY_RE = re.compile(r"^\s*\d{1,2}:\d{2}\s*(?:AM|PM)?\s*$")
TZ_RE = re.compile(r"^\s*(?:PDT|PST|MST|MDT|EST|EDT|CST|CDT|UTC|CT|ET|PT|MT)\s*[-–]?\s*$")
KNOWN_TYPES = [
    "Hospital Encounter", "Home Care Visit", "Virtual Office Visit", "Office Visit",
    "Urgent Care", "Video Visit", "Patient Message", "Patient E-mail", "Nurse Triage",
    "Telemedicine", "Telephone", "Phone Call", "E-Visit", "Refill Request", "Refill",
    "Emergency", "Surgery", "Procedure", "Appointment", "Anesthesia",
    "Outpatient", "Inpatient", "Observation", "Admission",
    "Lab Results", "Imaging", "Consult", "Infusion", "Sleep Study",
    "Dialysis", "Anticoag", "Scan",
    # Legacy/migrated-system encounter types (EvergreenHealth pre-Epic Cerner)
    "Legacy Kirkland", "Legacy Monroe", "Orders Only",
]


def walk_outline(reader, node=None, depth=0, out=None):
    if out is None:
        out = []
    if node is None:
        node = reader.outline
    for item in node:
        if isinstance(item, list):
            walk_outline(reader, item, depth + 1, out)
        else:
            try:
                page = reader.get_destination_page_number(item)
            except Exception:
                page = None
            out.append((depth, page, item.title))
    return out


def is_header_line(line):
    s = line.strip().rstrip(":")
    if s in SECTION_HEADERS:
        return True
    for h in SECTION_HEADERS:
        if s.startswith(h + " -") or s.startswith(h + " –"):
            return True
    return False


def is_skip_line(line):
    ls = line.strip()
    if not ls:
        return True
    if DATE_ONLY_RE.match(ls) or TIME_ONLY_RE.match(ls) or TZ_RE.match(ls):
        return True
    if ls in ("-", "–") or ls.endswith(" - "):
        return True
    return False


def parse_visit_meta(reader, start, end):
    txt = ""
    for pg in range(start, min(start + 3, end)):
        try:
            txt += "\n" + reader.pages[pg].extract_text()
        except Exception:
            pass
    lines = txt.split("\n")
    doc_type = "Unknown"
    for l in lines[:8]:
        if "generated on" in l:
            doc_type = l.split(", generated on")[0].strip()
            break

    enc_idx = None
    for i, l in enumerate(lines):
        if l.strip() == "Encounter Details":
            enc_idx = i
            break

    date = enc_type = facility = None
    if enc_idx is not None:
        data_start = None
        for i in range(enc_idx + 1, min(enc_idx + 6, len(lines))):
            if lines[i].strip().startswith("Date"):
                data_start = i + 1
                break
        if data_start:
            data = []
            for i in range(data_start, min(data_start + 30, len(lines))):
                if is_header_line(lines[i]):
                    break
                data.append(lines[i])

            for l in data:
                m = DATE_RE.match(l)
                if m:
                    date = m.group(1)
                    break

            j = 0
            while j < len(data) and is_skip_line(data[j]):
                j += 1
            if j < len(data):
                t1 = data[j].strip().lstrip("0123456789/ ").strip()
                matched = None
                for kt in sorted(KNOWN_TYPES, key=len, reverse=True):
                    if t1.startswith(kt):
                        matched = kt
                        rest = t1[len(kt):].strip()
                        if rest and rest[0].isalpha():
                            facility = rest
                        break
                if matched:
                    enc_type = matched
                else:
                    t2 = data[j + 1].strip() if j + 1 < len(data) else ""
                    combo = (t1 + " " + t2).strip()
                    for kt in sorted(KNOWN_TYPES, key=len, reverse=True):
                        if combo.startswith(kt):
                            enc_type = kt
                            rest = combo[len(kt):].strip()
                            if rest and rest[0].isalpha():
                                facility = rest
                            break
                    if not enc_type:
                        enc_type = t1

                if not facility:
                    for m in range(j + 1, len(data)):
                        ls = data[m].strip()
                        if not ls or is_skip_line(data[m]):
                            continue
                        if enc_type and ls in enc_type:
                            continue
                        if re.match(r"^[A-Za-z0-9 &\./\-]+$", ls) and any(c.isalpha() for c in ls):
                            facility = ls.strip()
                            break

                # Facility continuation: Epic wraps long facility names onto
                # a second line (e.g. "EvergreenHealth Legacy\nDepartment").
                # If the next non-skip line is a short alphabetic continuation,
                # append it. Stop at lines that look like addresses (digits)
                # or that introduce a new field (phone, person name pattern).
                if facility:
                    # Find where the facility line sits so we look after it
                    facility_idx = None
                    for m in range(j, len(data)):
                        if facility in data[m]:
                            facility_idx = m
                            break
                    if facility_idx is not None:
                        for m in range(facility_idx + 1, min(facility_idx + 3, len(data))):
                            cand = data[m].strip().rstrip("\xa0").strip()
                            if not cand or is_skip_line(data[m]):
                                continue
                            # Short, purely alphabetic continuation
                            if (
                                len(cand) <= 40
                                and re.match(r"^[A-Za-z][A-Za-z &\-]*$", cand)
                                and len(cand.split()) <= 4
                            ):
                                facility = f"{facility.rstrip(',. ')} {cand}".strip()
                            break  # only look at the very next non-skip line

    if not date:
        start_search = 0
        for i, l in enumerate(lines):
            if "This document contains information" in l:
                start_search = i + 1
                break
        for l in lines[start_search:]:
            m = re.search(r"\b(\d{2}/\d{2}/\d{4})\b", l)
            if m:
                date = m.group(1)
                break

    return {"doc_type": doc_type, "date": date, "enc_type": enc_type, "facility": facility}


def sanitize(s, maxlen=60):
    if not s:
        return ""
    s = s.strip()
    s = re.sub(r"[^A-Za-z0-9 .&-]", "", s)
    s = re.sub(r"\s+", "-", s)
    s = s.strip(".-&")
    return s[:maxlen]


def iso_date(mmddyyyy):
    if not mmddyyyy:
        return "unknown-date"
    try:
        m, d, y = mmddyyyy.split("/")
        return f"{y}-{m}-{d}"
    except Exception:
        return "unknown-date"


def collect_visits(pdf_path):
    reader = pypdf.PdfReader(str(pdf_path))
    items = walk_outline(reader)
    tops = [(p, t) for d, p, t in items if d == 0 and p is not None]
    ends = [tops[i + 1][0] for i in range(len(tops) - 1)] + [len(reader.pages)]
    visits = []
    for i, ((start, _), end) in enumerate(zip(tops, ends)):
        meta = parse_visit_meta(reader, start, end)
        meta.update(idx=i, start=start, end=end, n_pages=end - start)
        visits.append(meta)
    return reader, visits


def split_export(key: str, export: dict) -> int:
    """Split one export's PDFs into per-visit PDFs + manifest. Returns count."""
    out_dir = export["out_dir"]
    out_dir.mkdir(parents=True, exist_ok=True)
    overrides = export.get("overrides", {})

    rows = []
    counter = 0
    for stem, pdf_path in export["parts"]:
        reader, visits = collect_visits(pdf_path)
        for v in visits:
            override = overrides.get((stem, v["idx"]))
            if override:
                v.update(override)

            if v["doc_type"] in ("Patient Health Summary", "Continuity of Care Document"):
                fname = f"{counter:03d}_{sanitize(v['doc_type'])}.pdf"
            else:
                date_iso = iso_date(v["date"])
                type_part = sanitize(v["enc_type"]) or "UnknownType"
                fac_part = sanitize(v["facility"]) or "UnknownFacility"
                fname = f"{counter:03d}_{date_iso}_{type_part}_{fac_part}.pdf"

            out_path = out_dir / fname
            writer = pypdf.PdfWriter()
            for pg in range(v["start"], v["end"]):
                writer.add_page(reader.pages[pg])
            with open(out_path, "wb") as f:
                writer.write(f)

            rows.append({
                "seq": f"{counter:03d}",
                "source_pdf": f"part-{stem}",
                "source_visit_idx": v["idx"],
                "source_page_start": v["start"] + 1,
                "source_page_end": v["end"],
                "n_pages": v["n_pages"],
                "doc_type": v["doc_type"],
                "encounter_date": iso_date(v["date"]) if v["date"] else "",
                "encounter_type": v["enc_type"] or "",
                "facility": v["facility"] or "",
                "output_file": fname,
            })
            counter += 1

    manifest = out_dir / "visits_manifest.csv"
    with open(manifest, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    print(f"[{key}] wrote {counter} visit PDFs to {out_dir}")
    print(f"[{key}] manifest: {manifest}")
    return counter


def main():
    import sys
    keys = sys.argv[1:] if len(sys.argv) > 1 else list(EXPORTS.keys())
    unknown = [k for k in keys if k not in EXPORTS]
    if unknown:
        print(f"unknown export key(s): {unknown}. Known: {list(EXPORTS.keys())}")
        sys.exit(2)
    total = 0
    for k in keys:
        total += split_export(k, EXPORTS[k])
    print(f"done: {total} visits across {len(keys)} export(s)")


if __name__ == "__main__":
    main()
