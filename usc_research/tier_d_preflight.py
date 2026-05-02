#!/usr/bin/env python3
"""Tier D per-title pre-flight inspection — 29 unloaded titles."""
import os, sys, math
from pathlib import Path
from lxml import etree

USLM_NS = "http://xml.house.gov/schemas/uslm/1.0"
EXTRACTED = Path("/opt/wdws/data/usc/extracted")
CHUNK_TARGET_CHARS = 6000

# 29 unloaded titles per operator's enumeration (T5A skip-disposed)
TITLES = [
    ("T1",   "usc01.xml",  False),
    ("T3",   "usc03.xml",  False),
    ("T4",   "usc04.xml",  False),
    ("T8",   "usc08.xml",  False),
    ("T9",   "usc09.xml",  False),
    ("T11",  "usc11.xml",  False),
    ("T11a", "usc11a.xml", True),
    ("T13",  "usc13.xml",  False),
    ("T14",  "usc14.xml",  False),
    ("T17",  "usc17.xml",  False),
    ("T19",  "usc19.xml",  False),
    ("T23",  "usc23.xml",  False),
    ("T24",  "usc24.xml",  False),
    ("T27",  "usc27.xml",  False),
    ("T30",  "usc30.xml",  False),
    ("T32",  "usc32.xml",  False),
    ("T35",  "usc35.xml",  False),
    ("T36",  "usc36.xml",  False),
    ("T37",  "usc37.xml",  False),
    ("T39",  "usc39.xml",  False),
    ("T43",  "usc43.xml",  False),
    ("T44",  "usc44.xml",  False),
    ("T45",  "usc45.xml",  False),
    ("T46",  "usc46.xml",  False),
    ("T47",  "usc47.xml",  False),
    ("T48",  "usc48.xml",  False),
    ("T49",  "usc49.xml",  False),
    ("T52",  "usc52.xml",  False),
    ("T54",  "usc54.xml",  False),
]

def section_text_len(sec): return len(" ".join(sec.itertext()))
def estimate_chunks(L): return 1 if L <= CHUNK_TARGET_CHARS else max(1, math.ceil(L/CHUNK_TARGET_CHARS))
def nesting_depth(sec):
    md = 0
    def walk(el, d):
        nonlocal md
        if d > md: md = d
        for c in el: walk(c, d+1)
    walk(sec, 0)
    return md

def inspect(label, fname, is_app):
    path = EXTRACTED / fname
    if not path.exists():
        return {"label": label, "error": f"FILE_MISSING:{path}"}
    size_mb = path.stat().st_size / (1024*1024)
    try:
        root = etree.parse(str(path)).getroot()
    except Exception as e:
        return {"label": label, "error": f"PARSE_FAIL:{e}", "size_mb": size_mb}

    sections = [el for el in root.iter("{%s}section" % USLM_NS)
                if el.get("identifier","").startswith("/us/usc/")]
    court_rules = [el for el in root.iter("{%s}courtRule" % USLM_NS)
                   if el.get("identifier","").startswith("/us/usc/")]

    rule_set_hint = None
    for el in root.iter("{%s}courtRules" % USLM_NS):
        h = el.find(".//{%s}heading" % USLM_NS)
        if h is not None and h.text:
            rule_set_hint = (rule_set_hint or "") + h.text.strip()[:60] + " | "

    chapters = root.findall(".//{%s}chapter" % USLM_NS)
    subchapters = root.findall(".//{%s}subchapter" % USLM_NS)
    units = sections + court_rules
    total_units = len(units)
    if total_units == 0:
        return {"label": label, "error": "NO_UNITS", "size_mb": round(size_mb,1)}

    sample = units[:200]
    depths = [nesting_depth(u) for u in sample]
    text_lens = [section_text_len(u) for u in units]
    chunks_per = [estimate_chunks(L) for L in text_lens]
    xrefs = root.findall(".//{%s}ref" % USLM_NS)

    buckets = {"1":0, "2-3":0, "4-10":0, "11-30":0, "31+":0}
    for c in chunks_per:
        if c == 1: buckets["1"] += 1
        elif c <= 3: buckets["2-3"] += 1
        elif c <= 10: buckets["4-10"] += 1
        elif c <= 30: buckets["11-30"] += 1
        else: buckets["31+"] += 1

    return {
        "label": label, "size_mb": round(size_mb,1), "is_appendix": is_app,
        "sections": len(sections), "court_rules": len(court_rules), "total_units": total_units,
        "chapters": len(chapters), "subchapters": len(subchapters),
        "max_depth": max(depths) if depths else 0,
        "max_section_chars": max(text_lens), "avg_section_chars": int(sum(text_lens)/total_units),
        "est_total_chunks": sum(chunks_per),
        "max_chunks_per_unit": max(chunks_per),
        "avg_chunks_per_unit": round(sum(chunks_per)/total_units, 2),
        "buckets": buckets,
        "n_xrefs": len(xrefs),
        "xref_density_per_unit": round(len(xrefs)/total_units, 2),
        "rule_set_hint": rule_set_hint,
    }

def main():
    rows = [inspect(l, f, a) for l, f, a in TITLES]
    # Sort by est_total_chunks descending (greedy queue order)
    valid = sorted([r for r in rows if "error" not in r], key=lambda r: -r["est_total_chunks"])
    errors = [r for r in rows if "error" in r]

    # Stderr summary
    for r in valid:
        print(f"{r['label']:>5} sections={r['sections']:>5} "
              f"court_rules={r['court_rules']:>3} chunks={r['est_total_chunks']:>5} "
              f"max_d={r['max_depth']:>2} xref/u={r['xref_density_per_unit']:>5}",
              file=sys.stderr)
    for r in errors:
        print(f"!! {r['label']}: {r['error']}", file=sys.stderr)

    # CSV
    print("title|size_mb|is_appendix|sections|court_rules|total_units|chapters|subchapters|"
          "max_depth|max_section_chars|avg_section_chars|est_total_chunks|max_chunks_per_unit|"
          "avg_chunks_per_unit|b_1|b_2_3|b_4_10|b_11_30|b_31_plus|n_xrefs|xref_density_per_unit|rule_set_hint")
    for r in valid:
        b = r["buckets"]
        print("|".join(str(x) for x in [
            r["label"], r["size_mb"], r["is_appendix"], r["sections"], r["court_rules"],
            r["total_units"], r["chapters"], r["subchapters"], r["max_depth"],
            r["max_section_chars"], r["avg_section_chars"], r["est_total_chunks"],
            r["max_chunks_per_unit"], r["avg_chunks_per_unit"],
            b["1"], b["2-3"], b["4-10"], b["11-30"], b["31+"],
            r["n_xrefs"], r["xref_density_per_unit"], r["rule_set_hint"] or ""]))
    for r in errors:
        print(f"{r['label']}|ERROR:{r['error']}|||||||||||||||||||||")

    # Aggregate
    total_units = sum(r["total_units"] for r in valid)
    total_chunks = sum(r["est_total_chunks"] for r in valid)
    total_xrefs = sum(r["n_xrefs"] for r in valid)
    print(f"\n# SUM: titles={len(valid)} units={total_units} est_chunks={total_chunks} xrefs={total_xrefs}",
          file=sys.stderr)

if __name__ == "__main__":
    main()
