#!/usr/bin/env python3
"""Tier C per-title pre-flight inspection.

For each candidate title: section count (post identifier-guard),
chapter/subchapter count, chunk-count estimate (per the
CHUNK_TARGET_CHARS=6000 chunker), nesting depth max, xref density,
and special-handling notes (rule_set detection for appendix titles).

Output: pipe-delimited table to stdout.
"""
import os, sys, re, math
from pathlib import Path
from lxml import etree

USLM_NS = "http://xml.house.gov/schemas/uslm/1.0"
NS = {"u": USLM_NS}
EXTRACTED = Path("/opt/wdws/data/usc/extracted")
CHUNK_TARGET_CHARS = 6000  # mirrors usc_ingest.CHUNK_TARGET_CHARS

# (title_label, xml_filename, is_appendix)
TITLES = [
    ("T6",   "usc06.xml",  False),
    ("T7",   "usc07.xml",  False),
    ("T16",  "usc16.xml",  False),
    ("T18",  "usc18.xml",  False),
    ("T18a", "usc18a.xml", True),
    ("T21",  "usc21.xml",  False),
    ("T22",  "usc22.xml",  False),
    ("T25",  "usc25.xml",  False),
    ("T33",  "usc33.xml",  False),
    ("T38",  "usc38.xml",  False),
    ("T40",  "usc40.xml",  False),
    ("T41",  "usc41.xml",  False),
    ("T50",  "usc50.xml",  False),
    ("T51",  "usc51.xml",  False),
]

def section_text_len(sec) -> int:
    """Approximate the raw text payload of a <section> after stripping XML tags."""
    return len(" ".join(sec.itertext()))

def estimate_chunks(text_len: int) -> int:
    """Crude estimate matching ingest chunker: ceil(text/6000), min 1."""
    if text_len <= CHUNK_TARGET_CHARS:
        return 1
    return max(1, math.ceil(text_len / CHUNK_TARGET_CHARS))

def nesting_depth(sec) -> int:
    """Max element-depth under a section (proxy for sub-paragraph nesting)."""
    max_d = 0
    def walk(el, d):
        nonlocal max_d
        if d > max_d: max_d = d
        for c in el:
            walk(c, d+1)
    walk(sec, 0)
    return max_d

def inspect_title(label: str, fname: str, is_appendix: bool):
    path = EXTRACTED / fname
    if not path.exists():
        return {"label": label, "error": "FILE_MISSING", "path": str(path)}
    size_mb = path.stat().st_size / (1024*1024)
    try:
        tree = etree.parse(str(path))
    except Exception as e:
        return {"label": label, "error": f"PARSE_FAIL:{e}", "path": str(path), "size_mb": size_mb}
    root = tree.getroot()

    # All <section> + <courtRule> with /us/usc/... identifier (post identifier-guard)
    sections = []
    court_rules = []
    rule_set_hint = None
    for el in root.iter("{%s}section" % USLM_NS):
        ident = el.get("identifier", "")
        if ident.startswith("/us/usc/"):
            sections.append(el)
    for el in root.iter("{%s}courtRule" % USLM_NS):
        ident = el.get("identifier", "")
        if ident.startswith("/us/usc/"):
            court_rules.append(el)
    # Detect courtRules wrapper (rule_set hint)
    for el in root.iter("{%s}courtRules" % USLM_NS):
        h = el.find(".//{%s}heading" % USLM_NS)
        if h is not None and h.text:
            rule_set_hint = (rule_set_hint or "") + (h.text.strip()[:60] + " | ")

    chapters = root.findall(".//{%s}chapter" % USLM_NS)
    subchapters = root.findall(".//{%s}subchapter" % USLM_NS)

    units = sections + court_rules
    total_units = len(units)
    if total_units == 0:
        return {"label": label, "error": "NO_UNITS", "path": str(path), "size_mb": size_mb,
                "rule_set_hint": rule_set_hint}

    # Per-unit stats — sample up to first 200 + all units for chunk distribution
    sample = units[:200]
    depths = [nesting_depth(u) for u in sample]
    text_lens = [section_text_len(u) for u in units]
    chunks_per = [estimate_chunks(L) for L in text_lens]

    # xref density — count <ref> and <usc-ref> across whole title
    xrefs = root.findall(".//{%s}ref" % USLM_NS)
    n_xrefs = len(xrefs)

    # Chunk bucket distribution
    buckets = {"1": 0, "2-3": 0, "4-10": 0, "11-30": 0, "31+": 0}
    for c in chunks_per:
        if c == 1: buckets["1"] += 1
        elif c <= 3: buckets["2-3"] += 1
        elif c <= 10: buckets["4-10"] += 1
        elif c <= 30: buckets["11-30"] += 1
        else: buckets["31+"] += 1
    bucket_str = " | ".join(f"{k}:{v}" for k,v in buckets.items())

    return {
        "label": label,
        "path": str(path),
        "size_mb": round(size_mb, 1),
        "is_appendix": is_appendix,
        "sections": len(sections),
        "court_rules": len(court_rules),
        "total_units": total_units,
        "chapters": len(chapters),
        "subchapters": len(subchapters),
        "max_depth": max(depths) if depths else 0,
        "avg_depth": round(sum(depths)/len(depths), 1) if depths else 0,
        "max_section_chars": max(text_lens),
        "avg_section_chars": int(sum(text_lens)/len(text_lens)),
        "est_total_chunks": sum(chunks_per),
        "max_chunks_per_unit": max(chunks_per),
        "avg_chunks_per_unit": round(sum(chunks_per)/total_units, 2),
        "chunk_buckets": bucket_str,
        "n_xrefs": n_xrefs,
        "xref_density_per_unit": round(n_xrefs/total_units, 2),
        "rule_set_hint": rule_set_hint,
    }

def main():
    rows = []
    for label, fname, is_app in TITLES:
        r = inspect_title(label, fname, is_app)
        rows.append(r)
        if "error" in r:
            print(f"!! {label}: {r['error']} {r.get('path','')}", file=sys.stderr)
            continue
        print(f"{r['label']:>5} done — sections={r['sections']:>5} "
              f"court_rules={r['court_rules']:>4} est_chunks={r['est_total_chunks']:>6} "
              f"max_depth={r['max_depth']:>2} xref_density={r['xref_density_per_unit']:>5}",
              file=sys.stderr)
    # Write pipe-delim CSV
    out = ["title|path|size_mb|is_appendix|sections|court_rules|total_units|chapters|subchapters|"
           "max_depth|avg_depth|max_section_chars|avg_section_chars|est_total_chunks|"
           "max_chunks_per_unit|avg_chunks_per_unit|chunk_buckets|n_xrefs|xref_density_per_unit|rule_set_hint"]
    for r in rows:
        if "error" in r:
            out.append(f"{r['label']}|ERROR:{r['error']}|||||||||||||||||")
            continue
        out.append("|".join(str(r.get(k,"")) for k in [
            "label","path","size_mb","is_appendix","sections","court_rules","total_units",
            "chapters","subchapters","max_depth","avg_depth","max_section_chars","avg_section_chars",
            "est_total_chunks","max_chunks_per_unit","avg_chunks_per_unit","chunk_buckets",
            "n_xrefs","xref_density_per_unit","rule_set_hint"]))
    print("\n".join(out))

if __name__ == "__main__":
    main()
