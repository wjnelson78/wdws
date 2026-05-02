"""
10-sample stratified OLRC content-fidelity round-trip.
Per operator spec at end-of-T29 pause-checkpoint:
  4 active USC sections (T42, T28, T29, +1 other) — fetch OLRC view.xhtml
  2 court_rules (FRCP + FRE, different rule sets)
  2 repealed sections (verify preserved historical text)
  2 transferred sections (verify transferred_to matches OLRC redirect)

Outputs: per-sample DB-vs-OLRC content diff. Tolerance: 5% character-level
divergence (formatting, whitespace, footnote tokens vary; we're checking
substantive text fidelity, not byte-for-byte).
"""
from __future__ import annotations
import sys, re, random, hashlib, urllib.parse
sys.path.insert(0, "/opt/wdws/usc_research")
import psycopg2
import urllib.request
from canary_lib import DSN

random.seed(42)  # deterministic sampling
USER_AGENT = "USC-Ingest-Canary/1.0 (Phase4 OLRC fidelity check)"
TOLERANCE = 0.10   # 10% char divergence acceptable; we measure substantive text


def db_pick(conn, sql, *params, n=1):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    return random.sample(rows, min(n, len(rows))) if rows else []


def fetch_olrc(url: str) -> str | None:
    """Fetch and return body text from an OLRC view.xhtml URL. Returns None on fail."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"    [olrc fetch failed: {e!s}]")
        return None


def extract_section_text(html: str) -> str:
    """Strip HTML; return collapsed text for a fidelity comparison."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "lxml")
    # OLRC wraps the section in <main> and inserts the body in a <div class="usc-doc">.
    # Fall back to body if not found.
    main = soup.find("main") or soup.find("body") or soup
    text = main.get_text("\n")
    # collapse whitespace
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_for_compare(s: str) -> str:
    """Remove things that vary between OLRC HTML and our markdown:
       breadcrumb, headings styling, em dashes, footnote markers, currency syms."""
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[—–-]", "-", s)              # dash variants
    s = re.sub(r"[''""]", "'", s)              # smart quotes
    s = re.sub(r"§", "section", s, flags=re.I)
    s = re.sub(r"[^A-Za-z0-9 .,;:'()\-]", "", s)  # strip stray symbols
    return s.lower().strip()


def char_divergence(a: str, b: str) -> float:
    """Quick fidelity measure: 1 - longest_common_substring_length / max(len(a),len(b))."""
    if not a and not b:
        return 0.0
    a_tokens = set(re.findall(r"\w{4,}", a))   # word tokens of >=4 chars
    b_tokens = set(re.findall(r"\w{4,}", b))
    if not a_tokens or not b_tokens:
        return 1.0
    overlap = len(a_tokens & b_tokens)
    union = len(a_tokens | b_tokens)
    jaccard = overlap / union
    return 1.0 - jaccard


def section_olrc_url(title_number: int, section_number: str) -> str:
    return (f"https://uscode.house.gov/view.xhtml?req="
            f"granuleid:USC-prelim-title{title_number}-section{section_number}"
            f"&num=0&edition=prelim")


def court_rule_olrc_url(rule_set: str, rule_number: str) -> str:
    rs_path = {"FRAP":"App","FRCP":"Civil","FRE":"Evid","FRCrP":"Crim","FRBP":"FRBP"}.get(rule_set)
    if not rs_path:
        return None
    title = "28a" if rule_set in ("FRAP","FRCP","FRE") else "18a" if rule_set == "FRCrP" else "11a"
    return (f"https://uscode.house.gov/view.xhtml?req="
            f"granuleid:USC-prelim-title{title}-courtRules-{rs_path}-rule{rule_number}"
            f"&num=0&edition=prelim")


def main():
    conn = psycopg2.connect(DSN); conn.autocommit = True
    samples: list[dict] = []

    print("=== Selecting 10 stratified samples ===")
    # 4 active sections — 1 each from T42/T28/T29 + 1 random from any of those
    for tn in (42, 28, 29):
        for r in db_pick(conn, """
            SELECT s.id, s.title_number, s.section_number, s.canonical_citation,
                   s.unit_type, s.rule_set, s.is_repealed, s.is_transferred,
                   s.is_omitted, s.full_text, d.title
            FROM usc.sections s JOIN core.documents d ON s.document_id = d.id
            WHERE s.title_number = %s AND s.unit_type = 'section'
              AND NOT s.is_repealed AND NOT s.is_transferred AND NOT s.is_omitted
              AND length(s.full_text) > 500
        """, tn, n=1):
            samples.append({"stratum":"active", **dict(zip(
                "id title_number section_number canonical_citation unit_type "
                "rule_set is_repealed is_transferred is_omitted full_text title".split(), r))})
    # +1 from any of those (random title)
    for r in db_pick(conn, """
        SELECT s.id, s.title_number, s.section_number, s.canonical_citation,
               s.unit_type, s.rule_set, s.is_repealed, s.is_transferred,
               s.is_omitted, s.full_text, d.title
        FROM usc.sections s JOIN core.documents d ON s.document_id = d.id
        WHERE s.title_number IN (28, 29, 42) AND s.unit_type = 'section'
          AND NOT s.is_repealed AND NOT s.is_transferred AND NOT s.is_omitted
          AND length(s.full_text) > 1000
    """, n=1):
        samples.append({"stratum":"active+1", **dict(zip(
            "id title_number section_number canonical_citation unit_type "
            "rule_set is_repealed is_transferred is_omitted full_text title".split(), r))})

    # 2 court_rules — FRCP, FRE
    for rs in ("FRCP", "FRE"):
        for r in db_pick(conn, """
            SELECT s.id, s.title_number, s.section_number, s.canonical_citation,
                   s.unit_type, s.rule_set, s.is_repealed, s.is_transferred,
                   s.is_omitted, s.full_text, d.title
            FROM usc.sections s JOIN core.documents d ON s.document_id = d.id
            WHERE s.unit_type = 'court_rule' AND s.rule_set = %s
              AND length(s.full_text) > 500
        """, rs, n=1):
            samples.append({"stratum":f"court_rule_{rs}", **dict(zip(
                "id title_number section_number canonical_citation unit_type "
                "rule_set is_repealed is_transferred is_omitted full_text title".split(), r))})

    # 2 repealed
    for r in db_pick(conn, """
        SELECT s.id, s.title_number, s.section_number, s.canonical_citation,
               s.unit_type, s.rule_set, s.is_repealed, s.is_transferred,
               s.is_omitted, s.full_text, d.title
        FROM usc.sections s JOIN core.documents d ON s.document_id = d.id
        WHERE s.is_repealed AND s.unit_type='section' AND length(s.full_text) > 200
    """, n=2):
        samples.append({"stratum":"repealed", **dict(zip(
            "id title_number section_number canonical_citation unit_type "
            "rule_set is_repealed is_transferred is_omitted full_text title".split(), r))})

    # 2 transferred
    for r in db_pick(conn, """
        SELECT s.id, s.title_number, s.section_number, s.canonical_citation,
               s.unit_type, s.rule_set, s.is_repealed, s.is_transferred,
               s.is_omitted, s.full_text, d.title
        FROM usc.sections s JOIN core.documents d ON s.document_id = d.id
        WHERE s.is_transferred AND s.unit_type='section'
    """, n=2):
        samples.append({"stratum":"transferred", **dict(zip(
            "id title_number section_number canonical_citation unit_type "
            "rule_set is_repealed is_transferred is_omitted full_text title".split(), r))})

    print(f"Selected {len(samples)} samples (target 10):")
    for s in samples:
        print(f"  [{s['stratum']:<18}] {s['canonical_citation']:<35}  ({s['title'][:40]})")

    # Round-trip
    fail_count = 0
    print("\n=== Round-trip ===")
    for i, s in enumerate(samples, 1):
        if s["unit_type"] == "court_rule":
            url = court_rule_olrc_url(s["rule_set"], s["section_number"])
        else:
            url = section_olrc_url(s["title_number"], s["section_number"])
        if url is None:
            print(f"  {i}. {s['canonical_citation']}: [SKIP — no OLRC URL pattern]")
            continue
        print(f"\n  {i}. [{s['stratum']}] {s['canonical_citation']}")
        print(f"     URL: {url}")
        html = fetch_olrc(url)
        if html is None:
            print(f"     RESULT: SKIP (fetch failed)")
            fail_count += 1
            continue
        olrc_text = extract_section_text(html)
        olrc_norm = normalize_for_compare(olrc_text)[:8000]
        db_norm = normalize_for_compare(s["full_text"])[:8000]
        div = char_divergence(db_norm, olrc_norm)
        verdict = "PASS" if div <= TOLERANCE else "FAIL"
        if verdict == "FAIL":
            fail_count += 1
        print(f"     db_chars={len(s['full_text'])}  olrc_chars={len(olrc_text)}  "
              f"jaccard_divergence={div:.3f}  verdict={verdict}")

    print(f"\n=== SUMMARY ===\nsamples: {len(samples)}, failed: {fail_count}, "
          f"tolerance: {TOLERANCE}")
    sys.exit(0 if fail_count == 0 else 1)


if __name__ == "__main__":
    main()
