#!/usr/bin/env python3
"""Phase 2.2 — Source acquisition: 32 articles + amendment landing + ~110 amendments.

Pattern: 1 req/sec rate-limit, disk cache to /tmp/wa_const_cache/, retry-with-backoff.
Per kickoff acquisition discipline (RCW Phase 8 precedent).

Idempotent: skips files already cached. Safe to re-run.

Halt triggers:
- Per-article HTTP failure rate > 5% → halt
- Less than 32 articles cached → halt
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import httpx

CACHE_DIR = Path("/tmp/wa_const_cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
LOG_PATH = Path("/opt/wdws/wa_constitution_research/phase2_acquire.log")
ACQUIRED_INDEX = Path("/opt/wdws/wa_constitution_research/phase2_acquired_index.json")

USER_AGENT = "wdws/athena-wa-constitution-ingest/1.0 (legal-research; contact wjnelson78@gmail.com)"
RATE_LIMIT_SEC = 1.0
RETRY_MAX = 3
RETRY_BACKOFF = 2.0  # seconds, exponential


# Per-article URL index from Phase 1 verification (32 articles)
ARTICLE_URLS: dict[str, dict[str, str]] = {
    "I":     {"title": "Declaration of Rights",                         "guid": "15aea7cf-9f33-455d-9ebc-027ccb3c6b98"},
    "II":    {"title": "Legislative Department",                        "guid": "13c8f0de-58d7-4cee-b367-4478b3be34fb"},
    "III":   {"title": "The Executive",                                 "guid": "62da7e39-e41a-4405-be4e-ed6f593ca55f"},
    "IV":    {"title": "The Judiciary",                                 "guid": "ae405373-5163-4b8e-a2ab-690941cc89ac"},
    "V":     {"title": "Impeachment",                                   "guid": "30f8cb9d-4929-41fc-96d2-666f53e09b7b"},
    "VI":    {"title": "Elections and Elective Rights",                 "guid": "e0de4c3e-d3f1-47fd-85c3-f8e4946b81ff"},
    "VII":   {"title": "Revenue and Taxation",                          "guid": "36abc7f0-51d5-4d32-b0ca-c1dd1fa3c27c"},
    "VIII":  {"title": "State, County, and Municipal Indebtedness",     "guid": "c8680870-39e8-457c-b6f4-9e401940e28f"},
    "IX":    {"title": "Education",                                     "guid": "7ee60562-f33c-4f7c-af29-65d79a9f3892"},
    "X":     {"title": "Militia",                                       "guid": "b022defb-25c4-4749-8448-9468d22aa202"},
    "XI":    {"title": "County, City, and Township Organization",       "guid": "b5d3ff0e-7914-47c9-87b5-6f237d7be982"},
    "XII":   {"title": "Corporations Other Than Municipal",             "guid": "9472364f-ba82-4397-8383-60be45ebc91e"},
    "XIII":  {"title": "State Institutions",                            "guid": "4f8bddb6-9395-4079-a70e-37eb5be99567"},
    "XIV":   {"title": "Seat of Government",                            "guid": "331dcf48-d767-478c-aee4-d1efa514021b"},
    "XV":    {"title": "Harbors and Tide Waters",                       "guid": "92f99a09-72eb-4f91-a67b-614f9cfc9c02"},
    "XVI":   {"title": "School and Granted Lands",                      "guid": "4a617156-d5a9-4c7b-b901-7ec9572ea09b"},
    "XVII":  {"title": "Tide Lands",                                    "guid": "35330e9e-652f-439e-8e96-13ab0d698c36"},
    "XVIII": {"title": "State Seal",                                    "guid": "245b6d63-f504-4079-9137-5d6edcda397e"},
    "XIX":   {"title": "Exemptions",                                    "guid": "ea635f35-e084-4b95-ab6e-3a3d8262d741"},
    "XX":    {"title": "Public Health and Vital Statistics",            "guid": "c4013f0d-cbde-4b98-8e2f-64f6e842d025"},
    "XXI":   {"title": "Water and Water Rights",                        "guid": "3464b3b8-29b9-40d2-b1dd-c15ceed53e21"},
    "XXII":  {"title": "Legislative Apportionment",                     "guid": "ea1a454c-2c22-4b18-b3c0-8c62da993821"},
    "XXIII": {"title": "Amendments",                                    "guid": "697b5d20-b7a6-44e4-91c7-ae7fff95ce21"},
    "XXIV":  {"title": "Boundaries",                                    "guid": "0397defa-d288-412a-9d97-15230ed8a235"},
    "XXV":   {"title": "Jurisdiction",                                  "guid": "c3e09abb-3753-46d3-85cc-d09e52b790af"},
    "XXVI":  {"title": "Compact with the United States",                "guid": "55005615-ffde-4552-918f-bdb1ab9fb6fd"},
    "XXVII": {"title": "Schedule",                                      "guid": "67c88464-958c-42fd-92f0-a5c8a9e3970f"},
    "XXVIII":{"title": "Compensation of State Officers",                "guid": "09478a2c-5858-499e-8e28-2e737723d2f1"},
    "XXIX":  {"title": "Investments of Public Pension and Retirement Funds", "guid": "d9b609f5-915a-4c92-a095-3ef571aa58c0"},
    "XXX":   {"title": "Compensation of Public Officers",               "guid": "18db8560-7b68-43e8-a08e-0c15b50b724a"},
    "XXXI":  {"title": "Sex Equality - Rights and Responsibilities",    "guid": "273dba45-c5e5-4e5b-97e3-38636b0db9dd"},
    "XXXII": {"title": "Special Revenue Financing",                     "guid": "1354d034-48af-4534-9ea2-890b50a77898"},
}

AMENDMENT_LANDING_GUID = "dbfbe753-d62b-45a4-b6a5-92322f340d19"
BASE_URL = "https://leg.wa.gov/state-laws-and-rules/washington-state-constitution/"


class HaltError(Exception):
    pass


def log(msg: str) -> None:
    line = f"[{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}] {msg}"
    print(line, flush=True)
    with LOG_PATH.open("a") as f:
        f.write(line + "\n")


def fetch_with_retry(client: httpx.Client, url: str, dest: Path) -> bool:
    """Fetch url and write to dest. Returns True on success, False on persistent failure."""
    if dest.exists() and dest.stat().st_size > 100:
        return True  # cached
    for attempt in range(1, RETRY_MAX + 1):
        try:
            r = client.get(url, timeout=30.0, follow_redirects=True)
            if r.status_code == 200 and len(r.content) > 100:
                dest.write_bytes(r.content)
                return True
            else:
                log(f"  attempt {attempt}: HTTP {r.status_code}, len={len(r.content)} for {url}")
        except (httpx.RequestError, httpx.HTTPError) as e:
            log(f"  attempt {attempt}: {type(e).__name__}: {e} for {url}")
        if attempt < RETRY_MAX:
            time.sleep(RETRY_BACKOFF ** attempt)
    return False


def acquire_articles(client: httpx.Client) -> tuple[int, int]:
    """Acquire all 32 article HTMLs. Returns (success_count, failure_count)."""
    succ = 0
    fail = 0
    for roman, meta in ARTICLE_URLS.items():
        url = f"{BASE_URL}?section={meta['guid']}"
        dest = CACHE_DIR / f"article_{roman}.html"
        if dest.exists() and dest.stat().st_size > 100:
            succ += 1
            continue
        ok = fetch_with_retry(client, url, dest)
        if ok:
            log(f"  ✓ Article {roman}: {meta['title']} ({dest.stat().st_size} bytes)")
            succ += 1
        else:
            log(f"  ✗ Article {roman}: FAIL after {RETRY_MAX} attempts")
            fail += 1
        time.sleep(RATE_LIMIT_SEC)
    return succ, fail


def acquire_amendment_landing(client: httpx.Client) -> bool:
    """Acquire the amendment landing page."""
    url = f"{BASE_URL}?section={AMENDMENT_LANDING_GUID}"
    dest = CACHE_DIR / "amendment_landing.html"
    if dest.exists() and dest.stat().st_size > 100:
        return True
    ok = fetch_with_retry(client, url, dest)
    if ok:
        log(f"  ✓ Amendment landing ({dest.stat().st_size} bytes)")
    else:
        log(f"  ✗ Amendment landing FAIL")
    time.sleep(RATE_LIMIT_SEC)
    return ok


def main() -> int:
    log("=== Phase 2.2 acquisition begin ===")
    log(f"Cache dir: {CACHE_DIR}")
    log(f"Rate limit: {RATE_LIMIT_SEC}s/req")
    log(f"Article count: {len(ARTICLE_URLS)}")

    headers = {"User-Agent": USER_AGENT}
    with httpx.Client(headers=headers) as client:
        log("--- Acquiring articles ---")
        succ, fail = acquire_articles(client)
        total = succ + fail
        rate = (fail / total) if total else 0.0
        log(f"Articles: {succ} success, {fail} fail, failure_rate={rate:.1%}")

        if succ < 32:
            raise HaltError(f"Halt: only {succ}/32 articles cached")
        if rate > 0.05:
            raise HaltError(f"Halt: failure rate {rate:.1%} exceeds 5% threshold")

        log("--- Acquiring amendment landing ---")
        amend_ok = acquire_amendment_landing(client)
        if not amend_ok:
            raise HaltError("Halt: amendment landing fetch failed")

    # Write acquired index
    index = {
        "articles": {
            roman: {
                "title": meta["title"],
                "guid": meta["guid"],
                "cached_path": str(CACHE_DIR / f"article_{roman}.html"),
                "size_bytes": (CACHE_DIR / f"article_{roman}.html").stat().st_size,
            }
            for roman, meta in ARTICLE_URLS.items()
        },
        "amendment_landing": {
            "guid": AMENDMENT_LANDING_GUID,
            "cached_path": str(CACHE_DIR / "amendment_landing.html"),
            "size_bytes": (CACHE_DIR / "amendment_landing.html").stat().st_size,
        },
    }
    ACQUIRED_INDEX.write_text(json.dumps(index, indent=2))
    log(f"Wrote index: {ACQUIRED_INDEX}")
    log("=== Phase 2.2 acquisition close ===")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except HaltError as e:
        log(f"HALT: {e}")
        sys.exit(2)
