#!/usr/bin/env python3
"""
WAC Phase 2 Acquisition

Two-step acquisition: build coverage map, then fetch chapters at 1 req/sec.

Usage:
    python3 phase2_acquire.py --coverage-map     # fetch 116 title TOCs, build chapter list
    python3 phase2_acquire.py --acquire-chapters # fetch every chapter from coverage map

Halt protocol (operator-locked):
  - 4xx (except 404 — log + skip with warning) → halt immediately, no auto-retry
  - 429 → halt immediately
  - sustained 5xx (≥3 consecutive) → halt immediately
  - missing &full=true content (no <a name='...'> anchors found) → halt
  - ASP.NET ViewState rejection / unexpected redirect → halt
  - presence of HALT_FLAG file → clean shutdown at next iteration

Logs:
  /opt/wdws/wac_research/phase2_acquisition.log     — append-only run log
  /opt/wdws/wac_research/phase2_coverage_map.json   — title→chapters mapping
  /opt/wdws/wac_research/phase2_progress.json       — periodic progress snapshot
  /opt/wdws/wac_research/phase2_HALT.flag           — touch to request graceful halt
  /opt/wdws/wac_research/phase2_HALT_REASON.txt     — written on halt (if non-graceful)

Cache:
  /opt/wdws/data/wac/html/titles/<title>.html       — title TOC HTML
  /opt/wdws/data/wac/html/chapters/<title>/<chapter>.html — chapter &full=true HTML
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import urllib.request
import urllib.error
from urllib.parse import urlencode

# --- configuration -----------------------------------------------------------

UA = "Athena-WAC-Ingest/1.0 (research; will@nelson; pro se litigant assistive tech)"
BASE = "https://app.leg.wa.gov/WAC/default.aspx"
RATE_LIMIT_SEC = 1.0  # honest 1 req/sec to leg.wa.gov
TIMEOUT_SEC = 30
MAX_CONSECUTIVE_5XX = 3
PROGRESS_FLUSH_EVERY = 25  # write progress JSON every N fetches

CACHE_DIR = Path("/opt/wdws/data/wac/html")
RESEARCH_DIR = Path("/opt/wdws/wac_research")
LOG_PATH = RESEARCH_DIR / "phase2_acquisition.log"
COVERAGE_MAP_PATH = RESEARCH_DIR / "phase2_coverage_map.json"
PROGRESS_PATH = RESEARCH_DIR / "phase2_progress.json"
HALT_FLAG = RESEARCH_DIR / "phase2_HALT.flag"
HALT_REASON = RESEARCH_DIR / "phase2_HALT_REASON.txt"


# --- logging -----------------------------------------------------------------

def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with LOG_PATH.open("a") as f:
        f.write(line + "\n")


def halt(reason: str, code: int = 2) -> None:
    log(f"HALT: {reason}")
    HALT_REASON.write_text(reason + "\n")
    sys.exit(code)


def check_halt_flag() -> None:
    if HALT_FLAG.exists():
        halt("HALT_FLAG file present (graceful halt requested by operator)", code=0)


# --- HTTP --------------------------------------------------------------------

def fetch(url: str, *, expect_size_min: int = 1000) -> tuple[int, bytes]:
    """Fetch URL; return (status, body). Raise on transport errors."""
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_SEC) as resp:
            body = resp.read()
            return resp.status, body
    except urllib.error.HTTPError as e:
        body = e.read() if e.fp else b""
        return e.code, body
    except urllib.error.URLError as e:
        halt(f"Transport error fetching {url}: {e}")
    except Exception as e:
        halt(f"Unexpected error fetching {url}: {e}")


def fetch_with_halt_protocol(url: str, *, expect_size_min: int = 1000) -> bytes:
    """Fetch with halt-on-anomaly protocol."""
    check_halt_flag()
    status, body = fetch(url)

    # 429 / 4xx (except 404) / 5xx halt classes
    if status == 429:
        halt(f"HTTP 429 from {url} — rate-limited; halt immediately, no auto-retry")
    if status == 404:
        log(f"WARNING: 404 from {url} — logging and skipping (citation may not exist)")
        return b""
    if 400 <= status < 500:
        halt(f"HTTP {status} from {url} — client error; halt immediately, body={body[:200]!r}")
    if 500 <= status < 600:
        halt(f"HTTP {status} from {url} — server error; halt immediately (no auto-retry)")
    if status != 200:
        halt(f"HTTP {status} unexpected from {url}")

    # Size sanity check (very small responses likely indicate redirect or error page)
    if len(body) < expect_size_min:
        halt(f"Response from {url} is {len(body)} bytes < min {expect_size_min}; possible redirect/error")

    return body


# --- coverage map ------------------------------------------------------------

LANDING_PAGE_URL = "https://app.leg.wa.gov/wac/"


def fetch_landing_page() -> bytes:
    """Fetch the WAC landing page; cache to disk."""
    cache_path = CACHE_DIR / "titles" / "_LANDING.html"
    if cache_path.exists() and cache_path.stat().st_size > 5000:
        log("  cached: WAC landing page")
        return cache_path.read_bytes()
    log(f"  fetch:  WAC landing page ({LANDING_PAGE_URL})")
    body = fetch_with_halt_protocol(LANDING_PAGE_URL, expect_size_min=5000)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(body)
    time.sleep(RATE_LIMIT_SEC)
    return body


def discover_titles_from_landing_page(html: bytes) -> list[str]:
    """Parse the WAC landing page to extract the canonical title list.

    The landing page links to each title via cite=N (e.g., cite=1, cite=132A,
    cite=296, cite=132T, cite=516). Lettered variants are separate links; some
    bare numeric slots (e.g., 132) have no link because only their lettered
    variants are populated.
    """
    text = html.decode("utf-8", errors="replace")
    # cite=NNN or cite=NNNX where X is a single uppercase letter (132A, 132T, etc.)
    pat = re.compile(r"cite=(\d+[A-Za-z]?)(?:[\"'&>])", flags=re.IGNORECASE)
    found = set(m.group(1).upper() for m in pat.finditer(text))
    # Sort: numeric prefix ascending, lettered variants alphabetically after bare
    def sort_key(t: str) -> tuple[int, str]:
        m = re.match(r"(\d+)([A-Z]?)", t)
        return (int(m.group(1)), m.group(2))
    return sorted(found, key=sort_key)


def fetch_title_toc(title: str) -> bytes:
    url = f"{BASE}?{urlencode({'cite': title})}"
    cache_path = CACHE_DIR / "titles" / f"{title}.html"
    if cache_path.exists() and cache_path.stat().st_size > 1000:
        log(f"  cached: title {title}")
        return cache_path.read_bytes()

    log(f"  fetch:  title {title}")
    body = fetch_with_halt_protocol(url, expect_size_min=500)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(body)
    time.sleep(RATE_LIMIT_SEC)
    return body


def parse_chapter_list(title: str, html: bytes) -> list[str]:
    """Extract chapter numbers (T-CC form) from a title TOC page."""
    text = html.decode("utf-8", errors="replace")
    # Anchor pattern: cite=T-CC where CC may be numeric+letter.
    # Title number may also have letters (132A → cite=132A-NN).
    tre = re.escape(title)
    pat = re.compile(
        rf"cite={tre}-(\d+[A-Za-z]?)",
        flags=re.IGNORECASE,
    )
    chapters = sorted(set(pat.findall(text)), key=lambda c: (
        int(re.match(r"\d+", c).group(0)),
        c,
    ))
    if not chapters:
        # Title page exists but yields no chapters — possible "Citation not found"
        # marker (e.g., bare 132 returns the not-found page).
        if b"Citation you requested cannot be found" in html or b"cannot be found" in html:
            log(f"  WARN: title {title} returns 'Citation not found' — title is empty/lettered-only")
            return []
        log(f"  WARN: title {title} TOC parsed 0 chapters — unexpected; surfacing")
        # Don't halt — possible parser issue; let coverage map flag and operator review
        return []
    return chapters


def build_coverage_map() -> dict:
    log("--- Phase 2 coverage-map step starting ---")

    # Step 1: fetch landing page and discover canonical title list
    landing_html = fetch_landing_page()
    titles = discover_titles_from_landing_page(landing_html)
    log(f"Discovered {len(titles)} populated WAC titles from landing page")
    # Sanity range [180, 280] baselined to 228 (programmatic enumeration
    # 2026-04-30 supersedes Phase 0's LLM-summarized count of 116). ~25%
    # headroom in both directions.
    if len(titles) < 180 or len(titles) > 280:
        halt(f"Discovered title count {len(titles)} is outside sanity range [180, 280] — halt and review")

    # Step 2: fetch each title's TOC and parse chapter list
    coverage: dict[str, list[str]] = {}
    bad_titles: list[str] = []
    total_chapters = 0

    started = time.monotonic()
    for i, title in enumerate(titles, start=1):
        check_halt_flag()
        try:
            html = fetch_title_toc(title)
        except SystemExit:
            raise
        except Exception as e:
            halt(f"Unhandled exception during title {title}: {e}")

        chapters = parse_chapter_list(title, html)
        coverage[title] = chapters
        total_chapters += len(chapters)
        if not chapters:
            bad_titles.append(title)

        if i % 25 == 0 or i == len(titles):
            elapsed = time.monotonic() - started
            rate = i / elapsed if elapsed else 0
            log(f"  progress: {i}/{len(titles)} titles ({rate:.2f} req/sec); chapters so far: {total_chapters}")

    elapsed = time.monotonic() - started
    log(f"--- Coverage map COMPLETE in {elapsed:.0f}s. Total chapters: {total_chapters}")
    if bad_titles:
        log(f"  Empty/non-existent titles ({len(bad_titles)}): {bad_titles}")

    map_data = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "title_count": len(titles),
        "total_chapters": total_chapters,
        "empty_titles": bad_titles,
        "elapsed_seconds": elapsed,
        "titles": coverage,
    }
    COVERAGE_MAP_PATH.write_text(json.dumps(map_data, indent=2))
    log(f"  Written: {COVERAGE_MAP_PATH}")
    return map_data


# --- chapter acquisition -----------------------------------------------------

def fetch_chapter_full(title: str, chapter: str) -> bytes:
    """Fetch one chapter's &full=true HTML; cache to disk."""
    cite = f"{title}-{chapter}"
    url = f"{BASE}?{urlencode({'cite': cite, 'full': 'true'})}"
    cache_path = CACHE_DIR / "chapters" / title / f"{chapter}.html"

    if cache_path.exists() and cache_path.stat().st_size > 5000:
        return cache_path.read_bytes()

    body = fetch_with_halt_protocol(url, expect_size_min=2000)

    # Structural check: &full=true page MUST contain at least one section anchor.
    # Empty chapters are possible (rare); but ZERO `<a name=` anchors in a
    # chapter we expect to have sections is a structural anomaly → halt.
    text = body.decode("utf-8", errors="replace")
    has_section_anchor = bool(re.search(rf"<a\s+name=['\"]?{re.escape(cite)}-", text))
    if not has_section_anchor:
        # Could be a chapter that exists but has only repealed sections in its
        # disposition table (no active sections). Check for disposition markers.
        has_dispo_marker = (
            "Repealed by" in text or
            "Recodified as" in text or
            "decodified" in text or
            "Reserved" in text
        )
        if not has_dispo_marker:
            halt(
                f"Chapter {cite} &full=true returned no section anchors and no "
                f"disposition markers — structural anomaly. body[:500]={text[:500]!r}"
            )
        log(f"  WARN: {cite} has no active sections (disposition-only chapter)")

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(body)
    time.sleep(RATE_LIMIT_SEC)
    return body


def write_progress(progress: dict) -> None:
    progress["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    PROGRESS_PATH.write_text(json.dumps(progress, indent=2))


def acquire_chapters() -> None:
    if not COVERAGE_MAP_PATH.exists():
        halt("Coverage map missing; run --coverage-map first")

    map_data = json.loads(COVERAGE_MAP_PATH.read_text())
    titles = map_data["titles"]
    total = sum(len(chs) for chs in titles.values())
    log(f"--- Phase 2 chapter-acquisition step starting ---")
    log(f"Total chapters to fetch: {total}")

    started = time.monotonic()
    fetched = 0
    cached = 0
    warnings = 0
    last_checkpoint_pct = 0
    checkpoint_pcts = [25, 50, 75, 100]

    progress = {
        "phase": "acquire-chapters",
        "started_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "total_chapters": total,
        "fetched": 0,
        "cached_hits": 0,
        "warnings": 0,
        "last_chapter": None,
    }
    write_progress(progress)

    for title, chapters in titles.items():
        for chapter in chapters:
            check_halt_flag()
            cite = f"{title}-{chapter}"
            cache_path = CACHE_DIR / "chapters" / title / f"{chapter}.html"
            was_cached = cache_path.exists() and cache_path.stat().st_size > 5000

            try:
                fetch_chapter_full(title, chapter)
            except SystemExit:
                raise
            except Exception as e:
                halt(f"Unhandled exception fetching {cite}: {e}")

            if was_cached:
                cached += 1
            else:
                fetched += 1

            done = fetched + cached
            pct = (done * 100) // total
            progress["fetched"] = fetched
            progress["cached_hits"] = cached
            progress["last_chapter"] = cite
            if done % PROGRESS_FLUSH_EVERY == 0 or done == total:
                write_progress(progress)

            for cp in checkpoint_pcts:
                if pct >= cp and last_checkpoint_pct < cp:
                    elapsed = time.monotonic() - started
                    rate = (done / elapsed) if elapsed else 0
                    eta = ((total - done) / rate) if rate > 0 else 0
                    log(
                        f"  CHECKPOINT {cp}% — {done}/{total} chapters "
                        f"(fetched={fetched}, cached={cached}); "
                        f"rate={rate:.2f}/sec; "
                        f"ETA {eta/60:.1f}m"
                    )
                    last_checkpoint_pct = cp

    elapsed = time.monotonic() - started
    log(f"--- Chapter acquisition COMPLETE in {elapsed:.0f}s ({elapsed/60:.1f}m) ---")
    log(f"  Total: {fetched + cached} chapters (fetched={fetched}, cached={cached})")
    progress["status"] = "completed"
    progress["completed_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    progress["elapsed_seconds"] = elapsed
    write_progress(progress)


# --- main --------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--coverage-map", action="store_true", help="Fetch title TOCs and build coverage map")
    ap.add_argument("--acquire-chapters", action="store_true", help="Fetch every chapter listed in coverage map")
    args = ap.parse_args()

    if not (args.coverage_map or args.acquire_chapters):
        ap.error("--coverage-map or --acquire-chapters required")

    # Defensive: any prior HALT_REASON should be cleared by operator before
    # re-running (we don't auto-clear).
    if HALT_REASON.exists() and HALT_REASON.stat().st_size > 0:
        log(f"WARNING: prior HALT_REASON exists at {HALT_REASON}: "
            f"{HALT_REASON.read_text().strip()!r}. Continuing anyway "
            f"(operator should clear before retry if intent is fresh start).")

    if args.coverage_map:
        build_coverage_map()
    if args.acquire_chapters:
        acquire_chapters()


if __name__ == "__main__":
    main()
