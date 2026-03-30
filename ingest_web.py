#!/usr/bin/env python3
"""
WDWS Web Domain Ingest — playfasa.com Crawler

Crawls a website, extracts text from HTML pages and PDFs,
chunks, embeds (text-embedding-3-large), and stores in PostgreSQL.

Usage:
    python ingest_web.py                          # crawl all active sites
    python ingest_web.py --site playfasa.com      # crawl specific site
    python ingest_web.py --site playfasa.com --dry-run
    python ingest_web.py --site playfasa.com --max-pages 10
"""

import argparse
import asyncio
import hashlib
import json
import logging
import os
import re
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import asyncpg
import httpx
from bs4 import BeautifulSoup

# ============================================================
# Configuration
# ============================================================

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://wdws:NEL2233obs@127.0.0.1:5432/wdws"
)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY environment variable is required")

EMBEDDING_MODEL = "text-embedding-3-large"
EMBEDDING_DIMENSIONS = 3072
CHUNK_SIZE = 1000
CHUNK_OVERLAP = 200

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("ingest_web")


# ============================================================
# Data classes
# ============================================================

@dataclass
class CrawledPage:
    url: str
    url_path: str
    http_status: int
    content_type: str
    html: str
    title: str
    text_content: str
    content_hash: str
    links: List[str]
    content_length: int
    last_modified: Optional[str] = None


@dataclass
class DocumentChunk:
    content: str
    metadata: Dict
    chunk_index: int
    total_chunks: int


# ============================================================
# Text chunker (same algorithm as ingest.py)
# ============================================================

class TextChunker:
    def __init__(self, chunk_size=CHUNK_SIZE, chunk_overlap=CHUNK_OVERLAP):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.separators = ["\n\n", "\n", ". ", " ", ""]

    def split(self, text: str) -> List[str]:
        if not text:
            return []
        return self._split_text(text, self.separators)

    def _split_text(self, text: str, separators: list) -> list:
        final_chunks = []
        separator = separators[-1]
        new_separators = []
        for i, sep in enumerate(separators):
            if sep == "" or sep in text:
                separator = sep
                new_separators = separators[i + 1:]
                break
        splits = text.split(separator) if separator else list(text)
        good_splits = []
        for s in splits:
            if len(s) < self.chunk_size:
                good_splits.append(s)
            else:
                if good_splits:
                    final_chunks.extend(self._merge_splits(good_splits, separator))
                    good_splits = []
                if new_separators:
                    final_chunks.extend(self._split_text(s, new_separators))
                else:
                    final_chunks.append(s)
        if good_splits:
            final_chunks.extend(self._merge_splits(good_splits, separator))
        return final_chunks

    def _merge_splits(self, splits: list, separator: str) -> list:
        docs = []
        current = []
        total = 0
        for s in splits:
            s_len = len(s)
            if total + s_len + (len(separator) if current else 0) > self.chunk_size:
                if current:
                    docs.append(separator.join(current))
                    while total > self.chunk_overlap and len(current) > 1:
                        total -= len(current[0]) + len(separator)
                        current.pop(0)
                current = [s]
                total = s_len
            else:
                current.append(s)
                total += s_len + (len(separator) if len(current) > 1 else 0)
        if current:
            docs.append(separator.join(current))
        return docs


# ============================================================
# Embedding client (same as ingest.py)
# ============================================================

class EmbeddingClient:
    def __init__(self, api_key: str = OPENAI_API_KEY, model: str = EMBEDDING_MODEL):
        self.api_key = api_key
        self.model = model
        self.client = httpx.AsyncClient(timeout=60.0)
        self.total_tokens = 0
        self.total_requests = 0

    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        if not texts:
            return []
        truncated = [t[:30000] for t in texts]
        resp = await self.client.post(
            "https://api.openai.com/v1/embeddings",
            headers={"Authorization": f"Bearer {self.api_key}"},
            json={"input": truncated, "model": self.model, "dimensions": EMBEDDING_DIMENSIONS},
        )
        resp.raise_for_status()
        data = resp.json()
        self.total_tokens += data.get("usage", {}).get("total_tokens", 0)
        self.total_requests += 1
        return [d["embedding"] for d in data["data"]]

    async def close(self):
        await self.client.aclose()


# ============================================================
# Crawler
# ============================================================

class WebCrawler:
    """Async breadth-first crawler scoped to a single domain."""

    def __init__(self, base_url: str, domain: str, max_depth: int = 3,
                 rate_limit: float = 1.0, include_pdfs: bool = True,
                 max_pages: int = 500):
        self.base_url = base_url.rstrip("/")
        self.domain = domain
        self.max_depth = max_depth
        self.rate_limit = rate_limit
        self.include_pdfs = include_pdfs
        self.max_pages = max_pages
        self.visited: Set[str] = set()
        self.client = httpx.AsyncClient(
            timeout=30.0,
            follow_redirects=True,
            headers={"User-Agent": "WDWS-Crawler/1.0 (internal)"},
        )

    def _normalize_url(self, url: str) -> str:
        """Normalize URL for dedup — strip fragment, lowercase domain."""
        parsed = urlparse(url)
        # Only crawl same domain
        if parsed.netloc and parsed.netloc.lower() != self.domain:
            return ""
        # Rebuild without fragment
        path = parsed.path or "/"
        query = f"?{parsed.query}" if parsed.query else ""
        return f"{self.base_url}{path}{query}"

    def _is_crawlable(self, url: str) -> bool:
        """Filter out non-page resources."""
        parsed = urlparse(url)
        path_lower = parsed.path.lower()
        # Skip images, stylesheets, scripts, etc.
        skip_ext = {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".ico", ".svg",
                    ".css", ".js", ".woff", ".woff2", ".ttf", ".eot",
                    ".mp3", ".mp4", ".avi", ".mov", ".wmv", ".zip", ".rar"}
        ext = os.path.splitext(path_lower)[1]
        if ext in skip_ext:
            return False
        # Allow PDFs if configured
        if ext == ".pdf" and not self.include_pdfs:
            return False
        return True

    def _extract_text(self, html: str) -> Tuple[str, str, List[str]]:
        """Extract title, text content, and links from HTML."""
        soup = BeautifulSoup(html, "html.parser")

        # Remove script/style/nav elements
        for tag in soup(["script", "style", "noscript", "meta", "link"]):
            tag.decompose()

        title = ""
        title_tag = soup.find("title")
        if title_tag:
            title = title_tag.get_text(strip=True)

        # Extract text
        text = soup.get_text(separator="\n", strip=True)
        # Collapse excessive whitespace
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r"[ \t]{2,}", " ", text)

        # Extract links
        links = []
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href and not href.startswith(("#", "mailto:", "tel:", "javascript:")):
                links.append(href)

        return title, text, links

    async def crawl(self) -> List[CrawledPage]:
        """Crawl the site breadth-first, return list of pages."""
        pages: List[CrawledPage] = []
        seen_hashes: Set[str] = set()  # dedup by content hash
        # Queue: (url, depth)
        queue: List[Tuple[str, int]] = [(self.base_url + "/", 0)]
        # Also try /default.asp since this is a classic ASP site
        queue.append((self.base_url + "/default.asp", 0))

        while queue and len(pages) < self.max_pages:
            url, depth = queue.pop(0)
            normalized = self._normalize_url(url)
            if not normalized or normalized in self.visited:
                continue
            if not self._is_crawlable(normalized):
                continue

            self.visited.add(normalized)

            try:
                await asyncio.sleep(self.rate_limit)
                resp = await self.client.get(normalized)
                ct = resp.headers.get("content-type", "")
                last_mod = resp.headers.get("last-modified")

                parsed_path = urlparse(normalized).path or "/"

                if resp.status_code != 200:
                    log.warning(f"  HTTP {resp.status_code}: {normalized}")
                    continue

                if "text/html" in ct or "text/asp" in ct or not ct:
                    html = resp.text
                    title, text, links = self._extract_text(html)

                    if not text or len(text.strip()) < 50:
                        log.debug(f"  Skip (no content): {normalized}")
                        continue

                    content_hash = hashlib.sha256(text.encode()).hexdigest()

                    # Dedup by content hash (e.g. / and /default.asp)
                    if content_hash in seen_hashes:
                        log.debug(f"  Skip (duplicate content): {normalized}")
                        continue
                    seen_hashes.add(content_hash)

                    # Cap huge pages to avoid embedding cost blowup
                    if len(text) > 200000:
                        log.info(f"  Truncating {parsed_path} from {len(text)} to 200000 chars")
                        text = text[:200000]

                    page = CrawledPage(
                        url=normalized,
                        url_path=parsed_path,
                        http_status=resp.status_code,
                        content_type=ct.split(";")[0].strip(),
                        html=html,
                        title=title,
                        text_content=text,
                        content_hash=content_hash,
                        links=links,
                        content_length=len(resp.content),
                        last_modified=last_mod,
                    )
                    pages.append(page)
                    log.info(f"  [{len(pages)}/{self.max_pages}] ✓ {parsed_path}"
                             f"  ({len(text)} chars, {len(links)} links)")

                    # Enqueue discovered links
                    if depth < self.max_depth:
                        for link in links:
                            abs_url = urljoin(normalized, link)
                            queue.append((abs_url, depth + 1))

                elif "application/pdf" in ct and self.include_pdfs:
                    # Store PDF as binary — text extraction is a future enhancement
                    log.info(f"  [{len(pages)}/{self.max_pages}] PDF: {parsed_path}"
                             f"  ({len(resp.content)} bytes)")
                    # For PDFs, we store a placeholder and can OCR later
                    text = f"[PDF document: {parsed_path}]"
                    content_hash = hashlib.sha256(resp.content).hexdigest()
                    page = CrawledPage(
                        url=normalized,
                        url_path=parsed_path,
                        http_status=resp.status_code,
                        content_type="application/pdf",
                        html="",
                        title=os.path.basename(parsed_path),
                        text_content=text,
                        content_hash=content_hash,
                        links=[],
                        content_length=len(resp.content),
                        last_modified=last_mod,
                    )
                    pages.append(page)

            except httpx.TimeoutException:
                log.warning(f"  Timeout: {normalized}")
            except Exception as e:
                log.error(f"  Error crawling {normalized}: {e}")

        await self.client.aclose()
        return pages


# ============================================================
# Database writer
# ============================================================

def _vec_literal(embedding: List[float]) -> str:
    return "[" + ",".join(f"{v:.6f}" for v in embedding) + "]"


async def write_web_document(
    pool: asyncpg.Pool, site_id: str, page: CrawledPage,
    chunks: List[DocumentChunk], embeddings: List[List[float]],
):
    """Write a crawled page + chunks to PostgreSQL."""
    doc_id = uuid.uuid5(uuid.NAMESPACE_URL, page.url)

    async with pool.acquire() as conn:
        async with conn.transaction():
            # 1. core.documents
            await conn.execute("""
                INSERT INTO core.documents
                    (id, domain, source_path, filename, document_type, title,
                     content_hash, total_chunks, full_content, metadata)
                VALUES ($1, 'web', $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
                ON CONFLICT (id) DO UPDATE SET
                    content_hash = EXCLUDED.content_hash,
                    total_chunks = EXCLUDED.total_chunks,
                    full_content = EXCLUDED.full_content,
                    metadata = EXCLUDED.metadata,
                    updated_at = now()
            """,
                doc_id, page.url, os.path.basename(page.url_path) or "index",
                page.content_type.split("/")[-1],  # 'html', 'pdf'
                page.title or page.url_path,
                page.content_hash, len(chunks),
                page.text_content[:500000],
                json.dumps({"url": page.url, "url_path": page.url_path,
                            "link_count": len(page.links)}),
            )

            # 2. core.document_chunks — delete old chunks on re-crawl, then insert
            await conn.execute(
                "DELETE FROM core.document_chunks WHERE document_id = $1", doc_id
            )
            for chunk, emb in zip(chunks, embeddings):
                chunk_id = hashlib.md5(
                    f"{page.url}:{chunk.chunk_index}".encode()
                ).hexdigest()
                await conn.execute("""
                    INSERT INTO core.document_chunks
                        (id, document_id, chunk_index, total_chunks,
                         content, embedding, metadata)
                    VALUES ($1, $2, $3, $4, $5, $6::halfvec, $7::jsonb)
                    ON CONFLICT (id) DO NOTHING
                """,
                    chunk_id, doc_id,
                    chunk.chunk_index, chunk.total_chunks,
                    chunk.content, _vec_literal(emb),
                    json.dumps(chunk.metadata),
                )

            # 3. web.page_metadata
            await conn.execute("""
                INSERT INTO web.page_metadata
                    (document_id, site_id, url, url_path, http_status,
                     content_type, content_length, link_count)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (document_id) DO UPDATE SET
                    http_status = EXCLUDED.http_status,
                    content_type = EXCLUDED.content_type,
                    crawled_at = now(),
                    content_length = EXCLUDED.content_length,
                    link_count = EXCLUDED.link_count
            """,
                doc_id, uuid.UUID(site_id), page.url, page.url_path,
                page.http_status, page.content_type,
                page.content_length, len(page.links),
            )


# ============================================================
# Main pipeline
# ============================================================

async def ingest_site(pool: asyncpg.Pool, embedder: EmbeddingClient,
                      site: dict, max_pages: int, dry_run: bool = False) -> Dict:
    """Crawl and ingest a single website."""
    stats = {"pages": 0, "chunks": 0, "errors": 0, "skipped": 0}
    chunker = TextChunker()

    config = site["crawl_config"] if isinstance(site["crawl_config"], dict) \
        else json.loads(site["crawl_config"])

    log.info(f"Crawling {site['base_url']} (max_depth={config.get('max_depth', 3)}, "
             f"max_pages={max_pages})")

    crawler = WebCrawler(
        base_url=site["base_url"],
        domain=site["domain"],
        max_depth=config.get("max_depth", 3),
        rate_limit=config.get("rate_limit_seconds", 1),
        include_pdfs=config.get("include_pdfs", True),
        max_pages=max_pages,
    )

    pages = await crawler.crawl()
    log.info(f"Crawled {len(pages)} pages from {site['domain']}")

    if dry_run:
        for p in pages:
            log.info(f"  [DRY] {p.url_path} — {len(p.text_content)} chars, {p.title}")
        return stats

    # Get already-ingested URLs for content-hash dedup
    existing = await pool.fetch(
        "SELECT source_path, content_hash FROM core.documents WHERE domain = 'web'"
    )
    existing_hashes = {r["source_path"]: r["content_hash"] for r in existing}

    for page in pages:
        # Skip if content hasn't changed
        if page.url in existing_hashes and existing_hashes[page.url] == page.content_hash:
            log.info(f"  ⊘ Unchanged: {page.url_path}")
            stats["skipped"] += 1
            continue

        try:
            # Chunk
            chunk_texts = chunker.split(page.text_content)
            if not chunk_texts:
                stats["skipped"] += 1
                continue

            chunks = [
                DocumentChunk(
                    content=ct,
                    metadata={"url": page.url, "url_path": page.url_path,
                              "chunk_index": i, "site": site["domain"]},
                    chunk_index=i,
                    total_chunks=len(chunk_texts),
                )
                for i, ct in enumerate(chunk_texts)
            ]

            # Embed
            embeddings = await embedder.embed_batch(chunk_texts)

            # Write
            await write_web_document(pool, str(site["id"]), page, chunks, embeddings)
            stats["pages"] += 1
            stats["chunks"] += len(chunks)
            log.info(f"  ✓ {page.url_path} → {len(chunks)} chunks")

        except Exception as e:
            log.error(f"  ✗ {page.url_path}: {e}")
            stats["errors"] += 1

    return stats


async def main():
    parser = argparse.ArgumentParser(description="WDWS Web Domain Ingest")
    parser.add_argument("--site", type=str, default=None,
                        help="Crawl specific site domain (e.g. playfasa.com)")
    parser.add_argument("--max-pages", type=int, default=500,
                        help="Max pages to crawl per site")
    parser.add_argument("--dry-run", action="store_true",
                        help="Crawl but don't store")
    parser.add_argument("--database-url", type=str, default=None)
    args = parser.parse_args()

    db_url = args.database_url or DATABASE_URL
    pool = await asyncpg.create_pool(db_url, min_size=2, max_size=8)

    # Load sites from web.sites
    if args.site:
        sites = await pool.fetch(
            "SELECT * FROM web.sites WHERE domain = $1 AND is_active = true",
            args.site,
        )
        if not sites:
            log.error(f"Site '{args.site}' not found or not active in web.sites")
            await pool.close()
            sys.exit(1)
    else:
        sites = await pool.fetch("SELECT * FROM web.sites WHERE is_active = true")

    log.info(f"")
    log.info(f"╔{'═'*58}╗")
    log.info(f"║  WDWS Web Domain Ingest{' '*34}║")
    log.info(f"╠{'═'*58}╣")
    log.info(f"║  Sites:      {len(sites):<45}║")
    log.info(f"║  Max pages:  {args.max_pages:<45}║")
    log.info(f"║  Dry run:    {str(args.dry_run):<45}║")
    log.info(f"╚{'═'*58}╝")

    embedder = EmbeddingClient()
    results = {}

    for site in sites:
        site = dict(site)
        log.info(f"")
        log.info(f"{'═'*60}")
        log.info(f"INGESTING: {site['label'] or site['domain']}")
        log.info(f"{'═'*60}")
        t0 = time.time()
        results[site["domain"]] = await ingest_site(
            pool, embedder, site, args.max_pages, args.dry_run,
        )
        elapsed = time.time() - t0

        if not args.dry_run:
            await pool.execute("""
                INSERT INTO ops.ingestion_jobs
                    (domain, source, status, documents_processed, chunks_processed,
                     documents_failed, completed_at, metadata)
                VALUES ('web', $1, 'completed', $2, $3, $4, now(), $5::jsonb)
            """,
                site["domain"],
                results[site["domain"]]["pages"],
                results[site["domain"]]["chunks"],
                results[site["domain"]]["errors"],
                json.dumps({"elapsed_seconds": round(elapsed, 1),
                            "site": site["domain"],
                            **results[site["domain"]]}),
            )
        log.info(f"{site['domain']} completed in {elapsed:.1f}s")

    # Summary
    embed_cost = embedder.total_tokens / 1_000_000 * 0.13
    final_web = await pool.fetchval(
        "SELECT COUNT(*) FROM core.documents WHERE domain = 'web'"
    )

    await embedder.close()
    await pool.close()

    log.info(f"")
    log.info(f"╔{'═'*58}╗")
    log.info(f"║  WEB INGEST COMPLETE{' '*37}║")
    log.info(f"╠{'═'*58}╣")
    for domain, s in results.items():
        log.info(f"║  {domain:<56}║")
        for k, v in s.items():
            log.info(f"║    {k:>14}: {str(v):<40}║")
    log.info(f"╠{'═'*58}╣")
    log.info(f"║  Web docs total: {final_web:<41}║")
    log.info(f"║  Embed tokens:   {embedder.total_tokens:,}{' '*(41-len(f'{embedder.total_tokens:,}'))}║")
    log.info(f"║  Est. cost:      ${embed_cost:.4f}{' '*(40-len(f'${embed_cost:.4f}'))}║")
    log.info(f"╚{'═'*58}╝")


if __name__ == "__main__":
    asyncio.run(main())
