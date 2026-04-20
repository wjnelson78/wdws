"""
Athena Cognitive Engine — Contextual Retrieval
===============================================
Implements Anthropic's Contextual Retrieval technique:
  1. For each document, generate a concise context summary via Claude Sonnet
  2. Prepend that summary to every chunk before embedding
  3. Result: each chunk carries document-level awareness (type, parties, case, subject)

This dramatically improves retrieval accuracy for domain-specific legal/medical text.

Reference: https://www.anthropic.com/news/contextual-retrieval

Usage:
    from contextual_retrieval import generate_context, prepend_context

    # Generate context for a document
    context = await generate_context(title, domain, doc_type, content_preview, case_number)

    # Prepend to each chunk before embedding
    enriched = prepend_context(context, chunk_text)
"""
import asyncio
import json
import logging
import os
import shutil
import time
from typing import Optional

log = logging.getLogger("athena.contextual")

# ── Configuration ────────────────────────────────────────────
CONTEXT_MODEL = os.getenv("CONTEXT_MODEL", "sonnet")
CONTEXT_MAX_PREVIEW = int(os.getenv("CONTEXT_MAX_PREVIEW", "3000"))
CONTEXT_TIMEOUT = int(os.getenv("CONTEXT_TIMEOUT", "60"))

# Claude CLI binary
_CLAUDE_BIN = shutil.which("claude") or "/root/.local/bin/claude"

# System prompt for context generation
_SYSTEM_PROMPT = """You are a document context generator for a legal case management system.
Given a document's metadata and content preview, generate a 2-3 sentence context summary.

The summary MUST capture:
- Document type (email, court filing, medical record, etc.)
- Key parties/entities mentioned (names, roles, organizations)
- Core subject matter
- Relevant case number if provided

This summary will be prepended to every chunk of this document before embedding,
so it must provide enough context to make any individual chunk self-describing.

Output ONLY the 2-3 sentence summary. No preamble, no labels, no formatting."""


async def generate_context(
    title: str,
    domain: str,
    document_type: str,
    content_preview: str,
    case_number: Optional[str] = None,
) -> str:
    """Generate a contextual summary for a document using Claude Sonnet.

    Args:
        title: Document title
        domain: Domain (legal, medical, web, etc.)
        document_type: Type (email, filing, pdf, etc.)
        content_preview: First ~3000 chars of document content
        case_number: Optional linked case number

    Returns:
        2-3 sentence context summary string
    """
    # Build the prompt
    metadata = f"""Document metadata:
- Title: {title or 'Untitled'}
- Domain: {domain or 'unknown'}
- Type: {document_type or 'unknown'}
- Case: {case_number or '(not linked)'}"""

    preview = (content_preview or "")[:CONTEXT_MAX_PREVIEW]
    prompt = f"{metadata}\n\nContent preview:\n{preview}"

    # Call Claude CLI
    env = os.environ.copy()
    env.pop("ANTHROPIC_API_KEY", None)
    env.pop("OPENAI_API_KEY", None)
    env.setdefault("HOME", "/root")

    cmd = [
        _CLAUDE_BIN, "-p",
        "--model", CONTEXT_MODEL,
        "--output-format", "text",
        "--append-system-prompt", _SYSTEM_PROMPT,
    ]

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(input=prompt.encode("utf-8")),
            timeout=CONTEXT_TIMEOUT,
        )

        if proc.returncode != 0:
            err = stderr.decode("utf-8", errors="replace")[:300]
            log.warning("Context generation failed (rc=%d): %s", proc.returncode, err)
            return _fallback_context(title, domain, document_type, case_number)

        context = stdout.decode("utf-8", errors="replace").strip()

        # Sanity check — should be 1-5 sentences, not a full essay
        if len(context) > 1000:
            context = context[:1000].rsplit(".", 1)[0] + "."
        if len(context) < 20:
            return _fallback_context(title, domain, document_type, case_number)

        return context

    except asyncio.TimeoutError:
        log.warning("Context generation timed out for: %s", title[:80])
        return _fallback_context(title, domain, document_type, case_number)
    except Exception as e:
        log.warning("Context generation error for '%s': %s", title[:80], e)
        return _fallback_context(title, domain, document_type, case_number)


def _fallback_context(
    title: str,
    domain: str,
    document_type: str,
    case_number: Optional[str],
) -> str:
    """Generate a basic context string when LLM is unavailable."""
    parts = []
    if document_type:
        parts.append(f"This is a {document_type} document")
    else:
        parts.append("This is a document")

    if domain:
        parts.append(f"in the {domain} domain")

    if title:
        parts.append(f'titled "{title}"')

    if case_number:
        parts.append(f"related to case {case_number}")

    return " ".join(parts) + "."


def generate_context_sync(
    title: str,
    domain: str,
    document_type: str,
    content_preview: str,
    case_number: Optional[str] = None,
) -> str:
    """Synchronous wrapper for generate_context. Uses subprocess directly."""
    import subprocess

    metadata = f"""Document metadata:
- Title: {title or 'Untitled'}
- Domain: {domain or 'unknown'}
- Type: {document_type or 'unknown'}
- Case: {case_number or '(not linked)'}"""

    preview = (content_preview or "")[:CONTEXT_MAX_PREVIEW]
    prompt = f"{metadata}\n\nContent preview:\n{preview}"

    env = os.environ.copy()
    env.pop("ANTHROPIC_API_KEY", None)
    env.pop("OPENAI_API_KEY", None)
    env.setdefault("HOME", "/root")

    cmd = [
        _CLAUDE_BIN, "-p",
        "--model", CONTEXT_MODEL,
        "--output-format", "text",
        "--append-system-prompt", _SYSTEM_PROMPT,
    ]

    try:
        result = subprocess.run(
            cmd,
            input=prompt,
            capture_output=True,
            text=True,
            timeout=CONTEXT_TIMEOUT,
            env=env,
        )

        if result.returncode != 0:
            log.warning("Context generation failed (rc=%d): %s",
                        result.returncode, result.stderr[:300])
            return _fallback_context(title, domain, document_type, case_number)

        context = result.stdout.strip()
        if len(context) > 1000:
            context = context[:1000].rsplit(".", 1)[0] + "."
        if len(context) < 20:
            return _fallback_context(title, domain, document_type, case_number)
        return context

    except subprocess.TimeoutExpired:
        log.warning("Context generation timed out for: %s", title[:80])
        return _fallback_context(title, domain, document_type, case_number)
    except Exception as e:
        log.warning("Context generation error for '%s': %s", title[:80], e)
        return _fallback_context(title, domain, document_type, case_number)


def enrich_chunks(
    context: str,
    chunk_texts: list[str],
) -> list[str]:
    """Prepend context to a list of chunk texts for contextual embedding.

    Args:
        context: Document-level context summary
        chunk_texts: List of raw chunk text strings

    Returns:
        List of enriched texts (context + chunk) ready for embedding
    """
    return [prepend_context(context, t) for t in chunk_texts]


def prepend_context(context: str, chunk_text: str) -> str:
    """Prepend document context to a chunk for contextual embedding.

    Format:
        <context>
        {context summary}
        </context>
        {original chunk text}
    """
    if not context:
        return chunk_text
    return f"<context>\n{context}\n</context>\n{chunk_text}"


async def generate_contexts_batch(
    documents: list[dict],
    concurrency: int = 3,
    callback=None,
) -> dict[str, str]:
    """Generate context summaries for a batch of documents.

    Args:
        documents: List of dicts with keys: id, title, domain, document_type,
                   content_preview, case_number
        concurrency: Max concurrent Claude CLI calls
        callback: async fn(completed, total, doc_title)

    Returns:
        Dict mapping document_id -> context string
    """
    sem = asyncio.Semaphore(concurrency)
    results = {}
    total = len(documents)
    completed = 0

    async def _process(doc):
        nonlocal completed
        async with sem:
            context = await generate_context(
                title=doc.get("title", ""),
                domain=doc.get("domain", ""),
                document_type=doc.get("document_type", ""),
                content_preview=doc.get("content_preview", ""),
                case_number=doc.get("case_number"),
            )
            results[doc["id"]] = context
            completed += 1
            if callback:
                await callback(completed, total, doc.get("title", "")[:60])

    tasks = [asyncio.create_task(_process(doc)) for doc in documents]
    await asyncio.gather(*tasks, return_exceptions=True)
    return results


# ── CLI Test ─────────────────────────────────────────────────

if __name__ == "__main__":
    async def main():
        print("Contextual Retrieval — Test")
        print("=" * 50)

        context = await generate_context(
            title="Re: Motion to Dismiss — Nelson v. BIIA",
            domain="legal",
            document_type="email",
            content_preview="Dear Mr. Nelson, Please find attached the court's ruling on the motion to dismiss. The court has denied the defendant's motion on procedural grounds, noting that the filing deadline had not yet passed.",
            case_number="24-2-01031-31",
        )

        print(f"\nGenerated context:\n{context}\n")

        chunk = "The court denied the motion on procedural grounds, noting that the filing deadline had passed on March 15."
        enriched = prepend_context(context, chunk)
        print(f"Enriched chunk:\n{enriched}")

    asyncio.run(main())
