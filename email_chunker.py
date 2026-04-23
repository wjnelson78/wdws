"""
Email-aware chunker for RAG ingestion.
=======================================
Replaces the generic recursive-character TextChunker in email_sync.py and
email_sync_chat.py. Everything non-email (medical PDFs, court filings, web
pages, OneDrive) keeps its own TextChunker implementations — those have no
quoted-reply or signature structure to exploit.

What this module does that the generic chunker cannot:

1. EmailPreprocessor splits a raw email body into typed parts:
   - body       : the new content from this sender (the only part we embed)
   - quoted     : replied-to history (parent messages — already indexed as
                  their own rows; re-embedding them inflates the index and
                  pollutes retrieval with 19 copies of the same "On <date>
                  X wrote:" block in a 20-message thread)
   - signature  : sigdash / closer-plus-name block
   - disclaimer : confidentiality notice / attorney-client boilerplate

2. EmailAwareChunker runs the preprocessor first, chunks each part with the
   same recursive-character algorithm as the old TextChunker, and returns
   ChunkResult objects carrying per-chunk metadata (source_part, is_embedded)
   so the caller can:
     - skip the embedding call for non-body parts, and
     - write source_part into core.document_chunks.metadata for scoped
       retrieval filters ("body only", "include attachment text", etc.).

3. split_attachment() treats attachment text as a single 'attachment' part
   with is_embedded=True — no preprocessing, just chunking with metadata.

4. split() (no metadata) is preserved as a generic-text fallback so any
   caller that only needs the legacy list-of-strings interface still works.

Typical usage from email_sync.py:

    from email_chunker import EmailAwareChunker

    chunker = EmailAwareChunker()

    # Email body
    results = chunker.split_email_body(parsed["full_text"])
    for r in results:
        if not r.is_embedded:
            continue        # skip quoted/signature/disclaimer chunks
        embed_and_insert(r.text, metadata={"source_part": r.source_part, ...})

    # Attachment
    results = chunker.split_attachment(extracted_text, fname, ctype)
    for r in results:
        embed_and_insert(r.text, metadata={"source_part": "attachment",
                                           "attachment_filename": fname, ...})
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


# ============================================================
# Part types
# ============================================================

class PartType(str, Enum):
    BODY = "body"
    QUOTED = "quoted"
    SIGNATURE = "signature"
    DISCLAIMER = "disclaimer"
    ATTACHMENT = "attachment"
    GENERIC = "generic"


@dataclass
class EmailPart:
    part_type: PartType
    text: str
    start_offset: int  # character offset into original body — useful for debugging


@dataclass
class ChunkResult:
    text: str
    source_part: PartType
    is_embedded: bool
    extra: dict = field(default_factory=dict)


# ============================================================
# Detectors — quoted-reply, signature, disclaimer
# ============================================================
#
# Order matters: the preprocessor scans for the EARLIEST match of each
# category, then slices the body into regions. False positives here cause
# text to be mislabeled (and potentially skipped from embedding), so each
# pattern is kept deliberately conservative.

# Quoted-reply markers — various mail client conventions.
# All anchored to start-of-line; we search with re.MULTILINE.
_QUOTE_PATTERNS = [
    # "On Mon, Jan 1, 2026 at 10:00 AM, Jane Smith <jane@example.com> wrote:"
    # Also handles localized variants by keeping "wrote:" in English — Outlook
    # on the receiving side typically rewrites these.
    re.compile(r"^On\s.{1,200}\s+wrote:\s*$", re.MULTILINE | re.IGNORECASE),

    # Outlook reply block. Sent/Received are both common; Graph's text conversion
    # emits "From:\nSent:\nTo:\nSubject:" as consecutive header-style lines.
    re.compile(
        r"^(From|De|Von|Desde|Da)\s*:\s*.{1,500}\n\s*(Sent|Envoyé|Gesendet|Enviado|Inviato|Data)\s*:",
        re.MULTILINE | re.IGNORECASE,
    ),

    # Explicit separator — some clients insert this before the quoted chain.
    re.compile(r"^-{5,}\s*Original Message\s*-{5,}\s*$", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^-{5,}\s*Forwarded message\s*-{5,}\s*$", re.MULTILINE | re.IGNORECASE),

    # Outlook HTML-stripped ruler (appears as a long underscore run).
    re.compile(r"^_{20,}\s*$", re.MULTILINE),
]

# ">"-prefix quoting: handled separately because it's a run of lines, not a
# single marker. We look for the first line starting with "> " that has at
# least 3 consecutive quoted lines after it (to avoid matching stray
# ">"-arrows in prose like "-> see note").
_QUOTE_PREFIX_LINE = re.compile(r"^>\s?", re.MULTILINE)

# Signature detectors.
# RFC 3676 sigdash: two hyphens, space, newline. The space is required by the
# spec but commonly omitted in practice, so we accept either.
_SIG_DASH = re.compile(r"^-- ?\s*$", re.MULTILINE)

# Closer-plus-name heuristic: "Best regards,\n<Name>" at end of body region.
# Very conservative — only fires on a small set of well-known closers.
_CLOSERS = (
    r"Best(\s+regards)?|Sincerely|Regards|Thanks|Thank\s+you|"
    r"Cheers|Kind\s+regards|Warm\s+regards|Respectfully"
)
_CLOSER_PATTERN = re.compile(
    rf"^(?:{_CLOSERS})[,\.]?\s*$",
    re.MULTILINE | re.IGNORECASE,
)

# Disclaimer markers — legal/confidentiality footers. These run to end-of-body
# (or until the quoted block starts).
_DISCLAIMER_MARKERS = (
    "CONFIDENTIALITY NOTICE",
    "CONFIDENTIAL COMMUNICATION",
    "PRIVILEGED AND CONFIDENTIAL",
    "ATTORNEY-CLIENT PRIVILEGED",
    "ATTORNEY CLIENT PRIVILEGED",
    "ATTORNEY WORK PRODUCT",
    "IRS CIRCULAR 230",
    "This email and any attachments",
    "This message and any attachments",
    "The information contained in this",
    "This e-mail and any files transmitted",
    "If you are not the intended recipient",
    "NOTICE OF CONFIDENTIALITY",
)
_DISCLAIMER_PATTERN = re.compile(
    "|".join(re.escape(m) for m in _DISCLAIMER_MARKERS),
    re.IGNORECASE,
)


# ============================================================
# EmailPreprocessor
# ============================================================

class EmailPreprocessor:
    """Segment an email body into body/quoted/signature/disclaimer parts.

    Algorithm:
      1. Find the earliest quoted-reply marker (`quote_start`).
      2. In the pre-quoted region, find the signature start.
      3. In the pre-signature region, find the disclaimer start.
      4. Slice into parts in order: body, (signature|disclaimer), quoted.

    Unknown structure gracefully degrades to a single BODY part — that's
    why this preprocessor is safe to apply to non-email text (though
    EmailAwareChunker.split() skips preprocessing entirely for generic text).
    """

    def __init__(self, min_quote_run: int = 3):
        # Minimum consecutive ">"-prefixed lines to count as a quoted block.
        # Prevents false-positives on ">" used in prose ("-> see note").
        self.min_quote_run = min_quote_run

    def split(self, body: str) -> list[EmailPart]:
        if not body or not body.strip():
            return []

        quote_start = self._find_quote_start(body)
        pre_quote = body[:quote_start] if quote_start is not None else body

        disclaimer_start_rel = self._find_disclaimer_start(pre_quote)
        pre_disclaimer = (
            pre_quote[:disclaimer_start_rel]
            if disclaimer_start_rel is not None else pre_quote
        )

        sig_start_rel = self._find_signature_start(pre_disclaimer)

        parts: list[EmailPart] = []

        # BODY
        body_end = sig_start_rel if sig_start_rel is not None else len(pre_disclaimer)
        body_text = pre_disclaimer[:body_end].strip()
        if body_text:
            parts.append(EmailPart(PartType.BODY, body_text, 0))

        # SIGNATURE
        if sig_start_rel is not None:
            sig_text = pre_disclaimer[sig_start_rel:].strip()
            if sig_text:
                parts.append(EmailPart(PartType.SIGNATURE, sig_text, sig_start_rel))

        # DISCLAIMER
        if disclaimer_start_rel is not None:
            disc_text = pre_quote[disclaimer_start_rel:].strip()
            if disc_text:
                parts.append(
                    EmailPart(PartType.DISCLAIMER, disc_text, disclaimer_start_rel)
                )

        # QUOTED
        if quote_start is not None:
            quoted_text = body[quote_start:].strip()
            if quoted_text:
                parts.append(EmailPart(PartType.QUOTED, quoted_text, quote_start))

        # If nothing matched (single BODY only), that's the common case for
        # short new emails — already handled above.
        return parts

    # ── helpers ──────────────────────────────────────────────────

    def _find_quote_start(self, text: str) -> Optional[int]:
        """Return byte-offset of the first quoted-reply marker, or None."""
        earliest: Optional[int] = None
        for pat in _QUOTE_PATTERNS:
            m = pat.search(text)
            if m and (earliest is None or m.start() < earliest):
                earliest = m.start()

        # ">"-prefix quoting: find earliest line-start where >= min_quote_run
        # consecutive lines begin with "> ".
        prefix_start = self._find_quote_prefix_run(text)
        if prefix_start is not None and (earliest is None or prefix_start < earliest):
            earliest = prefix_start

        return earliest

    def _find_quote_prefix_run(self, text: str) -> Optional[int]:
        lines = text.splitlines(keepends=True)
        offset = 0
        run_start_offset: Optional[int] = None
        run_len = 0
        for line in lines:
            if _QUOTE_PREFIX_LINE.match(line):
                if run_start_offset is None:
                    run_start_offset = offset
                run_len += 1
                if run_len >= self.min_quote_run:
                    return run_start_offset
            else:
                run_start_offset = None
                run_len = 0
            offset += len(line)
        return None

    def _find_signature_start(self, text: str) -> Optional[int]:
        """Return offset of signature start, or None."""
        # Prefer sigdash (RFC 3676) — unambiguous.
        m = _SIG_DASH.search(text)
        if m:
            return m.start()

        # Fall back to closer-plus-short-trailing-block heuristic. Only fire
        # if the closer is near the end of the text (within the last 500 chars)
        # to avoid mislabeling inline "Thanks," acknowledgements.
        tail_window = max(0, len(text) - 500)
        m = _CLOSER_PATTERN.search(text, tail_window)
        if m and (len(text) - m.start()) < 400:
            return m.start()

        return None

    def _find_disclaimer_start(self, text: str) -> Optional[int]:
        m = _DISCLAIMER_PATTERN.search(text)
        if not m:
            return None
        # Back up to the start of the line containing the marker — disclaimers
        # usually start mid-sentence-capitalized or after a blank line.
        line_start = text.rfind("\n", 0, m.start())
        return line_start + 1 if line_start >= 0 else 0


# ============================================================
# EmailAwareChunker
# ============================================================

class EmailAwareChunker:
    """Drop-in replacement for the old TextChunker in email ingest paths.

    Exposes three methods:
      * split(text)              — legacy flat-list interface (no preprocessing)
      * split_email_body(text)   — structure-aware body splitting
      * split_attachment(text, filename, content_type) — attachment chunking
    """

    DEFAULT_CHUNK_SIZE = 1000
    DEFAULT_CHUNK_OVERLAP = 200

    def __init__(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        chunk_overlap: int = DEFAULT_CHUNK_OVERLAP,
    ):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.separators = ["\n\n", "\n", ". ", " ", ""]
        self.preprocessor = EmailPreprocessor()

    # ── public API ───────────────────────────────────────────────

    def split(self, text: str) -> list[str]:
        """Legacy: flat list of chunk strings. No email preprocessing.

        Kept so any caller using the bare .split() interface (including
        non-email code paths that were imported the old TextChunker by
        accident) keeps working unchanged.
        """
        if not text:
            return []
        return self._recursive_split(text, self.separators)

    def split_email_body(self, body_text: str) -> list[ChunkResult]:
        """Structure-aware body splitting.

        Returns ChunkResult for every chunk, including non-body parts.
        Callers MUST check `is_embedded` before embedding: quoted/signature/
        disclaimer chunks are returned so the caller can still write them to
        the DB (or drop them) but should not waste embedder cycles.
        """
        if not body_text or not body_text.strip():
            return []

        parts = self.preprocessor.split(body_text)
        if not parts:
            return []

        results: list[ChunkResult] = []
        for part in parts:
            chunks = self._recursive_split(part.text, self.separators)
            is_embed = part.part_type == PartType.BODY
            for chunk in chunks:
                if not chunk.strip():
                    continue
                results.append(ChunkResult(
                    text=chunk,
                    source_part=part.part_type,
                    is_embedded=is_embed,
                ))
        return results

    def split_attachment(
        self,
        text: str,
        filename: str = "",
        content_type: str = "",
    ) -> list[ChunkResult]:
        """Chunk attachment-extracted text. Always is_embedded=True."""
        if not text or not text.strip():
            return []
        chunks = self._recursive_split(text, self.separators)
        return [
            ChunkResult(
                text=c,
                source_part=PartType.ATTACHMENT,
                is_embedded=True,
                extra={"filename": filename, "content_type": content_type},
            )
            for c in chunks
            if c.strip()
        ]

    # ── generic recursive-character splitter (same algo as old TextChunker) ──

    def _recursive_split(self, text: str, separators: list) -> list[str]:
        if not text:
            return []
        final_chunks: list[str] = []
        separator = separators[-1]
        new_separators: list = []
        for i, sep in enumerate(separators):
            if sep == "" or sep in text:
                separator = sep
                new_separators = separators[i + 1:]
                break
        splits = text.split(separator) if separator else list(text)
        good_splits: list[str] = []
        for s in splits:
            if len(s) < self.chunk_size:
                good_splits.append(s)
            else:
                if good_splits:
                    final_chunks.extend(self._merge(good_splits, separator))
                    good_splits = []
                if new_separators:
                    final_chunks.extend(self._recursive_split(s, new_separators))
                else:
                    final_chunks.append(s)
        if good_splits:
            final_chunks.extend(self._merge(good_splits, separator))
        return final_chunks

    def _merge(self, splits: list, separator: str) -> list[str]:
        docs: list[str] = []
        current: list[str] = []
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


# Back-compat alias: old name still works if anything imports it directly.
TextChunker = EmailAwareChunker


# ============================================================
# CLI test harness
# ============================================================

if __name__ == "__main__":
    sample_outlook = """Hi Bill,

The mediation date is confirmed for May 3.

Thanks,
Sarah Jennings
Partner, Smith & Associates

CONFIDENTIALITY NOTICE: This email and any attachments are confidential.

From: Bill Nelson <wjnelson78@gmail.com>
Sent: Monday, April 21, 2026 3:14 PM
To: Sarah Jennings <sjennings@example.com>
Subject: RE: Mediation scheduling

Sarah — can we shift to the week of May 3?

Bill
"""

    sample_gmail = """Got it. I'll have the draft by Friday.

--
Bill Nelson
206-555-0100

On Mon, Apr 20, 2026 at 11:42 AM, Jane Doe <jane@example.com> wrote:
> Could you send the revised motion?
> Thanks.
"""

    sample_prefix_only = """Yes, confirmed.

> Could you send the revised motion?
> I'm prepping for Thursday.
> Thanks.
"""

    pp = EmailPreprocessor()
    for name, text in [
        ("Outlook with disclaimer + quoted", sample_outlook),
        ("Gmail with sigdash + wrote-quoted", sample_gmail),
        ("Prefix-only quoted", sample_prefix_only),
    ]:
        print(f"\n── {name} ──")
        for part in pp.split(text):
            preview = part.text.replace("\n", " ⏎ ")[:80]
            print(f"  [{part.part_type.value:10s}] {preview}")

    ch = EmailAwareChunker()
    print("\n── split_email_body on Outlook sample ──")
    for r in ch.split_email_body(sample_outlook):
        print(f"  [{r.source_part.value:10s} embed={r.is_embedded}] "
              f"{r.text[:60]!r}")
