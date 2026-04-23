# Session notes — 2026-04-22 → 2026-04-23

Overnight work: moved ACE from Falcon-OCR + HF-cloud-embed + inbox-only + rule-less
retention to Sonnet-OCR + local-GPU-embed + full-mailbox + classification-aware
retention. End-to-end tombstone flow validated with a real upstream deletion event.

## What shipped

### OCR: Falcon → Sonnet primary, pdftotext fast path

- New extraction order in `email_sync.py::extract_text_from_binary`:
  `pdftotext → Sonnet → Falcon (fallback) → Tesseract (last resort)`.
- Native-text PDFs skip OCR entirely via `_try_pdftotext_native` — most court
  filings, letters, reports extract in <1s with perfect fidelity.
- `_ocr_via_sonnet` sends scanned PDF pages + images to `claude-sonnet-4-6`.
- Attachment ingest now stamps `metadata.ocr_version='sonnet-v1'` when
  extraction lands via any current-tree method, so subsequent backfills skip
  already-processed rows.

### Sonnet backfill worker

- `sonnet_backfill_worker.py` — promotes pilot sidecars (`claude_sonnet_ocr_v1`)
  to primary `full_content`, or runs fresh extraction through the same tree as
  live ingest. Rechunks, re-embeds, writes `ocr_version='sonnet-v1'`.
- Runs across the existing corpus: 2,147 docs landed at sonnet-v1 primary.
- Noise images (no extractable text, <30 KB, or pattern-named) are marked
  `ocr_version='sonnet-v1-noise'` so they stop cycling as candidates.

### 100-doc Sonnet pilot

- `claude_sonnet_ocr_pilot.py` — stratified sample of 100 PDFs processed
  sidecar-only for A/B comparison vs Falcon.
- Result: 89 ok, 11 "not_a_pdf" (all since repaired). $20.03. 147 min.
- Headline: 496-page Sixth Amended Complaint jumped from 500k → 825k chars
  (+65%), confirming Falcon was truncating on long dense docs.

### Bogus-PDF repair

- `scan_pdf_magic.py` — range-GET first 32 bytes of every `*.pdf` blob,
  classify by magic bytes. Found **217/1,736 (12.5%)** of "PDFs" were actually
  raw email MIME stored under .pdf filename (parent-email bytes leaked into
  attachment slot).
- `repair_eml_as_pdf.py` — walked each MIME, extracted the real PDF
  attachment by filename match, re-uploaded to MinIO in place. **217/217
  recovered; 2,547 bogus chunks deleted.**

### Graph delta worker: full mailbox coverage

- `graph_delta_worker.py::list_all_folders` recursively enumerates every
  non-empty folder via `/users/{mailbox}/mailFolders`, skipping noise
  folders (Drafts, Outbox, Conversation History, Sync Issues, RSS).
- Athena now mirrors **9 folders** (Inbox, Sent Items, Deleted Items,
  Snohomish County Court, efilingmail, postmaster, WDWS, Nina-TRS,
  Conflicts) instead of only inbox/sentitems.
- Tombstone detection (`@removed`) validated end-to-end — 2 real deletions
  landed with `source_deleted_at` stamped.
- `DELTA_INGEST_CONCURRENCY=5` systemd drop-in (was 1, for Falcon-era GPU
  serialization).

### Saturn GPU service: BGE + reranker local

- `/opt/falcon-ocr/embedding_service.py` — FastAPI on port 9098, serves
  BAAI/bge-m3 (embedder) + bge-reranker-v2-m3 (reranker) from saturn's T4.
- Drop-in replacement for HF cloud endpoint: `HF_ENDPOINT_URL` flipped from
  AWS to `http://172.16.81.113:9098`. Same TEI-compatible request/response
  schema, so `embedding_service.py` client changes nothing.
- fp16 inference via `.half()` after load → **2.4× faster** than fp32
  (243 ms/text vs 587 ms/text).
- `asyncio.Semaphore(4)` for bounded concurrency + `torch.cuda.empty_cache()`
  in every finally block. Peaks at ~8 GB on T4, plenty of headroom.
- Reranker is now actually usable (was CPU-fallback only).
- `systemd` unit `bge-embedding.service` enabled.
- Falcon-OCR service stopped + disabled from boot — code path kept as
  fallback chain entry but never called in practice.

### Classification + retention

- `email_classifier.py` (existing, never run) backfilled 9,478 unclassified
  docs: 93% content, 6% transactional, 1% privileged.
- `wdws-classifier.timer` enabled, hourly.
- `retention_sweep.py` policy bug fixed: `unknown` classification now uses
  `source_deleted` clock (was `created_at`), preventing accidental 5-year
  auto-purge of live mailbox content. Re-applied: `retain_until` NULL for
  content/privileged, +1y from `created_at` for transactional.

### Haiku enrichment

- `enrichment_haiku.py` — tool-use `record_enrichment` call to
  claude-haiku-4-5 extracts {summary, parties, case_citations, dates,
  action_items, topics, importance} into `metadata.enrichment_v1`.
- ~9,896 docs enriched this session (~$30 at ~$0.003/doc).

### SMS notifications

- `email_notify.py` hardened:
  - **Recency gate**: `SMS_MAX_AGE_MINUTES=15` — bootstrap/backfill
    re-ingests of historical mail no longer trigger SMS.
  - **Missing received_at** → blocked (strict default).
  - **Body preview**: `_first_two_lines` extracts first 2 non-empty,
    non-quoted lines (skips `>`, `On ... wrote:`, `From:`, `Sent:`).
  - Format: `[ATHENA] Name @ time\nSubject\nbody line 1 · body line 2`.
  - Wire-through: `email_sync::queue_email_sms(body_preview=...)`.

## Corpus state at session end

| Metric | Count |
|--------|-------|
| Total docs | 10,562 |
| At `sonnet-v1` primary | 2,147 |
| `sonnet-v1-noise` (excluded) | 37 |
| Classified | 10,562 (all) |
| Haiku-enriched | 9,896 (all eligible) |
| Athena emails | 1,718 (from 1,084 at start) |
| Source-tombstoned | 2 |

## Cost

| | $ |
|---|---|
| Sonnet pilot (sidecar, 100 docs) | 20.03 |
| Sonnet backfill OCR (~170 calls) | ~8.50 |
| Haiku enrichment (~9,900 docs) | ~30.00 |
| **Session total** | **~$60** |

## What's now automatic

- Every 15 min: `wdws-graph-delta.timer` polls athena + william across all
  folders, tombstones upstream deletions.
- Every 5 min: `wdws-email-sync.timer` runs legacy FS scan (secondary).
- Hourly: `wdws-classifier.timer` classifies any new unclassified docs.
- Per-ingest: pdftotext → Sonnet for scanned → rechunk → embed → DB.

## Deferred

- Systemd unit + timer for `falcon_reprocess_worker.py` (not strictly
  needed since Falcon is disabled, but keeping the code path for
  "Sonnet down" disaster recovery).
- Sprint A privilege/PHI filter work (see `SPRINT_A_WORK_ORDER_v2.2.md`).
- Meeting transcription/summary (the biggest feature gap vs M365 Copilot).

## Known residual

- ~37 image attachments tagged `sonnet-v1-noise` because they have no
  extractable text. These are tiny signature/logo pngs slipping past the
  noise filter on size or name. Not a data quality issue — correctly
  identified as non-content.
- A small number of very large (>5 MB base64) iPhone photos fail Sonnet
  with `image exceeds 5 MB`. Would require pre-resize before upload;
  low priority.
- 2 real tombstones stamped. Retention sweep will log them in the next
  weekly report (whenever Sprint A.T5 lands).
