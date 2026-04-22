# Purview Decrypt — RMS/AIP Content Pipeline

Decrypts Microsoft Rights-Managed content (`.rpmsg` emails, AIP-protected PDFs,
IRM-protected Office docs) and feeds the plaintext into Athena's ingest pipeline.

Built April 21, 2026. Covers:

- `message.rpmsg` / `message_v2.rpmsg` — Outlook "Do Not Forward" / Encrypt-Only / custom IRM-encrypted emails
- PDFs with Azure Information Protection (sensitivity labels / encrypted)
- Office docs with embedded IRM (`.docx`/`.xlsx`/`.pptx`)
- `.pfile` generic protected containers

Decryption is performed server-side on Debian 13 (no Outlook / Windows required),
using the official **Microsoft Information Protection SDK for Linux** via its
Ubuntu 22.04 NuGet distribution (runs under `.NET 8` on Debian).

---

## Architecture

```
             Email ingest              Purview decrypt pipeline                  Athena index
  ─────────────────────────    ─────────────────────────────────────    ──────────────────────────
  email_sync.py (Graph API)
  ├── pulls mail inbox          .rpmsg found on disk
  ├── extracts attachments ───► ┌───────────────────────────────────┐
  │      (.rpmsg saved raw)     │ purview-decrypt (.NET CLI)        │
  └── writes .eml + folder      │   • MIP SDK File API              │
                                │   • Azure RMS client creds OR     │   body + attachments
                                │     delegated az CLI token        │◄─(decrypted HTML +
                                │   • InspectAsync for .rpmsg       │   PDFs + images)
                                │   • GetDecryptedTemporaryFileAsync│
                                │     for AIP PDFs / Office files   │
                                └─────────────┬─────────────────────┘
                                              │
                                              ▼
                                ┌───────────────────────────────────┐
                                │ ingest_rpmsg_decrypted.py         │
                                │   • chunk + BGE-M3 embed          │
                                │   • contextual retrieval          │
                                │   • domain/record_type inference  │───► core.documents
                                │   • provenance metadata           │     core.document_chunks
                                │     (parent_rpmsg_path, pl_owner) │     medical.record_metadata
                                └───────────────────────────────────┘
```

**Two-layer decryption.** Microsoft commonly nests protection: an `.rpmsg`
wrapper that contains AIP-protected PDFs as attachments. The tool transparently
handles both layers — `InspectAsync` first (for `.rpmsg`/`.msg`), falling back
to `GetDecryptedTemporaryFileAsync` for any file type the File API recognizes
as protected but doesn't return a msg inspector for.

**Inline integration with the main attachment pipeline.** `ingest_attachments.py`'s
OCR worker (`_ocr_attachment_worker`) now detects RMS-protected payloads (by
content-type, extension, or the `76 e8 04 60 c4 11 e3 86 a0 0f 00 00 00 10 00 00`
magic) and routes them through `_extract_rpmsg_text()`, which calls the .NET
decrypt CLI via `purview_decrypt.py` and returns `[body text]` + `[attachment text]`
concatenated for normal chunk+embed. Any `.rpmsg` arriving via the normal
`email_sync` → `attach-ingest` flow is therefore decrypted *transparently* on
its first OCR pass — no separate timer or retroactive batch required.

---

## Files

| Path | Purpose |
|------|---------|
| [purview-decrypt/](purview-decrypt/) | .NET 8 console app (`Program.cs`, `purview-decrypt.csproj`) |
| [purview-decrypt/bin/publish/](purview-decrypt/bin/publish/) | Published `purview-decrypt.dll` + MIP SDK `libmip_*.so` native libs |
| [purview_decrypt.py](purview_decrypt.py) | Python wrapper that shells into the .NET CLI and returns structured JSON |
| [batch_decrypt_rpmsg.py](batch_decrypt_rpmsg.py) | Walks all `.rpmsg` on disk, decrypts each, writes `<stem>.decrypted.html` + `.decrypted.att##_*` siblings, emits a CSV report |
| [ingest_rpmsg_decrypted.py](ingest_rpmsg_decrypted.py) | Reads the CSV report + any Workwell CPET tree, chunks + embeds + inserts into `core.documents` with provenance metadata |
| `/opt/dotnet/` | .NET 8 SDK (installed via `dot.net/v1/dotnet-install.sh`) |
| `~/.nuget/packages/microsoft.informationprotection.file.ubuntu2204/` | MIP SDK native Linux binaries |
| `/opt/wdws/.env` | Holds `PURVIEW_TENANT_ID` / `PURVIEW_CLIENT_ID` / `PURVIEW_CLIENT_SECRET` |

---

## Azure AD configuration

Registered application: **Athena Purview Decrypt** in tenant
`b1f6b00c-e0b4-4305-a505-bdb97888bed3` (Nelson Family).

- **App ID**: `f4a1af78-6866-4f2e-92da-9f2dac98aad1`
- **API permissions (application / admin-consented)**:
  - `Microsoft Rights Management Services` → `Content.SuperUser`
    (role id `7347eb49-7a1a-43c5-8eac-a5cd1d1c7cf0`)
  - `Microsoft Information Protection Sync Service` → `UnifiedPolicy.Tenant.Read`
    (role id `8b2071cd-015a-4025-8052-1c0dba2d3f64`)
- **Client secret**: 2-year, stored in [`.env`](.env) as `PURVIEW_CLIENT_SECRET`
  (display name `athena-purview-YYYYMMDD`)

**Re-create later (e.g. when the secret expires):**

```bash
APP_ID="f4a1af78-6866-4f2e-92da-9f2dac98aad1"
az ad app credential reset --id "$APP_ID" \
  --display-name "athena-purview-$(date +%Y%m%d)" \
  --years 2 --append
# copy the password into /opt/wdws/.env as PURVIEW_CLIENT_SECRET
```

---

## Authentication flows

Two are used, depending on who issued the content.

### 1. App-only (client credentials) — for content issued by our tenant

Used for: policy-sync calls, probing the Publishing License, decrypting content
that was protected *by* our tenant's RMS.

```http
POST https://login.microsoftonline.com/{PURVIEW_TENANT_ID}/oauth2/v2.0/token
  grant_type=client_credentials
  client_id={PURVIEW_CLIENT_ID}
  client_secret={PURVIEW_CLIENT_SECRET}
  scope=https://<resource>/.default
```

Roles in the resulting JWT: `Content.SuperUser`.

### 2. Delegated (az CLI) — for foreign-tenant content addressed to the user

Many `.rpmsg` files are issued by **external** tenants (former employer, legal
opposing counsel, medical-provider tenant, etc.) but name `william@seattleseahawks.me`
as the recipient in the Publishing License.

Cross-tenant app-only decrypt is **not** allowed by RMS design (a token from
your tenant cannot satisfy a foreign RMS server). Delegated user tokens work
because Azure AD issues them per-tenant after the user authenticates to the
*foreign* tenant directly.

The `AuthDelegate.AcquireDelegated()` in `Program.cs` shells out to:

```bash
az account get-access-token \
  --resource https://aadrm.com \
  --tenant <foreign-tenant-from-PL-challenge> \
  --query accessToken -o tsv
```

The target tenant is discovered at runtime by parsing the `issuerPrefix`
attribute from the RMS access-denied challenge and re-trying via
`RMS_TARGET_TENANT`.

**Tenant sign-in (one-time per tenant, re-auth every ~90 days):**

```bash
az login --use-device-code \
  --tenant <foreign-tenant-id> \
  --scope https://aadrm.com/.default
```

Device code fails mean the user is deprovisioned in that tenant; decryption is
not possible and the tool emits `auth_required` with the tenant id.

---

## Python API

```python
from purview_decrypt import decrypt_rpmsg

result = decrypt_rpmsg(
    "/path/to/message.rpmsg",
    "/path/to/body.html",     # body written here
)

# result status values:
#   "ok"                  — body + attachments written to disk
#   "no_permissions"      — PL issued, but for a different recipient
#   "auth_required"       — foreign tenant needs fresh `az login`
#                           (check result["required_tenant"])
#   "not_protected"       — file has no RMS protection
#   "error"               — other SDK failure (check result["error"])
```

Attachments are written as `<output_stem>.att00_<name>`, `.att01_<name>`, …

---

## CLI / batch use

**One-shot decrypt:**

```bash
source /opt/wdws/.env
python3 /opt/wdws/purview_decrypt.py input.rpmsg output.html
```

**Batch all `.rpmsg` under the email-attachments tree:**

```bash
python3 /opt/wdws/batch_decrypt_rpmsg.py
# Report: /opt/wdws/data/emails/attachments/rpmsg_decrypt_report.csv
# Status columns: ok | no_permissions | auth_required | not_protected | error
```

**Ingest decrypted content into Athena:**

```bash
python3 /opt/wdws/ingest_rpmsg_decrypted.py
# Idempotent via source_path. Adds:
#   core.documents rows (domain inferred: medical | legal)
#   core.document_chunks with BGE-M3 embeddings
#   medical.record_metadata rows for medical-tagged items
# metadata.source = 'rpmsg_decrypt'
# metadata.parent_rpmsg_path + metadata.pl_owner preserved for provenance
```

---

## Supported encrypted formats

Confirmed working on this server (April 2026):

| Format | How | Notes |
|---|---|---|
| `message.rpmsg` / `message_v2.rpmsg` | `InspectAsync` → `IMsgInspector` | Requires `enable_msg_file_type=true` in `FileEngineSettings.CustomSettings`. Returns decrypted body bytes + attachment byte streams. |
| AIP-protected PDF | `GetDecryptedTemporaryFileAsync` | Common as *attachments inside* `.rpmsg` — requires two decrypt passes. |
| IRM `.docx` / `.xlsx` / `.pptx` | `GetDecryptedTemporaryFileAsync` | Untested in production but supported by the SDK path. |
| `.pfile` generic wrapper | `GetDecryptedTemporaryFileAsync` | Untested in production. |

Not supported by the File API but PL can still be extracted (for forensic /
audit use):

- Standalone DRMContent streams — would need Protection API + manual parsing.

---

## MCP integration considerations

Designed so a future MCP tool can surface interactive-auth prompts cleanly:

- `status = "auth_required"` response carries `required_tenant` so the calling
  agent (Athena) can prompt the user with the exact tenant id.
- Ideal MCP contract: tool returns `auth_required` → agent relays the tenant +
  instructions to the user (verification URL + device code) → user completes
  `az login --tenant <id> --scope https://aadrm.com/.default` → agent retries.
- No secret material leaves the server; all auth stays in the server's `az`
  token cache.

---

## Limitations

1. **Cross-tenant identity required**: If `az login` to a foreign tenant
   returns "AADSTS50020: user does not exist," the content is cryptographically
   unrecoverable without that tenant's cooperation.
2. **Refresh-token lifetime** on cached `az` credentials is bounded by the
   foreign tenant's conditional-access policy — typically 14–90 days.
3. **No MIP SDK on ARM64** — `Microsoft.InformationProtection.File.Ubuntu2204`
   ships x86_64 only.
4. **NuGet warning NU1701** during build is expected — the managed assembly
   targets `.NET Framework 4.8`; runs fine under `.NET 8`'s compat shim.

---

## Known good test cases (April 21 2026 batch)

| PL Issuer | Count | Example | Notes |
|---|---:|---|---|
| `b1f6b00c-…` Nelson Family | 13 | 2024-01-11 *ADA accommodation 23-2-07759-31 Review* | Own-tenant, decrypted via app-only |
| `033adcfd-…` DTG - MSA | 11 | 2023-06-07 *Fw Urgent Miscommunication Issue* | Cross-tenant via delegated auth |
| `ee69be27-…` Starbucks-era | 6 | 2021-12-23 *Wills Payroll Woes* | Deprovisioned identity — parked |
| `f6b6dd5b-…` UW Medicine (forwarded) | 1 | 2025-08-25 *FW UW Medicine Records Request* | Not a guest user — parked |

**Doubly-encrypted AIP PDFs successfully recovered:**
- `Nelson_William_bike_report_T1.pdf` (Workwell 2-day CPET, Day 1, 6 pages)
- `Nelson_William_bike_report_T2.pdf` (Workwell 2-day CPET, Day 2, 6 pages)
- `Nelson_William_CPET_report_2026-03-23_CRS.pdf` (CPET Clinical Summary, 11 pages)
- `NELSON V STARBUCKS - MOTION TO COMPEL AND RECONSIDER THE COURTS ORDER.pdf` (19 pages)
- `NELSON V UNUM GROUP - MOTION TO COMPEL AND RECONSIDER THE COURTS ORDER.pdf` (19 pages)
- `Nelson_Medical_Chronology_Mar2026.pdf` (7 pages, attorney-client privileged)
- `letter101123-01.pdf` (2 pages, Unum claim correspondence)

---

## Troubleshooting

**`File type cannot be inspected`** — The tool now falls back from
`InspectAsync` to `GetDecryptedTemporaryFileAsync` automatically. If both
fail, the file isn't in a format MIP's File API handles (very rare).

**`The service didn't accept the auth token`** — Either (a) the foreign-tenant
delegated token needs refresh (run `az login --tenant <id> --scope https://aadrm.com/.default`),
or (b) the app is missing the `UnifiedPolicy.Tenant.Read` Sync-Service
permission (grant admin consent).

**`No principal found that matches the expected issuer`** — App-only token
from our tenant was rejected by a foreign RMS server. The tool will auto-retry
with a delegated token; if you're not signed into the foreign tenant via `az`,
you'll see `auth_required`.

**`AADSTS70043: refresh token expired`** — Run `az login --use-device-code
--tenant <id> --scope https://aadrm.com/.default` to refresh.

**`AADSTS50020: user does not exist in this tenant`** — Your identity is not a
member or guest of the issuing tenant. Content cannot be decrypted through this
pipeline. Options: ask the originating party to resend unprotected, or
subpoena/discovery in a litigation context.

**Build warning `NU1701: Package was restored using .NETFramework`** — Expected.
Ignore.

---

## Rebuild the .NET tool from source

```bash
cd /opt/wdws/purview-decrypt
/opt/dotnet/dotnet build -c Release
/opt/dotnet/dotnet publish -c Release --self-contained false -o bin/publish
```

Invocation:

```bash
export LD_LIBRARY_PATH=/opt/wdws/purview-decrypt/bin/publish
/opt/dotnet/dotnet /opt/wdws/purview-decrypt/bin/publish/purview-decrypt.dll \
    input.rpmsg output.html
```

Expect ~4–5 s wall time per call (.NET cold-start + token acquisition +
MIP SDK initialization dominate; actual crypto is sub-second).
