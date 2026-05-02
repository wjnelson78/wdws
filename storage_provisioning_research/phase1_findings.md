---
title: Storage Provisioning Agent — Phase 1 Findings
date: 2026-05-01
status: HALT — kickoff premise contradicted by ground truth; awaiting operator disposition
agent: Storage Provisioning Agent (single-purpose; stands down post-completion)
---

# Phase 1 Findings — HALT-AND-SURFACE

## Executive summary

**The kickoff's premise — "newly-installed 250 GB HDD to integrate" — is contradicted by hardware ground truth in two material ways:**

1. **There is no second physical disk.** `lsblk` shows ONE block device (`/dev/sda`, 370 GiB QEMU virtual disk). No `/dev/sdb`, no `/dev/nvmeXn1`. The "+250 GB" was an **online expansion of the existing virtual disk**, not the addition of a new HDD. dmesg confirms: `sda: detected capacity change from 251658240 to 775946240` (120 GiB → 370 GiB, delta = 250 GiB).
2. **The +250 GB is already integrated.** A new partition `/dev/sda4` (250 GB ext4, label `wa-caselaw-cache`, UUID `03e1320e-...`) already exists, is mounted at `/var/cache/wa_caselaw`, is in `/etc/fstab` for persistence, and is **actively in use right now** by the case law agent's `wget` (PID 327383, 53.3 GB downloaded so far). dmesg timestamp shows sda4 was mounted at boot-time +109090 sec — well before this kickoff.

**Halt triggers fired (per kickoff):**
- "Drive identity mismatch: if lsblk shows different drive count or sizes than expected → halt and surface" ✅ FIRED
- "Drive already-formatted: if new drive shows existing filesystem or partition table → halt (likely misidentified existing data drive)" ✅ FIRED

**§K methodology corroborations triggered:**
- §K #25 (coordinator structural-knowledge currency — verify before acting) — kickoff's "newly-installed 250 GB HDD" premise is stale; ground truth is "online disk-resize already integrated as sda4"
- §K #29 (per-source pre-flight vs coordinator estimates) — verified hardware state via `lsblk`/`dmesg`/`fdisk -l` before any destructive operation
- §K #37 (provable-existence vs convenient-completeness) — provable verification of "newly-installed" claim failed at the lsblk gate

## Mission target reconciliation

| Mission target | Status | Notes |
|---|---|---|
| `/var/cache/wa_caselaw/` has >100 GB free | ✅ **ALREADY MET** | 246 GB total, 52 GB used, **181 GB free** on /dev/sda4 (mounted) |
| PostgreSQL datadir has >100 GB free | ❌ **NOT MET** | Datadir = `/var/lib/postgresql/17/main` (8.0 GB current); on /dev/sda2 (root); only 27 GB free on that filesystem |
| Existing data preserved without interruption | ✅ Preserved (no destructive ops attempted) | |
| Reversibility | ✅ N/A (nothing changed) | |

The case law agent's halt-on-disk-headroom constraint is a **PostgreSQL-datadir** concern (Phase 4 staging tables for opinion ingest), not a `/var/cache/` concern. The cache filesystem already has the headroom required.

## Verified storage architecture (read-only inspection)

```
/dev/sda  370 GiB  QEMU HARDDISK  (single virtual disk, online-resized 2026-05-01 from 120→370 GiB)
├── sda1  EFI System  976 MB  → /boot/efi
├── sda2  ext4        112.9 GB → /  (75% used, 27 GB free)
├── sda3  swap        6.2 GB
└── sda4  ext4        250 GB  → /var/cache/wa_caselaw  (label wa-caselaw-cache, 181 GB free)
```

- **No LVM** (`pvs`/`vgs`/`lvs` not installed; this is a Proxmox guest VM, not the Proxmox host)
- **No ZFS** (nothing relevant in `zpool` queries)
- **Direct GPT partitioning** with ext4 on data partitions

## sda2 (root) space breakdown (top consumers)

| Path | Size | Notes |
|---|---:|---|
| /opt/wdws | 24 GB | Project files (corpus research dirs, cached source artifacts) |
| /usr | 18 GB | System binaries / libraries — system-critical, not freeable |
| /root | 16 GB | Operator/agent home, configs, caches |
| /var/lib/postgresql/17/main | 8.0 GB | Current PG datadir footprint |
| /var/log | 4.5 GB | System + service logs |
| /opt/minio | 4.3 GB | Object store data |
| /opt (other) | ~3 GB | Misc tooling (azure CLI, dotnet, recon tools) |

**Root filesystem is structurally crowded; no low-cost cleanup path will free 70+ GB without architectural change.**

## Coordination check — case law agent download in progress

- PID 327383: `wget -c -O opinions-2026-03-31.csv.bz2 https://com-courtlistener-storage.s3.amazonaws.com/bulk-data/...`
- Currently 53.3 GB written to `/var/cache/wa_caselaw/opinions-2026-03-31.csv.bz2`
- Writing to /dev/sda4 (the cache filesystem) — completely independent of /dev/sda2 (root)
- **No intersection with any storage-provisioning operation.** Phase 1 read-only ops have zero conflict; any future ops on sda2 also have zero conflict.

## Service inventory (relevant)

- PostgreSQL 17 (datadir on sda2)
- Several systemd services running off sda2 (system + Athena + ingest workers)
- Active `wget` (case law) on sda4

A pg-datadir relocation would require a postgres restart; magnitude depends on approach.

## Three approaches to address the PG-datadir headroom mission target

The kickoff's three approaches (A=mount+bind, B=LVM extend, C=ZFS extend) all assumed an unintegrated new HDD. With ground truth as it is, the actual viable approaches are:

### Approach 1 — Accept current state; do nothing on sda2; co-locate pg expansion on sda4
- **Idea:** PostgreSQL has `tablespaces`. Create a tablespace at `/var/cache/wa_caselaw/pg_tablespace/` (or a sibling dir at the sda4 root) and put the case-law agent's heavy staging tables there explicitly.
- **Pros:** No partition surgery. Reversible (DROP TABLESPACE). Cache filesystem has 181 GB free, easily covers staging.
- **Cons:** Mixes cache content with DB content on the same filesystem. Cache filesystem name (`wa-caselaw-cache`) is misleading if used as DB storage. If sda4 fills (case law downloads + pg staging both growing), both halt simultaneously.
- **Operator decision required:** create dedicated subdir `/var/cache/wa_caselaw/pg_staging/`? Or mount it at a more appropriately-named path?

### Approach 2 — Shrink sda4 and create a new sda5 for pg datadir
- **Idea:** sda4 has 181 GB free of 246 GB. Could be shrunk to ~100 GB, freeing ~146 GB for a new sda5 dedicated to PG.
- **Pros:** Clean separation; new partition can be mounted at `/var/lib/postgresql_data/` and pg datadir relocated.
- **Cons:** **DESTRUCTIVE.** Requires unmounting sda4 (case law agent's wget MUST halt first), `resize2fs --shrink` then `parted resizepart`, then partition creation + mkfs. Each step is recoverable from backup but not from runtime error. Risk during shrink even with healthy fs.
- **Strong recommendation against** while case law agent is mid-download (53 GB invested). At minimum, wait for download completion.

### Approach 3 — Online-resize the virtual disk again; add sda5 from new tail capacity
- **Idea:** Have Proxmox host extend `/dev/sda` further (e.g., +200 GB to bring total to 570 GiB), then create new sda5 on the unused tail without touching existing partitions.
- **Pros:** **NON-DESTRUCTIVE for existing partitions.** sda4 stays mounted, case law download keeps running. Just `parted` adds a new partition on the fresh tail capacity, mkfs, mount.
- **Cons:** Requires Proxmox-host action by operator (this VM cannot resize its own backing disk). Adds total VM disk allocation; verify host capacity.
- **Recommended approach if mission target is to be met**, but conditional on operator's authority over Proxmox host capacity allocation.

### Approach 4 — Defer to case law agent
- **Idea:** Surface the architectural reality back to the case law agent: their canon's §K #43 (streaming pipeline; no full materialization in pg datadir) was specifically authored to address this exact scenario. The case law agent's Phase 3 design is `bzcat | filter | psql COPY FROM stdin` — final tables ~14 GB, not 200 GB.
- **Pros:** No storage provisioning needed if case law agent's streaming architecture holds. Per their canon: "Final case_law.* tables hold ~94K WA-scoped rows (~14 GB), well within budget." 14 GB fits in current 27 GB headroom.
- **Cons:** Tight margin (~13 GB headroom after ingest). One unrelated growth event on sda2 could trip pg.
- **This may make storage provisioning unnecessary altogether.** Surfacing for operator review.

## Recommendation

**Recommend halting the kickoff itself** until the operator confirms which scenario applies:

- **Scenario A:** the operator was working from a stale mental model (the resize already happened earlier today), and the kickoff is moot — `/var/cache/wa_caselaw` already has 181 GB free, exceeds the >100 GB target. **Stand down; case law agent unblocked.**
- **Scenario B:** the operator intended to add a *second* physical disk on the Proxmox host (which would land as `/dev/sdb`) and the resize-of-sda was a separate event; the second disk has not yet appeared on this guest. **Wait for sdb to appear, then re-run Phase 1.**
- **Scenario C:** the operator wants additional headroom for pg datadir specifically. Then the right path is **Approach 3** (online-resize sda further, add sda5 on the tail, mount as dedicated pg datadir filesystem) **or Approach 4** (rely on case law agent's streaming architecture per §K #43).

No destructive operations performed. No fstab edits. No partitions created. Read-only investigation only.

## Backup verification status

Not verified in Phase 1 (would have been Phase 1 task 7 if proceeding to a Phase 2). Backup state should be confirmed before any approach 2 or 3 destructive step.

## Wall time

~10 minutes (read-only investigation only).

## §K corroboration candidate — to canonize

**§K #44 candidate — "newly-installed hardware" claims must distinguish between physical-add and online-resize on virtualized hosts.** On Proxmox/KVM/VMware/cloud guests, operators frequently expand existing virtual disks rather than attaching new ones. The semantic difference matters: physical-add yields `/dev/sdb` (or analog); online-resize yields a larger `/dev/sda` with new partition tail. Treating a kickoff's "newly-installed HDD" claim as physical-add when it's actually online-resize misroutes the entire integration approach. Audit channel: cross-check `lsblk` device count against expectation BEFORE designing partition strategy. If device count is unchanged, the "new HDD" was an online resize and the integration approach is fundamentally different from the physical-add case (no partprobe needed for physical add; resize tail typically already partitioned by an earlier step).

Generalizes §K #25 (coordinator structural-knowledge currency) from statutory/source structural claims to hardware structural claims. Same audit-predicate: ground truth (lsblk/dmesg) is authoritative; coordinator narrative is hypothesis.

Awaiting operator confirmation before adding to canonical list.
