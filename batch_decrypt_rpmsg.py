#!/usr/bin/env python3
"""
Batch-decrypt all .rpmsg files under the email-attachments tree and write a
per-file report. Decrypted bodies go into sibling files named
`<original_stem>.decrypted.html` (or .txt for plain-text bodies) next to the
.rpmsg. Attachments inside the decrypted .rpmsg get written out too.

Pipeline output is a CSV at /opt/wdws/data/emails/attachments/rpmsg_decrypt_report.csv
suitable for follow-up ingestion.
"""

import csv
import json
import sys
from pathlib import Path

sys.path.insert(0, "/opt/wdws")
from purview_decrypt import decrypt_rpmsg, _load_env_file

ROOTS = [
    Path("/opt/wdws/data/emails/attachments"),
    Path("/opt/wdws/data/inbox/attachments"),
]
REPORT = Path("/opt/wdws/data/emails/attachments/rpmsg_decrypt_report.csv")


def main():
    _load_env_file()
    rpmsgs = []
    for root in ROOTS:
        rpmsgs.extend(sorted(root.rglob("*.rpmsg")))
    print(f"Found {len(rpmsgs)} .rpmsg files", file=sys.stderr)

    rows = []
    for i, rpmsg in enumerate(rpmsgs, 1):
        # Output body alongside the .rpmsg (same stem + .decrypted.html)
        out = rpmsg.with_suffix("").with_name(rpmsg.stem + ".decrypted.html")
        if out.exists():
            print(f"[{i:3d}/{len(rpmsgs)}] skip (exists): {rpmsg.name}", file=sys.stderr)
            rows.append({
                "rpmsg_path": str(rpmsg),
                "status": "already_decrypted",
                "output": str(out),
                "body_size": out.stat().st_size,
            })
            continue

        print(f"[{i:3d}/{len(rpmsgs)}] decrypt: {rpmsg.parent.name}/{rpmsg.name}", file=sys.stderr)
        result = decrypt_rpmsg(rpmsg, out)
        status = result.get("status")
        row = {
            "rpmsg_path": str(rpmsg),
            "status": status,
            "pl_owner": result.get("plOwner", ""),
            "required_tenant": result.get("required_tenant", ""),
            "body_type": result.get("bodyType", ""),
            "body_size": result.get("bodySize", ""),
            "attachment_count": result.get("attachmentCount", ""),
            "output": str(out) if status == "ok" else "",
            "error": result.get("error", "")[:300],
        }
        rows.append(row)
        mark = {"ok": "✔", "no_permissions": "⊘ no-rights",
                "auth_required": "🔐 needs-login",
                "not_protected": "∅"}.get(status, f"✗ {status}")
        print(f"       {mark}  plOwner={result.get('plOwner','?')[:40]}", file=sys.stderr)

    # Write report
    with open(REPORT, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    # Summary
    counts = {}
    for r in rows:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    print("\n=== SUMMARY ===", file=sys.stderr)
    for s, n in sorted(counts.items(), key=lambda kv: -kv[1]):
        print(f"  {n:3d}  {s}", file=sys.stderr)
    print(f"\nReport: {REPORT}", file=sys.stderr)


if __name__ == "__main__":
    main()
