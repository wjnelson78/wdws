#!/usr/bin/env python3
"""
Athena Purview Decrypt — Python wrapper around the .NET MIP SDK tool.

Wraps /opt/wdws/purview-decrypt/bin/publish/purview-decrypt.dll which uses
the Microsoft Information Protection SDK (Ubuntu 22.04 NuGet) on Linux to
decrypt .rpmsg / .msg RMS-protected email files.

Auth flow for foreign (sender) tenants is handled by az CLI — the tool
shells out to `az account get-access-token --tenant <foreign-tenant>`
to acquire user-impersonation tokens for aadrm.com. Requires `az login`
against each tenant that issued the content.

Usage:
    from purview_decrypt import decrypt_rpmsg

    result = decrypt_rpmsg(
        "/path/to/message.rpmsg",
        "/path/to/output.html",   # decrypted body is written here
    )
    if result["status"] == "ok":
        print(f"body {result['bodySize']}b, {result['attachmentCount']} attachments")
"""

import json
import os
import subprocess
from pathlib import Path
from typing import Dict, Optional

DOTNET = "/opt/dotnet/dotnet"
DLL = "/opt/wdws/purview-decrypt/bin/publish/purview-decrypt.dll"
NATIVE_LIB_DIR = "/opt/wdws/purview-decrypt/bin/publish"


def decrypt_rpmsg(
    input_path: str | Path,
    output_path: str | Path,
    env_extra: Optional[Dict[str, str]] = None,
    timeout: int = 180,
) -> dict:
    """
    Decrypt a .rpmsg file. Returns structured status dict.

    Status values:
      - "ok"               — success; decrypted body written, attachments saved alongside
      - "NoPermissionsException" — user lacks rights (content protected for someone else)
      - "auth_required"    — stale delegated token; needs `az login --tenant <foreign>`
      - "not_protected"    — file is not RMS-protected
      - "error"            — other SDK error (see `error` field)
    """
    input_path = str(input_path)
    output_path = str(output_path)

    env = os.environ.copy()
    env["LD_LIBRARY_PATH"] = NATIVE_LIB_DIR
    for key in ("PURVIEW_TENANT_ID", "PURVIEW_CLIENT_ID", "PURVIEW_CLIENT_SECRET"):
        if key not in env:
            # Pull from /opt/wdws/.env if not set
            _load_env_file()
            env[key] = os.environ.get(key, "")
    if env_extra:
        env.update(env_extra)

    try:
        proc = subprocess.run(
            [DOTNET, DLL, input_path, output_path],
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
        )
    except subprocess.TimeoutExpired:
        return {"status": "timeout", "input": input_path, "timeout_s": timeout}

    # Result is the last line of stdout (single-line JSON)
    stdout = proc.stdout.strip()
    if not stdout:
        return {
            "status": "no_output",
            "input": input_path,
            "returncode": proc.returncode,
            "stderr_tail": proc.stderr.strip().split("\n")[-5:],
        }

    # Pick the last JSON-looking line
    lines = [l for l in stdout.splitlines() if l.startswith("{") and l.endswith("}")]
    if not lines:
        return {
            "status": "no_json",
            "input": input_path,
            "returncode": proc.returncode,
            "raw_stdout": stdout[:500],
        }
    try:
        result = json.loads(lines[-1])
    except json.JSONDecodeError as e:
        return {"status": "parse_error", "input": input_path, "raw": lines[-1], "error": str(e)}

    # Map .NET error types to cleaner status codes for downstream consumers
    if result.get("status") == "error":
        t = result.get("type", "")
        if t == "NoPermissionsException":
            result["status"] = "no_permissions"
        elif t in ("NoAuthTokenException",) or "AADSTS70043" in result.get("error", ""):
            result["status"] = "auth_required"
            # Extract the tenant that needs fresh login
            import re
            m = re.search(r"--tenant\s+\"?([0-9a-f-]+)", result.get("error", ""))
            if m:
                result["required_tenant"] = m.group(1)
            m2 = re.search(r"tenant=([0-9a-f-]+)", result.get("error", ""))
            if not m and m2:
                result["required_tenant"] = m2.group(1)

    return result


def _load_env_file(env_path: str = "/opt/wdws/.env") -> None:
    """Load PURVIEW_* keys from .env into os.environ if not already set."""
    try:
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                if k.startswith("PURVIEW_") and k not in os.environ:
                    os.environ[k] = v
    except FileNotFoundError:
        pass


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("usage: purview_decrypt.py <input.rpmsg> <output_body>", file=sys.stderr)
        sys.exit(2)
    result = decrypt_rpmsg(sys.argv[1], sys.argv[2])
    print(json.dumps(result, indent=2))
    sys.exit(0 if result.get("status") == "ok" else 1)
