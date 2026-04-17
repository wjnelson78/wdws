#!/usr/bin/env python3
"""Athena raw transfer helper for Graph-style upload/download sessions.

This helper talks only to the raw HTTP data-plane endpoints returned by the MCP
control-plane tools:

- `create_document_upload_session(...)` -> `upload_url`
- `create_document_download_session(...)` -> `download_url`

Typical flow:
  1. Create an upload session via MCP.
  2. Run: `python raw_transfer_client.py upload --upload-url ... --file ./report.pdf`
  3. Create a download session via MCP.
  4. Run: `python raw_transfer_client.py download --download-url ... --output ./report.pdf`

Server defaults at the time this helper was added:
- max file size: 100 MiB
- max upload chunk size: 4 MiB
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Optional

import httpx

DEFAULT_UPLOAD_CHUNK_SIZE = 4 * 1024 * 1024
DEFAULT_STREAM_CHUNK_SIZE = 64 * 1024
DEFAULT_TIMEOUT_SECONDS = 60.0


def _json_or_text(response: httpx.Response) -> Any:
    content_type = response.headers.get("content-type", "")
    if "application/json" in content_type:
        try:
            return response.json()
        except ValueError:
            pass
    text = response.text
    return {
        "status_code": response.status_code,
        "content_type": content_type,
        "text": text,
    }


def _parse_content_disposition_filename(value: str) -> Optional[str]:
    if not value:
        return None

    match = re.search(r"filename\*=UTF-8''([^;]+)", value, flags=re.IGNORECASE)
    if match:
        return httpx.URL("https://placeholder.invalid/" + match.group(1)).path.lstrip("/")

    match = re.search(r'filename="([^"]+)"', value, flags=re.IGNORECASE)
    if match:
        return match.group(1)

    match = re.search(r"filename=([^;]+)", value, flags=re.IGNORECASE)
    if match:
        return match.group(1).strip().strip('"')

    return None


def get_upload_status(upload_url: str, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> Any:
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        response = client.get(upload_url)
        response.raise_for_status()
        return _json_or_text(response)


def cancel_upload(upload_url: str, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> Any:
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        response = client.delete(upload_url)
        response.raise_for_status()
        return _json_or_text(response)


def upload_file(
    upload_url: str,
    file_path: str,
    chunk_size: int = DEFAULT_UPLOAD_CHUNK_SIZE,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    path = Path(file_path).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"File not found: {path}")
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero")

    total_size = path.stat().st_size
    if total_size <= 0:
        raise ValueError("File is empty; upload sessions require a positive file size")

    bytes_sent = 0
    chunk_count = 0
    last_response: Any = None

    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        with path.open("rb") as handle:
            while bytes_sent < total_size:
                body = handle.read(chunk_size)
                if not body:
                    break

                start = bytes_sent
                end = start + len(body) - 1
                response = client.put(
                    upload_url,
                    headers={"Content-Range": f"bytes {start}-{end}/{total_size}"},
                    content=body,
                )
                response.raise_for_status()

                bytes_sent = end + 1
                chunk_count += 1
                last_response = _json_or_text(response)

        status = get_upload_status(upload_url, timeout=timeout)

    return {
        "file": str(path),
        "file_size": total_size,
        "chunk_size": chunk_size,
        "chunks_sent": chunk_count,
        "bytes_sent": bytes_sent,
        "last_chunk_response": last_response,
        "status": status,
    }


def head_download(download_url: str, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> dict[str, Any]:
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        response = client.head(download_url)
        response.raise_for_status()
        return {
            "status_code": response.status_code,
            "content_length": response.headers.get("content-length"),
            "content_type": response.headers.get("content-type"),
            "content_disposition": response.headers.get("content-disposition"),
            "accept_ranges": response.headers.get("accept-ranges"),
            "etag": response.headers.get("etag"),
        }


def download_file(
    download_url: str,
    output_path: Optional[str] = None,
    byte_range: Optional[str] = None,
    overwrite: bool = False,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    headers: dict[str, str] = {}
    if byte_range:
        headers["Range"] = f"bytes={byte_range}"

    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        with client.stream("GET", download_url, headers=headers) as response:
            response.raise_for_status()

            content_disposition = response.headers.get("content-disposition", "")
            suggested_name = _parse_content_disposition_filename(content_disposition)
            destination = Path(output_path or suggested_name or "download.bin").expanduser().resolve()

            if destination.exists() and not overwrite:
                raise FileExistsError(
                    f"Output file already exists: {destination}. Pass --overwrite to replace it."
                )

            destination.parent.mkdir(parents=True, exist_ok=True)
            bytes_written = 0
            with destination.open("wb") as handle:
                for chunk in response.iter_bytes(DEFAULT_STREAM_CHUNK_SIZE):
                    if not chunk:
                        continue
                    handle.write(chunk)
                    bytes_written += len(chunk)

            return {
                "output": str(destination),
                "bytes_written": bytes_written,
                "status_code": response.status_code,
                "content_type": response.headers.get("content-type"),
                "content_length": response.headers.get("content-length"),
                "content_disposition": content_disposition,
                "accept_ranges": response.headers.get("accept-ranges"),
                "range": byte_range,
            }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Athena raw transfer session helper")
    subparsers = parser.add_subparsers(dest="command", required=True)

    upload_parser = subparsers.add_parser("upload", help="Upload a file to an existing upload session")
    upload_parser.add_argument("--upload-url", required=True, help="Upload session URL returned by Athena")
    upload_parser.add_argument("--file", required=True, help="Path to the file to upload")
    upload_parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_UPLOAD_CHUNK_SIZE,
        help="Chunk size in bytes (default: 4194304)",
    )
    upload_parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="HTTP timeout in seconds",
    )

    status_parser = subparsers.add_parser("status", help="Read upload session status")
    status_parser.add_argument("--upload-url", required=True, help="Upload session URL returned by Athena")
    status_parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="HTTP timeout in seconds",
    )

    cancel_parser = subparsers.add_parser("cancel", help="Cancel an upload session")
    cancel_parser.add_argument("--upload-url", required=True, help="Upload session URL returned by Athena")
    cancel_parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="HTTP timeout in seconds",
    )

    head_parser = subparsers.add_parser("head", help="Inspect download headers without fetching the body")
    head_parser.add_argument("--download-url", required=True, help="Download session URL returned by Athena")
    head_parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="HTTP timeout in seconds",
    )

    download_parser = subparsers.add_parser("download", help="Download a file from an existing download session")
    download_parser.add_argument("--download-url", required=True, help="Download session URL returned by Athena")
    download_parser.add_argument("--output", help="Optional output path; defaults to server filename or download.bin")
    download_parser.add_argument(
        "--range",
        dest="byte_range",
        help="Optional inclusive byte range like 0-1023",
    )
    download_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite the output file if it already exists",
    )
    download_parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="HTTP timeout in seconds",
    )

    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "upload":
        result = upload_file(
            upload_url=args.upload_url,
            file_path=args.file,
            chunk_size=args.chunk_size,
            timeout=args.timeout,
        )
    elif args.command == "status":
        result = get_upload_status(args.upload_url, timeout=args.timeout)
    elif args.command == "cancel":
        result = cancel_upload(args.upload_url, timeout=args.timeout)
    elif args.command == "head":
        result = head_download(args.download_url, timeout=args.timeout)
    elif args.command == "download":
        result = download_file(
            download_url=args.download_url,
            output_path=args.output,
            byte_range=args.byte_range,
            overwrite=args.overwrite,
            timeout=args.timeout,
        )
    else:
        parser.error(f"Unsupported command: {args.command}")
        return 2

    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except httpx.HTTPStatusError as exc:
        error = {
            "error": str(exc),
            "status_code": exc.response.status_code,
            "response": _json_or_text(exc.response),
        }
        print(json.dumps(error, indent=2, sort_keys=True), file=sys.stderr)
        raise SystemExit(1)
    except Exception as exc:
        print(json.dumps({"error": str(exc)}, indent=2, sort_keys=True), file=sys.stderr)
        raise SystemExit(1)
