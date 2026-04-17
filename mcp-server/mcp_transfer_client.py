#!/usr/bin/env python3
"""Athena MCP transfer client.

This script uses the real MCP control-plane over Streamable HTTP to create
upload/download sessions, then uses Athena's raw HTTP data-plane helper for the
actual bytes.

Requirements:
- A bearer access token for Athena MCP (`MCP_ACCESS_TOKEN` or `--access-token`)
- `read` scope for download session creation
- `write` scope for upload session creation

Examples:
  python mcp_transfer_client.py upload \
    --endpoint http://127.0.0.1:9200/mcp/sse \
    --transfer-base-url http://127.0.0.1:9200 \
    --access-token "$MCP_ACCESS_TOKEN" \
    --file ./report.pdf \
    --domain operations

  python mcp_transfer_client.py download \
    --endpoint http://127.0.0.1:9200/mcp/sse \
    --transfer-base-url http://127.0.0.1:9200 \
    --access-token "$MCP_ACCESS_TOKEN" \
    --document-id 11111111-2222-3333-4444-555555555555 \
    --output ./report.pdf

  python mcp_transfer_client.py roundtrip \
    --endpoint http://127.0.0.1:9200/mcp/sse \
    --transfer-base-url http://127.0.0.1:9200 \
    --access-token "$MCP_ACCESS_TOKEN" \
    --file ./sample.txt \
    --output ./sample.out.txt \
    --domain operations
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import mimetypes
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse, urlunparse

from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamablehttp_client
from mcp.types import Implementation

from raw_transfer_client import download_file, head_download, upload_file

DEFAULT_MCP_ENDPOINT = os.getenv("ATHENA_MCP_ENDPOINT", "http://127.0.0.1:9200/mcp/sse")
DEFAULT_ACCESS_TOKEN = os.getenv("MCP_ACCESS_TOKEN") or os.getenv("ATHENA_MCP_ACCESS_TOKEN")
DEFAULT_CONNECT_TIMEOUT = float(os.getenv("ATHENA_MCP_TIMEOUT", "30"))
DEFAULT_TRANSFER_TIMEOUT = float(os.getenv("ATHENA_TRANSFER_TIMEOUT", "60"))
DEFAULT_UPLOAD_CHUNK_SIZE = 4 * 1024 * 1024


def _guess_content_type(path: Path) -> str:
    guessed, _ = mimetypes.guess_type(path.name)
    return guessed or "application/octet-stream"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _rewrite_url_base(url: str, transfer_base_url: Optional[str]) -> str:
    if not transfer_base_url:
        return url

    original = urlparse(url)
    replacement = urlparse(transfer_base_url)
    if not replacement.scheme or not replacement.netloc:
        raise ValueError("transfer_base_url must include a scheme and host, e.g. http://127.0.0.1:9200")

    return urlunparse(
        (
            replacement.scheme,
            replacement.netloc,
            original.path,
            original.params,
            original.query,
            original.fragment,
        )
    )


def _parse_tool_payload(result: Any) -> Any:
    structured = getattr(result, "structuredContent", None)
    if structured:
        if isinstance(structured, dict) and set(structured.keys()) == {"result"}:
            inner = structured.get("result")
            if isinstance(inner, str):
                try:
                    return json.loads(inner)
                except json.JSONDecodeError:
                    return {"text": inner}
            return inner
        return structured

    text_parts: list[str] = []
    for item in getattr(result, "content", None) or []:
        text = getattr(item, "text", None)
        if text is not None:
            text_parts.append(text)

    if not text_parts:
        if getattr(result, "isError", False):
            raise RuntimeError("MCP tool returned an error with no text payload")
        return {}

    if len(text_parts) == 1:
        text = text_parts[0]
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return {"text": text}

    joined = "\n".join(text_parts)
    try:
        return json.loads(joined)
    except json.JSONDecodeError:
        return {"text": joined}


def _require_tool_success(payload: Any) -> Any:
    if isinstance(payload, dict) and payload.get("error"):
        raise RuntimeError(str(payload["error"]))
    return payload


async def _call_tool_json(session: ClientSession, name: str, arguments: dict[str, Any]) -> Any:
    result = await session.call_tool(name, arguments)
    payload = _parse_tool_payload(result)
    return _require_tool_success(payload)


@asynccontextmanager
async def _mcp_session(endpoint: str, access_token: str, timeout: float):
    headers = {"Authorization": f"Bearer {access_token}"} if access_token else None
    async with streamablehttp_client(endpoint, headers=headers, timeout=timeout) as (read_stream, write_stream, _):
        client_info = Implementation(name="athena-mcp-transfer-client", version="1.0")
        async with ClientSession(read_stream, write_stream, client_info=client_info) as session:
            initialize_result = await session.initialize()
            yield session, initialize_result


async def _create_upload_session(
    session: ClientSession,
    file_path: Path,
    *,
    domain: str,
    content_type: Optional[str],
    title: Optional[str],
    document_type: Optional[str],
    description: Optional[str],
    expires_in_seconds: Optional[int],
    sha256: Optional[str],
) -> dict[str, Any]:
    arguments: dict[str, Any] = {
        "filename": file_path.name,
        "file_size": file_path.stat().st_size,
        "domain": domain,
        "content_type": content_type or _guess_content_type(file_path),
    }
    if title:
        arguments["title"] = title
    if document_type:
        arguments["document_type"] = document_type
    if description:
        arguments["description"] = description
    if expires_in_seconds is not None:
        arguments["expires_in_seconds"] = int(expires_in_seconds)
    if sha256:
        arguments["sha256"] = sha256

    payload = await _call_tool_json(session, "create_document_upload_session", arguments)
    if not isinstance(payload, dict):
        raise RuntimeError(f"Unexpected upload session payload: {payload!r}")
    return payload


async def _create_download_session(
    session: ClientSession,
    document_id: str,
    *,
    disposition: str,
    expires_in_seconds: Optional[int],
) -> dict[str, Any]:
    arguments: dict[str, Any] = {
        "document_id": document_id,
        "disposition": disposition,
    }
    if expires_in_seconds is not None:
        arguments["expires_in_seconds"] = int(expires_in_seconds)

    payload = await _call_tool_json(session, "create_document_download_session", arguments)
    if not isinstance(payload, dict):
        raise RuntimeError(f"Unexpected download session payload: {payload!r}")
    return payload


async def _run_upload(args: argparse.Namespace) -> dict[str, Any]:
    access_token = args.access_token or DEFAULT_ACCESS_TOKEN
    if not access_token:
        raise RuntimeError("An Athena MCP bearer token is required. Use --access-token or MCP_ACCESS_TOKEN.")

    file_path = Path(args.file).expanduser().resolve()
    if not file_path.is_file():
        raise FileNotFoundError(f"File not found: {file_path}")

    sha256 = args.sha256
    if args.compute_sha256 and not sha256:
        sha256 = _sha256_file(file_path)

    async with _mcp_session(args.endpoint, access_token, args.timeout) as (session, initialize_result):
        session_payload = await _create_upload_session(
            session,
            file_path,
            domain=args.domain,
            content_type=args.content_type,
            title=args.title,
            document_type=args.document_type,
            description=args.description,
            expires_in_seconds=args.expires_in_seconds,
            sha256=sha256,
        )

    upload_url = _rewrite_url_base(str(session_payload["upload_url"]), args.transfer_base_url)
    transfer_result = upload_file(
        upload_url=upload_url,
        file_path=str(file_path),
        chunk_size=args.chunk_size,
        timeout=args.transfer_timeout,
    )

    status = transfer_result.get("status") or {}
    document_id = status.get("document_id") or (status.get("ingest_result") or {}).get("document_id")

    return {
        "mode": "upload",
        "endpoint": args.endpoint,
        "server": {
            "name": initialize_result.serverInfo.name,
            "version": initialize_result.serverInfo.version,
        },
        "session": session_payload,
        "transfer_url": upload_url,
        "transfer": transfer_result,
        "document_id": document_id,
    }


async def _run_download(args: argparse.Namespace) -> dict[str, Any]:
    access_token = args.access_token or DEFAULT_ACCESS_TOKEN
    if not access_token:
        raise RuntimeError("An Athena MCP bearer token is required. Use --access-token or MCP_ACCESS_TOKEN.")

    async with _mcp_session(args.endpoint, access_token, args.timeout) as (session, initialize_result):
        session_payload = await _create_download_session(
            session,
            args.document_id,
            disposition=args.disposition,
            expires_in_seconds=args.expires_in_seconds,
        )

    download_url = _rewrite_url_base(str(session_payload["download_url"]), args.transfer_base_url)
    head_result = head_download(download_url=download_url, timeout=args.transfer_timeout)
    transfer_result = download_file(
        download_url=download_url,
        output_path=args.output,
        byte_range=args.byte_range,
        overwrite=args.overwrite,
        timeout=args.transfer_timeout,
    )

    return {
        "mode": "download",
        "endpoint": args.endpoint,
        "server": {
            "name": initialize_result.serverInfo.name,
            "version": initialize_result.serverInfo.version,
        },
        "session": session_payload,
        "transfer_url": download_url,
        "head": head_result,
        "transfer": transfer_result,
    }


async def _run_roundtrip(args: argparse.Namespace) -> dict[str, Any]:
    upload_args = argparse.Namespace(**vars(args))
    upload_result = await _run_upload(upload_args)
    document_id = upload_result.get("document_id")
    if not document_id:
        raise RuntimeError(f"Upload completed without document_id: {upload_result}")

    download_args = argparse.Namespace(**vars(args))
    download_args.document_id = document_id
    download_args.byte_range = getattr(download_args, "byte_range", None)
    download_result = await _run_download(download_args)

    original_path = Path(args.file).expanduser().resolve()
    output_path = Path(download_result["transfer"]["output"]).expanduser().resolve()
    same_bytes = original_path.read_bytes() == output_path.read_bytes()

    return {
        "mode": "roundtrip",
        "document_id": document_id,
        "upload": upload_result,
        "download": download_result,
        "bytes_match": same_bytes,
        "sha256": {
            "input": _sha256_file(original_path),
            "output": _sha256_file(output_path),
        },
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Athena MCP transfer client")
    parser.add_argument(
        "--endpoint",
        default=DEFAULT_MCP_ENDPOINT,
        help=f"Athena MCP Streamable HTTP endpoint (default: {DEFAULT_MCP_ENDPOINT})",
    )
    parser.add_argument(
        "--access-token",
        help="Bearer token for the Athena MCP endpoint (defaults to MCP_ACCESS_TOKEN env var)",
    )
    parser.add_argument(
        "--transfer-base-url",
        help="Optional base URL used to rewrite upload/download session URLs for local testing",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_CONNECT_TIMEOUT,
        help="MCP connection timeout in seconds",
    )
    parser.add_argument(
        "--transfer-timeout",
        type=float,
        default=DEFAULT_TRANSFER_TIMEOUT,
        help="Raw HTTP transfer timeout in seconds",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    upload_parser = subparsers.add_parser("upload", help="Create an upload session via MCP, then upload a file")
    upload_parser.add_argument("--file", required=True, help="Path to the file to upload")
    upload_parser.add_argument("--domain", default="general", help="Athena document domain")
    upload_parser.add_argument("--content-type", help="Optional MIME type override")
    upload_parser.add_argument("--title", help="Optional document title")
    upload_parser.add_argument("--document-type", help="Optional Athena document type")
    upload_parser.add_argument("--description", help="Optional document description")
    upload_parser.add_argument("--expires-in-seconds", type=int, help="Optional upload session TTL override")
    upload_parser.add_argument("--sha256", help="Optional expected SHA-256 digest")
    upload_parser.add_argument(
        "--compute-sha256",
        action="store_true",
        help="Compute the file SHA-256 locally and send it with the upload session request",
    )
    upload_parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_UPLOAD_CHUNK_SIZE,
        help="Upload chunk size in bytes",
    )

    download_parser = subparsers.add_parser("download", help="Create a download session via MCP, then download bytes")
    download_parser.add_argument("--document-id", required=True, help="Athena document UUID to download")
    download_parser.add_argument("--output", required=True, help="Output file path")
    download_parser.add_argument(
        "--disposition",
        default="attachment",
        choices=["attachment", "inline"],
        help="Requested content disposition",
    )
    download_parser.add_argument("--expires-in-seconds", type=int, help="Optional download session TTL override")
    download_parser.add_argument("--range", dest="byte_range", help="Optional byte range like 0-1023")
    download_parser.add_argument("--overwrite", action="store_true", help="Overwrite the output file if it exists")

    roundtrip_parser = subparsers.add_parser(
        "roundtrip",
        help="Create upload and download sessions via MCP and verify byte-for-byte round-trip",
    )
    roundtrip_parser.add_argument("--file", required=True, help="Path to the file to upload")
    roundtrip_parser.add_argument("--output", required=True, help="Where to save the downloaded copy")
    roundtrip_parser.add_argument("--domain", default="general", help="Athena document domain")
    roundtrip_parser.add_argument("--content-type", help="Optional MIME type override")
    roundtrip_parser.add_argument("--title", help="Optional document title")
    roundtrip_parser.add_argument("--document-type", help="Optional Athena document type")
    roundtrip_parser.add_argument("--description", help="Optional document description")
    roundtrip_parser.add_argument("--expires-in-seconds", type=int, help="Optional session TTL override")
    roundtrip_parser.add_argument(
        "--disposition",
        default="attachment",
        choices=["attachment", "inline"],
        help="Requested content disposition for the download session",
    )
    roundtrip_parser.add_argument(
        "--compute-sha256",
        action="store_true",
        help="Compute and send the expected upload SHA-256",
    )
    roundtrip_parser.add_argument("--sha256", help="Optional expected SHA-256 digest")
    roundtrip_parser.add_argument("--range", dest="byte_range", help="Optional byte range like 0-1023")
    roundtrip_parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_UPLOAD_CHUNK_SIZE,
        help="Upload chunk size in bytes",
    )
    roundtrip_parser.add_argument("--overwrite", action="store_true", help="Overwrite the output file if it exists")

    return parser


async def _async_main(argv: Optional[list[str]] = None) -> dict[str, Any]:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "upload":
        return await _run_upload(args)
    if args.command == "download":
        return await _run_download(args)
    if args.command == "roundtrip":
        return await _run_roundtrip(args)

    parser.error(f"Unsupported command: {args.command}")
    raise SystemExit(2)


def main(argv: Optional[list[str]] = None) -> int:
    result = asyncio.run(_async_main(argv))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(json.dumps({"error": str(exc)}, indent=2, sort_keys=True), file=sys.stderr)
        raise SystemExit(1)
