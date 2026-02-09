#!/usr/bin/env python3
"""Stdio MCP Server for Paperless-ngx (for Claude Desktop)."""
import os
import sys
import json
import asyncio
from typing import Any, Optional

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

from paperless_client import PaperlessClient, PaperlessConfig

# Initialize
mcp_server = Server("paperless-ngx")
_client: Optional[PaperlessClient] = None
_document_cache: dict[str, dict] = {}


def get_client() -> PaperlessClient:
    global _client
    if _client is None:
        _client = PaperlessClient()
    return _client


@mcp_server.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="search",
            description="Search for documents in Paperless-ngx.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query"},
                    "page": {"type": "integer", "default": 1},
                    "page_size": {"type": "integer", "default": 10}
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="fetch",
            description="Fetch a document by ID and return full text.",
            inputSchema={
                "type": "object",
                "properties": {
                    "id": {"type": "string", "description": "Document ID"}
                },
                "required": ["id"]
            }
        ),
        Tool(
            name="search_documents",
            description="Search for documents in Paperless-ngx by content, title, or metadata",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query"},
                    "page": {"type": "integer", "default": 1},
                    "page_size": {"type": "integer", "default": 25}
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="get_document",
            description="Get detailed information about a specific document by ID",
            inputSchema={
                "type": "object",
                "properties": {
                    "document_id": {"type": "integer", "description": "The document ID"}
                },
                "required": ["document_id"]
            }
        ),
        Tool(
            name="get_document_content",
            description="Get the full text content of a document",
            inputSchema={
                "type": "object",
                "properties": {
                    "document_id": {"type": "integer", "description": "The document ID"}
                },
                "required": ["document_id"]
            }
        ),
        Tool(
            name="list_documents",
            description="List documents with optional filters",
            inputSchema={
                "type": "object",
                "properties": {
                    "page": {"type": "integer", "default": 1},
                    "page_size": {"type": "integer", "default": 25},
                    "correspondent_id": {"type": "integer"},
                    "document_type_id": {"type": "integer"},
                    "tag_ids": {"type": "array", "items": {"type": "integer"}}
                }
            }
        ),
        Tool(
            name="list_tags",
            description="List all tags in Paperless-ngx",
            inputSchema={"type": "object", "properties": {}}
        ),
        Tool(
            name="list_correspondents",
            description="List all correspondents in Paperless-ngx",
            inputSchema={"type": "object", "properties": {}}
        ),
        Tool(
            name="list_document_types",
            description="List all document types in Paperless-ngx",
            inputSchema={"type": "object", "properties": {}}
        ),
        Tool(
            name="get_statistics",
            description="Get Paperless-ngx system statistics",
            inputSchema={"type": "object", "properties": {}}
        )
    ]


@mcp_server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    client = get_client()
    global _document_cache
    
    try:
        if name == "search":
            result = client.search_documents(
                query=arguments["query"],
                page=arguments.get("page", 1),
                page_size=arguments.get("page_size", 10)
            )

            search_results = []
            for doc in result.get("results", []):
                doc_id = doc.get("id")
                if doc_id is None:
                    continue
                doc_id_str = str(doc_id)
                title = doc.get("title") or doc.get("original_file_name") or f"Document {doc_id_str}"
                url = client.get_document_download_url(doc_id)

                _document_cache[doc_id_str] = {
                    "id": doc_id_str,
                    "title": title,
                    "url": url,
                    "metadata": doc
                }

                search_results.append({
                    "id": doc_id_str,
                    "title": title,
                    "url": url
                })

            return [TextContent(type="text", text=json.dumps({"results": search_results}, indent=2))]

        elif name == "fetch":
            doc_id_str = str(arguments["id"])

            if doc_id_str in _document_cache and _document_cache[doc_id_str].get("text"):
                return [TextContent(type="text", text=json.dumps(_document_cache[doc_id_str], indent=2, default=str))]

            try:
                doc_id = int(doc_id_str)
            except ValueError:
                return [TextContent(type="text", text=json.dumps({"error": "Invalid document id"}))]

            doc = client.get_document(doc_id)
            content = doc.get("content", "")
            title = doc.get("title") or doc.get("original_file_name") or f"Document {doc_id_str}"
            url = client.get_document_download_url(doc_id)

            payload = {
                "id": doc_id_str,
                "title": title,
                "text": content,
                "url": url,
                "metadata": doc
            }
            _document_cache[doc_id_str] = payload

            return [TextContent(type="text", text=json.dumps(payload, indent=2, default=str))]

        elif name == "search_documents":
            result = client.search_documents(
                query=arguments["query"],
                page=arguments.get("page", 1),
                page_size=arguments.get("page_size", 25)
            )
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
        
        elif name == "get_document":
            result = client.get_document(arguments["document_id"])
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
        
        elif name == "get_document_content":
            content = client.get_document_content(arguments["document_id"])
            result = {"document_id": arguments["document_id"], "content": content}
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
        
        elif name == "list_documents":
            result = client.list_documents(
                page=arguments.get("page", 1),
                page_size=arguments.get("page_size", 25),
                correspondent=arguments.get("correspondent_id"),
                document_type=arguments.get("document_type_id"),
                tags__id__all=arguments.get("tag_ids")
            )
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
        
        elif name == "list_tags":
            result = client.list_tags()
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
        
        elif name == "list_correspondents":
            result = client.list_correspondents()
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
        
        elif name == "list_document_types":
            result = client.list_document_types()
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
        
        elif name == "get_statistics":
            result = client.get_statistics()
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
        
        else:
            return [TextContent(type="text", text=json.dumps({"error": f"Unknown tool: {name}"}))]
    
    except Exception as e:
        return [TextContent(type="text", text=json.dumps({"error": str(e)}))]


async def main():
    async with stdio_server() as (read_stream, write_stream):
        await mcp_server.run(
            read_stream,
            write_stream,
            mcp_server.create_initialization_options()
        )


if __name__ == "__main__":
    asyncio.run(main())
