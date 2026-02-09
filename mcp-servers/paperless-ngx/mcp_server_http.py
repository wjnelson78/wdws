#!/usr/bin/env python3
"""HTTP MCP Server for Paperless-ngx with OAuth 2.0 authentication."""
import os
import json
import asyncio
from typing import Any, Optional
from functools import wraps

from starlette.applications import Starlette
from starlette.routing import Route, Mount
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, HTMLResponse, RedirectResponse
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
import uvicorn

from mcp.server import Server
from mcp.server.sse import SseServerTransport
from mcp.types import Tool, TextContent

from paperless_client import PaperlessClient, PaperlessConfig
from oauth import OAuthProvider, OAuthConfig, TokenResponse, get_oauth_provider

# Initialize the MCP server
mcp_server = Server("paperless-ngx")

# Lazy initialization
_client: Optional[PaperlessClient] = None


def get_client() -> PaperlessClient:
    global _client
    if _client is None:
        _client = PaperlessClient()
    return _client


# ============= OAuth Endpoints =============

def _get_public_base_url(request: Request) -> str:
    """Resolve the public base URL, honoring proxy headers."""
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("x-forwarded-host", request.headers.get("host", request.url.netloc))
    return f"{scheme}://{host}".rstrip("/")


def _get_resource_url(request: Request, base_url: str, well_known_prefix: str) -> str:
    """Resolve resource URL from well-known suffixes like /mcp/sse."""
    rest = request.path_params.get("rest")
    if rest:
        return f"{base_url}/" + rest.lstrip("/")

    path = request.url.path
    if path.startswith(well_known_prefix) and len(path) > len(well_known_prefix):
        suffix = path[len(well_known_prefix):]
        return f"{base_url}{suffix}"

    return base_url


async def oauth_token(request: Request) -> JSONResponse:
    """OAuth 2.0 token endpoint."""
    oauth = get_oauth_provider()
    
    # Parse request body
    if request.headers.get("content-type") == "application/json":
        data = await request.json()
    else:
        form = await request.form()
        data = dict(form)

    # Support Basic auth for client credentials
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Basic ") and ("client_id" not in data or "client_secret" not in data):
        import base64
        try:
            decoded = base64.b64decode(auth_header[6:]).decode()
            basic_client_id, basic_client_secret = decoded.split(":", 1)
            data.setdefault("client_id", basic_client_id)
            data.setdefault("client_secret", basic_client_secret)
        except Exception:
            return JSONResponse(
                {"error": "invalid_client", "error_description": "Invalid basic auth header"},
                status_code=401
            )
    
    grant_type = data.get("grant_type")
    client_id = data.get("client_id")
    client_secret = data.get("client_secret")
    has_client_secret = bool(client_secret)
    print(f"OAuth token request: grant_type={grant_type}, client_id={client_id}, has_client_secret={has_client_secret}")
    
    if grant_type == "client_credentials":
        scope = data.get("scope")
        
        if not client_id or not client_secret:
            return JSONResponse(
                {"error": "invalid_request", "error_description": "Missing client credentials"},
                status_code=400
            )
        
        token_response = oauth.client_credentials_grant(client_id, client_secret, scope)
        
        if not token_response:
            return JSONResponse(
                {"error": "invalid_client", "error_description": "Invalid client credentials"},
                status_code=401
            )
        
        return JSONResponse({
            "access_token": token_response.access_token,
            "token_type": token_response.token_type,
            "expires_in": token_response.expires_in,
            "refresh_token": token_response.refresh_token,
            "scope": token_response.scope
        })
    
    elif grant_type == "refresh_token":
        refresh_token = data.get("refresh_token")
        scope = data.get("scope")
        
        if not refresh_token:
            return JSONResponse(
                {"error": "invalid_request", "error_description": "Missing refresh token"},
                status_code=400
            )
        
        token_response = oauth.refresh_token_grant(refresh_token, scope)
        
        if not token_response:
            return JSONResponse(
                {"error": "invalid_grant", "error_description": "Invalid refresh token"},
                status_code=400
            )
        
        return JSONResponse({
            "access_token": token_response.access_token,
            "token_type": token_response.token_type,
            "expires_in": token_response.expires_in,
            "refresh_token": token_response.refresh_token,
            "scope": token_response.scope
        })

    elif grant_type == "authorization_code":
        code = data.get("code")
        redirect_uri = data.get("redirect_uri")
        code_verifier = data.get("code_verifier")
        print(f"OAuth auth_code exchange: client_id={client_id}, redirect_uri={redirect_uri}, code_verifier={'yes' if code_verifier else 'no'}")
        
        if not code or not client_id or not redirect_uri:
            return JSONResponse(
                {"error": "invalid_request", "error_description": "Missing code, client_id, or redirect_uri"},
                status_code=400
            )

        client = oauth.get_client(client_id)
        if not client:
            return JSONResponse(
                {"error": "invalid_client", "error_description": "Unknown client"},
                status_code=401
            )

        auth_method = client.get("token_endpoint_auth_method", "client_secret_post")
        if auth_method != "none":
            if not client_secret or not oauth.validate_client(client_id, client_secret):
                return JSONResponse(
                    {"error": "invalid_client", "error_description": "Invalid client credentials"},
                    status_code=401
                )

        token_response = oauth.exchange_authorization_code(
            code=code,
            client_id=client_id,
            redirect_uri=redirect_uri,
            code_verifier=code_verifier
        )

        if not token_response:
            return JSONResponse(
                {"error": "invalid_grant", "error_description": "Invalid or expired code"},
                status_code=400
            )

        return JSONResponse({
            "access_token": token_response.access_token,
            "token_type": token_response.token_type,
            "expires_in": token_response.expires_in,
            "refresh_token": token_response.refresh_token,
            "scope": token_response.scope
        })

    else:
        return JSONResponse(
            {"error": "unsupported_grant_type"},
            status_code=400
        )


async def oauth_authorize(request: Request) -> Response:
    """OAuth 2.0 authorization endpoint."""
    oauth = get_oauth_provider()
    
    params = dict(request.query_params)
    client_id = params.get("client_id", "")
    redirect_uri = params.get("redirect_uri", "")
    response_type = params.get("response_type", "")
    scope = params.get("scope", "")
    state = params.get("state", "")
    code_challenge = params.get("code_challenge")
    code_challenge_method = params.get("code_challenge_method")
    
    print(f"OAuth authorize request: client_id={client_id}, redirect_uri={redirect_uri}")
    
    if response_type != "code":
        return JSONResponse(
            {"error": "unsupported_response_type"},
            status_code=400
        )
    
    # Validate client
    client = oauth.get_client(client_id) if client_id else None
    if not client:
        return JSONResponse(
            {"error": "invalid_client", "error_description": "Unknown client_id"},
            status_code=400
        )
    
    # Auto-approve for now (in production, show consent screen)
    auto_approve = os.environ.get("MCP_OAUTH_AUTO_APPROVE", "true").lower() == "true"
    
    if auto_approve:
        scopes = scope.split() if scope else ["read", "write", "search"]
        code = oauth.create_authorization_code(
            client_id=client_id,
            redirect_uri=redirect_uri,
            scopes=scopes,
            code_challenge=code_challenge,
            code_challenge_method=code_challenge_method or "plain",
            subject="chatgpt-user"
        )
        
        # Redirect with auth code
        separator = "&" if "?" in redirect_uri else "?"
        redirect_url = f"{redirect_uri}{separator}code={code}"
        if state:
            redirect_url += f"&state={state}"
        
        print(f"OAuth authorize success, redirecting to: {redirect_url[:100]}...")
        return RedirectResponse(url=redirect_url, status_code=302)
    
    # Show consent form
    return HTMLResponse(f"""
    <!DOCTYPE html>
    <html>
    <head><title>Authorize Paperless-ngx MCP</title></head>
    <body>
        <h1>Authorize Application</h1>
        <p>Application <strong>{client_id}</strong> wants to access Paperless-ngx MCP Server.</p>
        <p>Requested scopes: <strong>{scope or "default"}</strong></p>
        <form method="POST">
            <input type="hidden" name="client_id" value="{client_id}">
            <input type="hidden" name="redirect_uri" value="{redirect_uri}">
            <input type="hidden" name="scope" value="{scope}">
            <input type="hidden" name="state" value="{state}">
            <input type="hidden" name="code_challenge" value="{code_challenge or ''}">
            <input type="hidden" name="code_challenge_method" value="{code_challenge_method or ''}">
            <button type="submit" name="action" value="approve">Approve</button>
            <button type="submit" name="action" value="deny">Deny</button>
        </form>
    </body>
    </html>
    """)


async def oauth_authorize_post(request: Request) -> Response:
    """Handle OAuth authorization form submission."""
    oauth = get_oauth_provider()
    form = await request.form()
    
    action = form.get("action")
    client_id = form.get("client_id", "")
    redirect_uri = form.get("redirect_uri", "")
    scope = form.get("scope", "")
    state = form.get("state", "")
    code_challenge = form.get("code_challenge") or None
    code_challenge_method = form.get("code_challenge_method") or None
    
    if action == "deny":
        separator = "&" if "?" in redirect_uri else "?"
        return RedirectResponse(
            url=f"{redirect_uri}{separator}error=access_denied&state={state}",
            status_code=302
        )
    
    scopes = scope.split() if scope else ["read", "write", "search"]
    code = oauth.create_authorization_code(
        client_id=client_id,
        redirect_uri=redirect_uri,
        scopes=scopes,
        code_challenge=code_challenge,
        code_challenge_method=code_challenge_method or "plain",
        subject="chatgpt-user"
    )
    
    separator = "&" if "?" in redirect_uri else "?"
    redirect_url = f"{redirect_uri}{separator}code={code}"
    if state:
        redirect_url += f"&state={state}"
    
    return RedirectResponse(url=redirect_url, status_code=302)


async def oauth_register(request: Request) -> JSONResponse:
    """Dynamic Client Registration endpoint."""
    oauth = get_oauth_provider()
    
    try:
        data = await request.json()
    except:
        return JSONResponse({"error": "invalid_request"}, status_code=400)
    
    # Set defaults for public clients (ChatGPT uses token_endpoint_auth_method=none)
    if data.get("token_endpoint_auth_method") == "none":
        data.setdefault("grant_types", ["authorization_code", "refresh_token"])
        data.setdefault("response_types", ["code"])
    
    result = oauth.register_client(data)
    
    if "error" in result:
        return JSONResponse(result, status_code=400)
    
    return JSONResponse(result, status_code=201)


async def oauth_metadata(request: Request) -> JSONResponse:
    """OAuth 2.0 Authorization Server Metadata."""
    base_url = os.environ.get("MCP_PUBLIC_BASE_URL") or _get_public_base_url(request)
    
    return JSONResponse({
        "issuer": base_url,
        "authorization_endpoint": f"{base_url}/oauth/authorize",
        "token_endpoint": f"{base_url}/oauth/token",
        "revocation_endpoint": f"{base_url}/oauth/revoke",
        "introspection_endpoint": f"{base_url}/oauth/introspect",
        "registration_endpoint": f"{base_url}/oauth/register",
        "token_endpoint_auth_methods_supported": ["none", "client_secret_post", "client_secret_basic"],
        "grant_types_supported": ["authorization_code", "client_credentials", "refresh_token"],
        "scopes_supported": ["read", "write", "search"],
        "response_types_supported": ["code"],
        "code_challenge_methods_supported": ["S256", "plain"],
        "service_documentation": f"{base_url}/docs"
    })


async def openid_configuration(request: Request) -> JSONResponse:
    """OpenID Connect discovery endpoint (compatibility)."""
    base_url = os.environ.get("MCP_PUBLIC_BASE_URL") or _get_public_base_url(request)

    return JSONResponse({
        "issuer": base_url,
        "authorization_endpoint": f"{base_url}/oauth/authorize",
        "token_endpoint": f"{base_url}/oauth/token",
        "revocation_endpoint": f"{base_url}/oauth/revoke",
        "introspection_endpoint": f"{base_url}/oauth/introspect",
        "registration_endpoint": f"{base_url}/oauth/register",
        "token_endpoint_auth_methods_supported": ["none", "client_secret_post", "client_secret_basic"],
        "grant_types_supported": ["authorization_code", "client_credentials", "refresh_token"],
        "scopes_supported": ["read", "write", "search"],
        "response_types_supported": ["code"],
        "code_challenge_methods_supported": ["S256", "plain"]
    })


async def mcp_well_known(request: Request) -> JSONResponse:
    """MCP server metadata for discovery."""
    base_url = os.environ.get("MCP_PUBLIC_BASE_URL") or _get_public_base_url(request)

    return JSONResponse({
        "name": "paperless-ngx",
        "description": "Paperless-ngx MCP Server",
        "version": "1.0",
        "mcp": {
            "transport": "sse",
            "sse_endpoint": f"{base_url}/mcp/sse",
            "messages_endpoint": f"{base_url}/mcp/messages/"
        },
        "oauth": {
            "authorization_endpoint": f"{base_url}/oauth/authorize",
            "token_endpoint": f"{base_url}/oauth/token",
            "registration_endpoint": f"{base_url}/oauth/register",
            "scopes_supported": ["read", "write", "search"]
        }
    })


async def protected_resource_metadata(request: Request) -> JSONResponse:
    """OAuth 2.0 Protected Resource Metadata."""
    base_url = os.environ.get("MCP_PUBLIC_BASE_URL") or _get_public_base_url(request)
    resource_url = _get_resource_url(request, base_url, "/.well-known/oauth-protected-resource")
    
    return JSONResponse({
        "resource": resource_url,
        "authorization_servers": [base_url],
        "scopes_supported": ["read", "write", "search"],
        "bearer_methods_supported": ["header"]
    })


async def health_check(request: Request) -> JSONResponse:
    """Health check endpoint."""
    return JSONResponse({"status": "healthy", "service": "paperless-ngx-mcp"})


# ============= MCP Tools =============

# Cache for search->fetch flows
_document_cache: dict[str, dict] = {}

@mcp_server.list_tools()
async def list_tools() -> list[Tool]:
    """List available MCP tools."""
    return [
        Tool(
            name="search",
            description="Search for documents in Paperless-ngx (ChatGPT-compatible).",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query to find documents"
                    },
                    "page": {
                        "type": "integer",
                        "description": "Page number (default 1)",
                        "default": 1
                    },
                    "page_size": {
                        "type": "integer",
                        "description": "Results per page (default 10)",
                        "default": 10
                    }
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="fetch",
            description="Fetch a document by ID and return full text (ChatGPT-compatible).",
            inputSchema={
                "type": "object",
                "properties": {
                    "id": {
                        "type": "string",
                        "description": "Document ID to fetch"
                    }
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
                    "query": {
                        "type": "string",
                        "description": "Search query to find documents"
                    },
                    "page": {
                        "type": "integer",
                        "description": "Page number (default 1)",
                        "default": 1
                    },
                    "page_size": {
                        "type": "integer",
                        "description": "Results per page (default 25)",
                        "default": 25
                    }
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
                    "document_id": {
                        "type": "integer",
                        "description": "The document ID"
                    }
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
                    "document_id": {
                        "type": "integer",
                        "description": "The document ID"
                    }
                },
                "required": ["document_id"]
            }
        ),
        Tool(
            name="list_documents",
            description="List documents with optional filters for correspondent, document type, or tags",
            inputSchema={
                "type": "object",
                "properties": {
                    "page": {
                        "type": "integer",
                        "description": "Page number",
                        "default": 1
                    },
                    "page_size": {
                        "type": "integer",
                        "description": "Results per page",
                        "default": 25
                    },
                    "correspondent_id": {
                        "type": "integer",
                        "description": "Filter by correspondent ID"
                    },
                    "document_type_id": {
                        "type": "integer",
                        "description": "Filter by document type ID"
                    },
                    "tag_ids": {
                        "type": "array",
                        "items": {"type": "integer"},
                        "description": "Filter by tag IDs (documents must have ALL specified tags)"
                    }
                }
            }
        ),
        Tool(
            name="list_tags",
            description="List all tags in Paperless-ngx",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="list_correspondents",
            description="List all correspondents (senders/recipients) in Paperless-ngx",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="list_document_types",
            description="List all document types in Paperless-ngx",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="get_statistics",
            description="Get Paperless-ngx system statistics (document count, storage, etc.)",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="update_document",
            description="Update document metadata like title, correspondent, document type, or tags",
            inputSchema={
                "type": "object",
                "properties": {
                    "document_id": {
                        "type": "integer",
                        "description": "The document ID to update"
                    },
                    "title": {
                        "type": "string",
                        "description": "New title for the document"
                    },
                    "correspondent_id": {
                        "type": "integer",
                        "description": "Set correspondent by ID"
                    },
                    "document_type_id": {
                        "type": "integer",
                        "description": "Set document type by ID"
                    },
                    "tag_ids": {
                        "type": "array",
                        "items": {"type": "integer"},
                        "description": "Set tags by IDs (replaces existing tags)"
                    }
                },
                "required": ["document_id"]
            }
        ),
        Tool(
            name="create_tag",
            description="Create a new tag in Paperless-ngx",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Name of the new tag"
                    },
                    "color": {
                        "type": "string",
                        "description": "Hex color for the tag (e.g., '#ff0000')"
                    }
                },
                "required": ["name"]
            }
        ),
        Tool(
            name="create_correspondent",
            description="Create a new correspondent in Paperless-ngx",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Name of the correspondent"
                    }
                },
                "required": ["name"]
            }
        ),
        Tool(
            name="create_document_type",
            description="Create a new document type in Paperless-ngx",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Name of the document type"
                    }
                },
                "required": ["name"]
            }
        )
    ]


@mcp_server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Execute an MCP tool."""
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

        if name == "search_documents":
            result = client.search_documents(
                query=arguments["query"],
                page=arguments.get("page", 1),
                page_size=arguments.get("page_size", 25)
            )
        
        elif name == "get_document":
            result = client.get_document(arguments["document_id"])
        
        elif name == "get_document_content":
            content = client.get_document_content(arguments["document_id"])
            result = {"document_id": arguments["document_id"], "content": content}
        
        elif name == "list_documents":
            result = client.list_documents(
                page=arguments.get("page", 1),
                page_size=arguments.get("page_size", 25),
                correspondent=arguments.get("correspondent_id"),
                document_type=arguments.get("document_type_id"),
                tags__id__all=arguments.get("tag_ids")
            )
        
        elif name == "list_tags":
            result = client.list_tags()
        
        elif name == "list_correspondents":
            result = client.list_correspondents()
        
        elif name == "list_document_types":
            result = client.list_document_types()
        
        elif name == "get_statistics":
            result = client.get_statistics()
        
        elif name == "update_document":
            update_data = {}
            if "title" in arguments:
                update_data["title"] = arguments["title"]
            if "correspondent_id" in arguments:
                update_data["correspondent"] = arguments["correspondent_id"]
            if "document_type_id" in arguments:
                update_data["document_type"] = arguments["document_type_id"]
            if "tag_ids" in arguments:
                update_data["tags"] = arguments["tag_ids"]
            result = client.update_document(arguments["document_id"], **update_data)
        
        elif name == "create_tag":
            result = client.create_tag(
                name=arguments["name"],
                color=arguments.get("color")
            )
        
        elif name == "create_correspondent":
            result = client.create_correspondent(name=arguments["name"])
        
        elif name == "create_document_type":
            result = client.create_document_type(name=arguments["name"])
        
        else:
            result = {"error": f"Unknown tool: {name}"}
        
        return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
    
    except Exception as e:
        return [TextContent(type="text", text=json.dumps({"error": str(e)}))]


# ============= Token Validation Middleware =============

def require_oauth(func):
    """Decorator to require valid OAuth token for MCP endpoints."""
    @wraps(func)
    async def wrapper(request: Request, *args, **kwargs):
        if not os.environ.get("MCP_REQUIRE_OAUTH", "true").lower() == "true":
            return await func(request, *args, **kwargs)
        
        oauth = get_oauth_provider()
        auth_header = request.headers.get("Authorization", "")
        
        if not auth_header.startswith("Bearer "):
            return JSONResponse(
                {"error": "unauthorized", "error_description": "Missing or invalid Authorization header"},
                status_code=401,
                headers={"WWW-Authenticate": "Bearer"}
            )
        
        token = auth_header[7:]
        token_info = oauth.validate_access_token(token)
        
        if not token_info:
            return JSONResponse(
                {"error": "invalid_token", "error_description": "Token is invalid or expired"},
                status_code=401,
                headers={"WWW-Authenticate": 'Bearer error="invalid_token"'}
            )
        
        request.state.token_info = token_info
        return await func(request, *args, **kwargs)
    
    return wrapper


# ============= SSE Transport =============

sse_transport = SseServerTransport("/mcp/messages/")
sse_transport_alt = SseServerTransport("/messages/")


@require_oauth
async def handle_sse(request: Request) -> Response:
    """Handle SSE connections for MCP."""
    transport = sse_transport if request.url.path.startswith("/mcp") else sse_transport_alt
    async with transport.connect_sse(
        request.scope, request.receive, request._send
    ) as streams:
        await mcp_server.run(
            streams[0], streams[1], mcp_server.create_initialization_options()
        )
    return Response()


async def handle_messages(request: Request) -> Response:
    """Handle MCP messages over SSE."""
    transport = sse_transport if request.url.path.startswith("/mcp") else sse_transport_alt
    return await transport.handle_post_message(
        request.scope, request.receive, request._send
    )


# ============= Application Setup =============

routes = [
    Route("/health", health_check),
    Route("/.well-known/mcp.json", mcp_well_known),
    Route("/.well-known/oauth-authorization-server", oauth_metadata),
    Route("/.well-known/oauth-protected-resource/{rest:path}", protected_resource_metadata),
    Route("/.well-known/oauth-protected-resource", protected_resource_metadata),
    Route("/.well-known/openid-configuration", openid_configuration),
    Route("/.well-known/openid-configuration/{rest:path}", openid_configuration),
    Route("/oauth/token", oauth_token, methods=["POST"]),
    Route("/oauth/authorize", oauth_authorize, methods=["GET"]),
    Route("/oauth/authorize", oauth_authorize_post, methods=["POST"]),
    Route("/oauth/register", oauth_register, methods=["POST"]),
    Route("/mcp/sse", handle_sse),
    Route("/sse", handle_sse),
    Route("/mcp/messages/", handle_messages, methods=["POST"]),
    Route("/messages/{path:path}", handle_messages, methods=["POST"]),
    Route("/messages/", handle_messages, methods=["POST"]),
]

middleware = [
    Middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
]

app = Starlette(routes=routes, middleware=middleware)


if __name__ == "__main__":
    port = int(os.environ.get("MCP_PORT", "9001"))
    print(f"Starting Paperless-ngx MCP Server on port {port}")
    print(f"OAuth enabled: {os.environ.get('MCP_REQUIRE_OAUTH', 'true')}")
    uvicorn.run(app, host="0.0.0.0", port=port)
