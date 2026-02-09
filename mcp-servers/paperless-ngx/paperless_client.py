#!/usr/bin/env python3
"""Paperless-ngx API client."""
import os
import json
import base64
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from urllib.request import Request, urlopen
from urllib.parse import urlencode, quote
from urllib.error import HTTPError
import ssl


@dataclass
class PaperlessConfig:
    """Configuration for Paperless-ngx connection."""
    base_url: str
    api_token: str
    verify_ssl: bool = True
    
    @classmethod
    def from_env(cls) -> "PaperlessConfig":
        return cls(
            base_url=os.environ.get("PAPERLESS_URL", "http://172.16.32.11:8000"),
            api_token=os.environ.get("PAPERLESS_API_TOKEN", ""),
            verify_ssl=os.environ.get("PAPERLESS_VERIFY_SSL", "true").lower() == "true"
        )


class PaperlessClient:
    """Client for interacting with Paperless-ngx API."""
    
    def __init__(self, config: Optional[PaperlessConfig] = None):
        self.config = config or PaperlessConfig.from_env()
        self.base_url = self.config.base_url.rstrip("/")
        self._token: Optional[str] = None
        
        # SSL context
        if self.config.verify_ssl:
            self._ssl_context = ssl.create_default_context()
        else:
            self._ssl_context = ssl.create_default_context()
            self._ssl_context.check_hostname = False
            self._ssl_context.verify_mode = ssl.CERT_NONE
    
    def _get_auth_header(self) -> str:
        """Get authentication header (API Token)."""
        return f"Token {self.config.api_token}"
    
    def _request(self, method: str, endpoint: str, data: Optional[Dict] = None, 
                 params: Optional[Dict] = None) -> Dict[str, Any]:
        """Make an authenticated request to Paperless-ngx API."""
        url = f"{self.base_url}/api{endpoint}"
        
        if params:
            url += "?" + urlencode(params)
        
        headers = {
            "Authorization": self._get_auth_header(),
            "Accept": "application/json",
            "User-Agent": "Paperless-MCP/1.0",
        }
        
        body = None
        if data:
            headers["Content-Type"] = "application/json"
            body = json.dumps(data).encode()
        
        req = Request(url, data=body, headers=headers, method=method)
        
        try:
            with urlopen(req, context=self._ssl_context, timeout=30) as response:
                return json.loads(response.read().decode())
        except HTTPError as e:
            error_body = e.read().decode()
            raise Exception(f"Paperless API error {e.code}: {error_body}")
    
    def _get(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        return self._request("GET", endpoint, params=params)
    
    def _post(self, endpoint: str, data: Dict) -> Dict[str, Any]:
        return self._request("POST", endpoint, data=data)
    
    def _patch(self, endpoint: str, data: Dict) -> Dict[str, Any]:
        return self._request("PATCH", endpoint, data=data)
    
    def _delete(self, endpoint: str) -> Dict[str, Any]:
        return self._request("DELETE", endpoint)
    
    # ============= Document Operations =============
    
    def search_documents(self, query: str, page: int = 1, page_size: int = 25) -> Dict[str, Any]:
        """Search documents by content or title."""
        return self._get("/documents/", params={
            "query": query,
            "page": page,
            "page_size": page_size
        })
    
    def get_document(self, doc_id: int) -> Dict[str, Any]:
        """Get a specific document by ID."""
        return self._get(f"/documents/{doc_id}/")
    
    def get_document_content(self, doc_id: int) -> str:
        """Get the text content of a document."""
        doc = self.get_document(doc_id)
        return doc.get("content", "")
    
    def list_documents(self, page: int = 1, page_size: int = 25, 
                       ordering: str = "-created",
                       correspondent: Optional[int] = None,
                       document_type: Optional[int] = None,
                       tags__id__all: Optional[List[int]] = None) -> Dict[str, Any]:
        """List documents with optional filters."""
        params = {
            "page": page,
            "page_size": page_size,
            "ordering": ordering
        }
        if correspondent:
            params["correspondent__id"] = correspondent
        if document_type:
            params["document_type__id"] = document_type
        if tags__id__all:
            params["tags__id__all"] = ",".join(map(str, tags__id__all))
        
        return self._get("/documents/", params=params)
    
    def get_document_download_url(self, doc_id: int, original: bool = False) -> str:
        """Get the download URL for a document."""
        if original:
            return f"{self.base_url}/api/documents/{doc_id}/download/?original=true"
        return f"{self.base_url}/api/documents/{doc_id}/download/"
    
    def get_document_preview_url(self, doc_id: int) -> str:
        """Get the preview/thumbnail URL for a document."""
        return f"{self.base_url}/api/documents/{doc_id}/preview/"
    
    def update_document(self, doc_id: int, **kwargs) -> Dict[str, Any]:
        """Update document metadata (title, correspondent, document_type, tags, etc.)."""
        return self._patch(f"/documents/{doc_id}/", kwargs)
    
    # ============= Tags =============
    
    def list_tags(self, page: int = 1, page_size: int = 100) -> Dict[str, Any]:
        """List all tags."""
        return self._get("/tags/", params={"page": page, "page_size": page_size})
    
    def get_tag(self, tag_id: int) -> Dict[str, Any]:
        """Get a specific tag."""
        return self._get(f"/tags/{tag_id}/")
    
    def create_tag(self, name: str, color: Optional[str] = None, 
                   match: Optional[str] = None, matching_algorithm: int = 0) -> Dict[str, Any]:
        """Create a new tag."""
        data = {"name": name, "matching_algorithm": matching_algorithm}
        if color:
            data["color"] = color
        if match:
            data["match"] = match
        return self._post("/tags/", data)
    
    # ============= Correspondents =============
    
    def list_correspondents(self, page: int = 1, page_size: int = 100) -> Dict[str, Any]:
        """List all correspondents."""
        return self._get("/correspondents/", params={"page": page, "page_size": page_size})
    
    def get_correspondent(self, corr_id: int) -> Dict[str, Any]:
        """Get a specific correspondent."""
        return self._get(f"/correspondents/{corr_id}/")
    
    def create_correspondent(self, name: str, match: Optional[str] = None,
                             matching_algorithm: int = 0) -> Dict[str, Any]:
        """Create a new correspondent."""
        data = {"name": name, "matching_algorithm": matching_algorithm}
        if match:
            data["match"] = match
        return self._post("/correspondents/", data)
    
    # ============= Document Types =============
    
    def list_document_types(self, page: int = 1, page_size: int = 100) -> Dict[str, Any]:
        """List all document types."""
        return self._get("/document_types/", params={"page": page, "page_size": page_size})
    
    def get_document_type(self, type_id: int) -> Dict[str, Any]:
        """Get a specific document type."""
        return self._get(f"/document_types/{type_id}/")
    
    def create_document_type(self, name: str, match: Optional[str] = None,
                             matching_algorithm: int = 0) -> Dict[str, Any]:
        """Create a new document type."""
        data = {"name": name, "matching_algorithm": matching_algorithm}
        if match:
            data["match"] = match
        return self._post("/document_types/", data)
    
    # ============= Storage Paths =============
    
    def list_storage_paths(self, page: int = 1, page_size: int = 100) -> Dict[str, Any]:
        """List all storage paths."""
        return self._get("/storage_paths/", params={"page": page, "page_size": page_size})
    
    # ============= Saved Views =============
    
    def list_saved_views(self, page: int = 1, page_size: int = 100) -> Dict[str, Any]:
        """List all saved views."""
        return self._get("/saved_views/", params={"page": page, "page_size": page_size})
    
    # ============= Statistics =============
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get system statistics."""
        return self._get("/statistics/")
    
    # ============= Tasks =============
    
    def list_tasks(self) -> Dict[str, Any]:
        """List pending/running tasks."""
        return self._get("/tasks/")
    
    # ============= Logs =============
    
    def get_logs(self, page: int = 1, page_size: int = 100) -> Dict[str, Any]:
        """Get system logs."""
        return self._get("/logs/", params={"page": page, "page_size": page_size})


# Test connection
if __name__ == "__main__":
    client = PaperlessClient()
    print("Testing Paperless-ngx connection...")
    try:
        stats = client.get_statistics()
        print(f"✅ Connected! Stats: {json.dumps(stats, indent=2)}")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
