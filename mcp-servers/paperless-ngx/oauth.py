#!/usr/bin/env python3
"""OAuth 2.0 Authentication for MCP Server."""
import os
import secrets
import hashlib
import time
import json
from typing import Optional, Dict, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

import jwt
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend
import base64


@dataclass
class OAuthConfig:
    """OAuth 2.0 configuration."""
    client_id: str
    client_secret: str
    issuer: str = "nelson-court-cases-mcp"
    token_expiry_seconds: int = 3600  # 1 hour
    refresh_token_expiry_seconds: int = 86400 * 30  # 30 days
    jwt_secret: str = field(default_factory=lambda: secrets.token_hex(32))
    jwt_algorithm: str = "HS256"
    scopes: list = field(default_factory=lambda: ["read", "search", "rag"])


@dataclass
class TokenResponse:
    """OAuth token response."""
    access_token: str
    token_type: str = "Bearer"
    expires_in: int = 3600
    refresh_token: Optional[str] = None
    scope: str = ""


class OAuthProvider:
    """OAuth 2.0 provider for MCP server authentication."""
    
    def __init__(self, config: OAuthConfig, token_store_path: Optional[Path] = None):
        self.config = config
        self.token_store_path = token_store_path or Path(__file__).parent / ".oauth_tokens.json"
        self.client_store_path = Path(__file__).parent / ".oauth_clients.json"
        self._refresh_tokens: Dict[str, Dict[str, Any]] = {}
        self._revoked_tokens: set = set()
        self._clients: Dict[str, Dict[str, Any]] = {}
        self._auth_codes: Dict[str, Dict[str, Any]] = {}
        self._load_token_store()
        self._load_client_store()
        self._ensure_default_client()
    
    def _load_token_store(self):
        """Load persisted tokens from disk."""
        if self.token_store_path.exists():
            try:
                with open(self.token_store_path, 'r') as f:
                    data = json.load(f)
                    self._refresh_tokens = data.get("refresh_tokens", {})
                    self._revoked_tokens = set(data.get("revoked_tokens", []))
            except Exception:
                pass
    
    def _save_token_store(self):
        """Persist tokens to disk."""
        try:
            with open(self.token_store_path, 'w') as f:
                json.dump({
                    "refresh_tokens": self._refresh_tokens,
                    "revoked_tokens": list(self._revoked_tokens)
                }, f)
        except Exception:
            pass

    def _load_client_store(self):
        """Load persisted OAuth clients from disk."""
        if self.client_store_path.exists():
            try:
                with open(self.client_store_path, 'r') as f:
                    data = json.load(f)
                    self._clients = data.get("clients", {})
            except Exception:
                pass

    def _save_client_store(self):
        """Persist OAuth clients to disk."""
        try:
            with open(self.client_store_path, 'w') as f:
                json.dump({"clients": self._clients}, f)
        except Exception:
            pass

    def _ensure_default_client(self):
        """Ensure the configured client exists in the client registry."""
        if self.config.client_id not in self._clients:
            self._clients[self.config.client_id] = {
                "client_id": self.config.client_id,
                "client_secret_hash": self._hash_secret(self.config.client_secret),
                "client_name": "Default MCP Client",
                "redirect_uris": [],
                "grant_types": ["client_credentials", "refresh_token"],
                "scope": " ".join(self.config.scopes),
                "token_endpoint_auth_method": "client_secret_post",
                "client_id_issued_at": int(time.time()),
                "client_secret_expires_at": 0
            }
            self._save_client_store()
    
    def _hash_secret(self, secret: str) -> str:
        """Hash a client secret for comparison."""
        return hashlib.sha256(secret.encode()).hexdigest()

    def _normalize_redirect_uri(self, uri: Optional[str]) -> str:
        """Normalize redirect URIs for comparison."""
        if not uri:
            return ""
        return uri.rstrip("/")
    
    def validate_client(self, client_id: str, client_secret: str) -> bool:
        """Validate client credentials."""
        if client_id == self.config.client_id:
            return self._hash_secret(client_secret) == self._hash_secret(self.config.client_secret)

        client = self._clients.get(client_id)
        if not client:
            return False

        return self._hash_secret(client_secret) == client.get("client_secret_hash")

    def get_client(self, client_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a registered client by ID."""
        if client_id == self.config.client_id:
            return {
                "client_id": self.config.client_id,
                "client_secret_hash": self._hash_secret(self.config.client_secret),
                "client_name": "Default MCP Client",
                "redirect_uris": [],
                "grant_types": ["client_credentials", "refresh_token", "authorization_code"],
                "scope": " ".join(self.config.scopes),
                "token_endpoint_auth_method": "client_secret_post",
                "client_id_issued_at": int(time.time()),
                "client_secret_expires_at": 0
            }

        return self._clients.get(client_id)

    def register_client(
        self,
        client_name: str,
        redirect_uris: list[str],
        grant_types: list[str],
        scope: str,
        token_endpoint_auth_method: str = "none"
    ) -> Dict[str, Any]:
        """Register a new OAuth client (RFC 7591)."""
        client_id = f"client_{secrets.token_hex(8)}"
        client_secret = None
        client_secret_hash = None
        if token_endpoint_auth_method != "none":
            client_secret = secrets.token_hex(32)
            client_secret_hash = self._hash_secret(client_secret)
        issued_at = int(time.time())

        self._clients[client_id] = {
            "client_id": client_id,
            "client_secret_hash": client_secret_hash,
            "client_name": client_name,
            "redirect_uris": redirect_uris,
            "grant_types": grant_types,
            "scope": scope,
            "token_endpoint_auth_method": token_endpoint_auth_method,
            "client_id_issued_at": issued_at,
            "client_secret_expires_at": 0
        }
        self._save_client_store()

        response = {
            "client_id": client_id,
            "client_name": client_name,
            "redirect_uris": redirect_uris,
            "grant_types": grant_types,
            "scope": scope,
            "token_endpoint_auth_method": token_endpoint_auth_method,
            "client_id_issued_at": issued_at,
            "client_secret_expires_at": 0
        }

        if client_secret is not None:
            response["client_secret"] = client_secret

        return response

    def create_authorization_code(
        self,
        client_id: str,
        redirect_uri: str,
        scopes: list[str],
        code_challenge: Optional[str] = None,
        code_challenge_method: Optional[str] = None,
        subject: Optional[str] = None,
        expires_in: int = 600
    ) -> str:
        """Create a short-lived authorization code."""
        code = secrets.token_urlsafe(32)
        self._auth_codes[code] = {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "redirect_uri_normalized": self._normalize_redirect_uri(redirect_uri),
            "scopes": scopes,
            "code_challenge": code_challenge,
            "code_challenge_method": code_challenge_method,
            "subject": subject or client_id,
            "expires_at": time.time() + expires_in
        }
        return code

    def exchange_authorization_code(
        self,
        code: str,
        client_id: str,
        redirect_uri: str,
        code_verifier: Optional[str]
    ) -> Optional[TokenResponse]:
        """Exchange an authorization code for tokens."""
        record = self._auth_codes.get(code)
        if not record:
            return None

        if time.time() > record["expires_at"]:
            del self._auth_codes[code]
            return None

        redirect_uri_normalized = self._normalize_redirect_uri(redirect_uri)
        if record["client_id"] != client_id or record["redirect_uri_normalized"] != redirect_uri_normalized:
            return None

        client = self.get_client(client_id)
        require_pkce = bool(client and client.get("token_endpoint_auth_method") == "none")
        code_challenge = record.get("code_challenge")
        if require_pkce and not code_challenge:
            return None

        if code_challenge:
            if not code_verifier:
                return None
            method = record.get("code_challenge_method", "plain")
            if not self._validate_pkce(code_verifier, code_challenge, method):
                return None
        elif require_pkce and not code_verifier:
            return None

        scopes = record["scopes"]
        subject = record.get("subject")
        del self._auth_codes[code]

        access_token = self.generate_access_token(client_id, scopes, subject=subject)
        refresh_token = self.generate_refresh_token(client_id, scopes)
        return TokenResponse(
            access_token=access_token,
            expires_in=self.config.token_expiry_seconds,
            refresh_token=refresh_token,
            scope=" ".join(scopes)
        )

    def _validate_pkce(self, verifier: str, challenge: str, method: str) -> bool:
        if method == "S256":
            digest = hashlib.sha256(verifier.encode()).digest()
            encoded = base64.urlsafe_b64encode(digest).decode().rstrip("=")
            return encoded == challenge
        if method == "plain":
            return verifier == challenge
        return False
    
    def generate_access_token(
        self,
        client_id: str,
        scopes: list[str],
        subject: Optional[str] = None
    ) -> str:
        """Generate a JWT access token."""
        now = datetime.utcnow()
        payload = {
            "iss": self.config.issuer,
            "sub": subject or client_id,
            "aud": "nelson-court-cases-api",
            "client_id": client_id,
            "scope": " ".join(scopes),
            "iat": now,
            "exp": now + timedelta(seconds=self.config.token_expiry_seconds),
            "jti": secrets.token_hex(16)
        }
        return jwt.encode(payload, self.config.jwt_secret, algorithm=self.config.jwt_algorithm)
    
    def generate_refresh_token(self, client_id: str, scopes: list[str]) -> str:
        """Generate a refresh token."""
        token = secrets.token_urlsafe(64)
        expiry = datetime.utcnow() + timedelta(seconds=self.config.refresh_token_expiry_seconds)
        
        self._refresh_tokens[token] = {
            "client_id": client_id,
            "scopes": scopes,
            "expires_at": expiry.isoformat(),
            "created_at": datetime.utcnow().isoformat()
        }
        self._save_token_store()
        return token
    
    def validate_access_token(self, token: str) -> Optional[Dict[str, Any]]:
        """Validate and decode an access token."""
        try:
            # Check if token is revoked
            token_hash = hashlib.sha256(token.encode()).hexdigest()[:16]
            if token_hash in self._revoked_tokens:
                return None
            
            payload = jwt.decode(
                token,
                self.config.jwt_secret,
                algorithms=[self.config.jwt_algorithm],
                audience="nelson-court-cases-api"
            )
            return payload
        except jwt.ExpiredSignatureError:
            return None
        except jwt.InvalidTokenError:
            return None
    
    def validate_refresh_token(self, refresh_token: str) -> Optional[Dict[str, Any]]:
        """Validate a refresh token."""
        token_data = self._refresh_tokens.get(refresh_token)
        if not token_data:
            return None
        
        expiry = datetime.fromisoformat(token_data["expires_at"])
        if datetime.utcnow() > expiry:
            del self._refresh_tokens[refresh_token]
            self._save_token_store()
            return None
        
        return token_data
    
    def revoke_token(self, token: str):
        """Revoke an access token."""
        token_hash = hashlib.sha256(token.encode()).hexdigest()[:16]
        self._revoked_tokens.add(token_hash)
        self._save_token_store()
    
    def revoke_refresh_token(self, refresh_token: str):
        """Revoke a refresh token."""
        if refresh_token in self._refresh_tokens:
            del self._refresh_tokens[refresh_token]
            self._save_token_store()
    
    def client_credentials_grant(
        self,
        client_id: str,
        client_secret: str,
        scope: Optional[str] = None
    ) -> Optional[TokenResponse]:
        """Handle client credentials grant type."""
        if not self.validate_client(client_id, client_secret):
            return None
        
        # Parse requested scopes
        requested_scopes = scope.split() if scope else self.config.scopes
        granted_scopes = [s for s in requested_scopes if s in self.config.scopes]
        
        if not granted_scopes:
            granted_scopes = self.config.scopes
        
        access_token = self.generate_access_token(client_id, granted_scopes)
        refresh_token = self.generate_refresh_token(client_id, granted_scopes)
        
        return TokenResponse(
            access_token=access_token,
            expires_in=self.config.token_expiry_seconds,
            refresh_token=refresh_token,
            scope=" ".join(granted_scopes)
        )
    
    def refresh_token_grant(
        self,
        refresh_token: str,
        scope: Optional[str] = None
    ) -> Optional[TokenResponse]:
        """Handle refresh token grant type."""
        token_data = self.validate_refresh_token(refresh_token)
        if not token_data:
            return None
        
        client_id = token_data["client_id"]
        original_scopes = token_data["scopes"]
        
        # Requested scopes must be subset of original
        if scope:
            requested_scopes = scope.split()
            granted_scopes = [s for s in requested_scopes if s in original_scopes]
        else:
            granted_scopes = original_scopes
        
        # Revoke old refresh token and generate new ones
        self.revoke_refresh_token(refresh_token)
        
        access_token = self.generate_access_token(client_id, granted_scopes)
        new_refresh_token = self.generate_refresh_token(client_id, granted_scopes)
        
        return TokenResponse(
            access_token=access_token,
            expires_in=self.config.token_expiry_seconds,
            refresh_token=new_refresh_token,
            scope=" ".join(granted_scopes)
        )
    
    def has_scope(self, token_payload: Dict[str, Any], required_scope: str) -> bool:
        """Check if token has required scope."""
        token_scopes = token_payload.get("scope", "").split()
        return required_scope in token_scopes


def create_oauth_config_from_env() -> OAuthConfig:
    """Create OAuth config from environment variables."""
    client_id = os.getenv("MCP_OAUTH_CLIENT_ID", "nelson-court-cases-client")
    client_secret = os.getenv("MCP_OAUTH_CLIENT_SECRET", secrets.token_hex(32))
    jwt_secret = os.getenv("MCP_JWT_SECRET", secrets.token_hex(32))
    issuer = os.getenv("MCP_OAUTH_ISSUER") or os.getenv("MCP_PUBLIC_BASE_URL") or "nelson-court-cases-mcp"
    
    return OAuthConfig(
        client_id=client_id,
        client_secret=client_secret,
        jwt_secret=jwt_secret,
        issuer=issuer
    )


# Global OAuth provider instance
_oauth_provider: Optional[OAuthProvider] = None


def get_oauth_provider() -> OAuthProvider:
    """Get or create the OAuth provider."""
    global _oauth_provider
    if _oauth_provider is None:
        config = create_oauth_config_from_env()
        _oauth_provider = OAuthProvider(config)
    return _oauth_provider
