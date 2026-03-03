"""Marketo target authenticator."""

from __future__ import annotations

import time
from typing import Any

import requests

from hotglue_singer_sdk.target_sdk.auth import Authenticator

from hotglue_etl_exceptions import InvalidCredentialsError

class MarketoAuthenticator(Authenticator):
    """OAuth authenticator for Marketo REST APIs."""

    _refresh_buffer_seconds = 60

    def __init__(self, target, state: dict[str, Any] | None = None) -> None:
        super().__init__(target, state or {})
        self._access_token = self._config.get("access_token")
        self._expires_at = self._resolve_initial_expiry()

    def _resolve_initial_expiry(self) -> float:
        expires_in = self._config.get("expires_in")
        if not expires_in:
            return 0.0

        # Support either epoch seconds or duration seconds.
        expiry = float(expires_in)
        if expiry > 10_000_000_000:
            return 0.0
        if expiry > time.time():
            return expiry
        return time.time() + expiry

    @property
    def _identity_url(self) -> str:
        identity_url = self._config.get("identity_url")
        if identity_url:
            return str(identity_url).rstrip("/")

        base_url = str(self._config["base_url"]).rstrip("/")
        return f"{base_url}/identity"

    def _is_expired(self) -> bool:
        if not self._access_token:
            return True
        return time.time() >= (self._expires_at - self._refresh_buffer_seconds)

    def refresh_access_token(self, force: bool = False) -> None:
        """Refresh access token if needed."""
        if not force and not self._is_expired():
            return

        response = requests.get(
            f"{self._identity_url}/oauth/token",
            params={
                "grant_type": "client_credentials",
                "client_id": self._config["client_id"],
                "client_secret": self._config["client_secret"],
            },
            timeout=30,
        )
        if response.status_code >= 400:
            raise InvalidCredentialsError(response.text)

        payload = response.json()
        self._access_token = payload["access_token"]
        expires_in = int(payload.get("expires_in", 3600))
        self._expires_at = time.time() + expires_in
        self.state.update(
            {
                "access_token": self._access_token,
                "expires_at": int(self._expires_at),
            }
        )

    @property
    def auth_headers(self) -> dict[str, str]:
        """Get auth headers."""
        self.refresh_access_token()
        return {"Authorization": f"Bearer {self._access_token}"}
