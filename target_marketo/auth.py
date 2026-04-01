"""Marketo target authenticator."""

from __future__ import annotations

import time
from typing import Any
from singer import utils
import json
import re

import requests

from hotglue_singer_sdk.target_sdk.auth import OAuthAuthenticator

from hotglue_etl_exceptions import InvalidCredentialsError

class MarketoAuthenticator(OAuthAuthenticator):
    """OAuth authenticator for Marketo REST APIs."""

    def __init__(self, target, state: dict[str, Any] | None = None) -> None:
        super().__init__(target, state or {})
        self.auth_endpoint = f"{self._identity_url}/oauth/token"
        self.access_token = self._config.get("access_token")
        self.last_refreshed = None
        self.expires_in = None


    @property
    def _identity_url(self) -> str:
        identity_url = self._config.get("identity_url")
        if identity_url:
            return str(identity_url).rstrip("/")

        base_url = str(self._config["base_url"]).rstrip("/")
        return f"{base_url}/identity"
    
    @property
    def oauth_request_payload(self) -> dict:
        """Query params for token request."""
        return {
            "grant_type": "client_credentials",
            "client_id": self._config["client_id"],
            "client_secret": self._config["client_secret"],
        }
    
    def _update_access_token_locally(self) -> None:
        """Update `access_token` locally."""

        request_time = int(utils.now().timestamp())
        auth_request_payload = self.oauth_request_payload
        token_response = requests.post(self.auth_endpoint, data=auth_request_payload)
        try:
            token_response.raise_for_status()
            token_text = token_response.text or ""
            token_text = re.sub(r'("access_token"\s*:\s*")([^"]*)(".*?"token_type")',
                                lambda m: m.group(1) + (m.group(2)[:8] + ("*" * max(0, len(m.group(2)) - 8))) + m.group(3),
                                token_text, count=1) or token_text
            self.logger.info(f"OAuth authorization attempt was successful, response was '{token_text}'")
        except Exception as ex:
            raise InvalidCredentialsError(
                f"Failed OAuth login, response was '{token_response.text}'. {ex}"
            )
        token_json = token_response.json()
        self.access_token = token_json["access_token"]
        expires_in = token_json.get("expires_in", 0)
        if expires_in == 0:
            self.expires_in = 0
            self.logger.debug(
                "No expires_in received in OAuth response and no "
                "default_expiration set. Token will be treated as if it is "
                "expired."
            )
        else:
            self.expires_in = int(expires_in) + int(request_time)
        
        self.last_refreshed = request_time

        self._config["access_token"] = self.access_token
        self._config["expires_in"] = self.expires_in

        with open(self._config_file_path, "w") as outfile:
            json.dump(self._config, outfile, indent=4)


    def is_token_valid(self) -> bool:
        """Check if token is valid.

        Returns:
            True if the token is valid (fresh).
        """
        # if expires_in is not set, try to get it from the tap config
        if self.expires_in is None and self._config.get("expires_in"):
            self.expires_in = self._config.get("expires_in")

        if self.last_refreshed is None:
            return False
        if not self.expires_in:
            return False

        seconds_left = self.expires_in - int(utils.now().timestamp())
        if seconds_left <= 2:
            if seconds_left >= 0:
                self.logger.info(f"Access token expires in {seconds_left} seconds; waiting so it fully expires before refreshing.")
                time.sleep(seconds_left + 2)            
            return False

        return True
