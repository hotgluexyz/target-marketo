"""Marketo target client classes."""

from __future__ import annotations


from hotglue_singer_sdk.target_sdk.client import HotglueSink

from target_marketo.auth import MarketoAuthenticator


class MarketoSink(HotglueSink):
    """Marketo target sink class."""

    endpoint = ""
    name = ""

    @property
    def base_url(self) -> str:
        """Base URL for Marketo REST API."""
        return str(self.config["base_url"]).rstrip("/")

    def __init__(self, target, stream_name, schema, key_properties) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        self.authenticator = MarketoAuthenticator(target)
