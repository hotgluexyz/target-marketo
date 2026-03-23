"""Marketo target client classes."""

from __future__ import annotations

import hashlib
import json

from hotglue_singer_sdk.target_sdk.client import HotglueBatchSink
from hotglue_singer_sdk.target_sdk.common import HGJSONEncoder

from target_marketo.auth import MarketoAuthenticator


class MarketoSink(HotglueBatchSink):
    """Marketo target sink class."""

    endpoint = ""
    name = ""
    MAX_SIZE_DEFAULT = 300

    @property
    def base_url(self) -> str:
        """Base URL for Marketo REST API."""
        return str(self.config["base_url"]).rstrip("/")

    def __init__(self, target, stream_name, schema, key_properties) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        self.authenticator = MarketoAuthenticator(target)

    def build_record_hash(self, record: dict) -> str:
        """Match HotglueSink.build_record_hash for bookmark parity with single-record sinks."""
        return hashlib.sha256(json.dumps(record, cls=HGJSONEncoder).encode()).hexdigest()

    def update_state(self, state: dict, is_duplicate: bool = False, record=None) -> None:
        dup = bool(is_duplicate or state.pop("is_duplicate", False))
        super().update_state(state, is_duplicate=dup, record=record)