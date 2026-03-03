"""Marketo target sink class, which handles writing streams."""

from __future__ import annotations
import json
from target_marketo.client import MarketoSink


class EventsSink(MarketoSink):
    """Marketo target sink class."""
    endpoint = "/event"
    name = "events"

    def preprocess_record(self, record: dict, context: dict):
        record["key"] = self.config.get("event_key")
        record["actid"] = self.config.get("account_id")
        record["visit"] = json.dumps(record.get("visit")) if type(record.get("visit")) == dict else record.get("visit")

        return record

    def upsert_record(self, record: dict, context: dict):
        response = self.request_api(
            "POST", endpoint=self.endpoint, request_data=record
        )

        # NOTE: hardcoded to return id as 1 because the API does not return an id
        return 1, response.ok, dict()
