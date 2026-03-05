"""Marketo target sink classes."""

from __future__ import annotations

from target_marketo.client import MarketoSink

from hotglue_etl_exceptions import InvalidPayloadError
class LeadsSink(MarketoSink):
    """Marketo leads sink class."""

    endpoint = "/rest/v1/leads.json"
    name = "leads"

    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Pass through lead record unchanged."""
        return record

    def upsert_record(self, record: dict, context: dict):
        """Create or update one lead record at a time."""
        payload = {
            "action": "createOrUpdate",
            "lookupField": "email",
            "input": [record],
        }
        response = self.request_api(
            "POST",
            endpoint=self.endpoint,
            request_data=payload,
        )
        body = response.json()
        result = next(iter(body.get("result", [])), None)
        if not result:
            raise Exception("No result record found on Marketo API response")
        state_updates = {}

        if result.get("status") not in ["success", "updated"]:
            raise InvalidPayloadError(str(result.get("reasons", [])))

        if result.get("status") == "updated":
            state_updates["is_updated"] = True

        return result.get("id"), True, state_updates
