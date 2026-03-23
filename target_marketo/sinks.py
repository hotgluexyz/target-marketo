"""Marketo target sink classes."""

from __future__ import annotations

from typing import Any, List

from hotglue_etl_exceptions import InvalidPayloadError

from target_marketo.client import MarketoSink


class LeadsSink(MarketoSink):
    """Marketo leads sink class."""

    endpoint = "/rest/v1/leads.json"
    name = "leads"


    def process_batch_record(self, record: dict, index: int) -> dict:
        """Mirror HotglueSink.process_record: do not send externalId to Marketo; keep originals for state."""
        if index == 0:
            self._batch_originals = []
        self._batch_originals.append(dict(record))
        if self.name in self.allows_externalid:
            return record
        key = self._target.EXTERNAL_ID_KEY
        if key not in record:
            return record
        out = dict(record)
        out.pop(key, None)
        return out
        
    def make_batch_request(self, records: List[dict]) -> Any:
        """POST up to MAX_SIZE_DEFAULT leads per request."""
        self._last_batch_input = records
        return self.request_api(
            "POST",
            endpoint=self.endpoint,
            request_data={
                "action": "createOrUpdate",
                "lookupField": "email",
                "input": records,
            },
        )

    def handle_batch_response(self, response: Any) -> dict:
        """Map Marketo `result[]` (same order as `input`) to target state rows."""
        body = response.json()
        records = getattr(self, "_last_batch_input", None) or []
        results = body.get("result") or []

        state_updates: List[dict] = []

        if body.get("success") is False and not results:
            err = body.get("errors", body)
            for rec in records:
                state_updates.append(
                    self._failed_state(rec, str(err)),
                )
            return {"state_updates": state_updates}

        for i, rec in enumerate(records):
            row = results[i] if i < len(results) else None
            if row is None:
                state_updates.append(
                    self._failed_state(rec, "No result row from Marketo for this input"),
                )
                continue

            status = row.get("status")
            if status in ("created", "success", "updated"):
                st: dict = {
                    "success": True,
                    "id": row.get("id"),
                }
                if status == "updated":
                    st["is_updated"] = True
                ext = rec.get("externalId")
                if ext is not None:
                    st["externalId"] = ext
                state_updates.append(st)
            else:
                state_updates.append(
                    self._failed_state(rec, str(row.get("reasons", []))),
                )

        return {"state_updates": state_updates}

    def _failed_state(self, record: dict, error: str) -> dict:
        st = {
            "success": False,
            "error": error,
            "hg_error_class": InvalidPayloadError.__name__,
        }
        ext = record.get("externalId")
        if ext is not None:
            st["externalId"] = ext
        return st
