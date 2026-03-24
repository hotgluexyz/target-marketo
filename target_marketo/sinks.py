"""Marketo target sink classes."""

from __future__ import annotations

from typing import Any, List

from target_marketo.client import MarketoSink

from hotglue_singer_sdk.exceptions import FatalAPIError
from hotglue_etl_exceptions import InvalidCredentialsError, InvalidPayloadError

class LeadsSink(MarketoSink):
    """Marketo leads sink class."""

    endpoint = "/rest/v1/leads.json"
    name = "leads"


    def process_batch_record(self, record: dict, index: int) -> dict:
        """Mirror HotglueSink.process_record: do not send externalId to Marketo; keep originals for state/hash."""
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
        self._last_batch_input = getattr(self, "_batch_originals", None) or records
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
            error_items = body.get("errors", body)
            for i, rec in enumerate(records):
                error_item = error_items[i] if i < len(error_items) else (error_items[0] if error_items else {})
                state_updates.append(
                    self._failed_state(rec, error_code=int(error_item.get("code")), error_message=error_item.get("message")),
                )
            return {"state_updates": state_updates}

        for i, rec in enumerate(records):
            row = results[i] if i < len(results) else None
            if row is None:
                state_updates.append(
                    self._failed_state(rec),
                )
                continue

            status = row.get("status")
            if status in ("updated", "created"):
                st: dict = {
                    "hash": self.build_record_hash(rec),
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
                st = self._failed_state(
                    rec, 
                    error_code=int(row.get("reasons", [])[0].get("code")), \
                    error_message=row.get("reasons", [])[0].get("message"),
                )
                if status == "skipped":
                    st["is_duplicate"] = True
                state_updates.append(st)

        return {"state_updates": state_updates}

    def _failed_state(
        self,
        record: dict,
        error_code: int | None = None,
        error_message: str = "",
        generic_error: str = "No result row from Marketo for this input",
    ) -> dict:
        if error_message:
            err_text = error_message if error_code is None else f"{error_message} (code {error_code})"
        else:
            err_text = generic_error

        st = {
            "hash": self.build_record_hash(record),
            "success": False,
            "error": err_text,
        }
        ext = record.get("externalId")
        if ext is not None:
            st["externalId"] = ext

        
        if error_code in (601, 602, 603):
            st["hg_error_class"] = InvalidCredentialsError.__name__
        elif error_code is not None and error_code >= 1000:
            st["hg_error_class"] = InvalidPayloadError.__name__
        else:
            st["hg_error_class"] = FatalAPIError.__name__
        
        return st
