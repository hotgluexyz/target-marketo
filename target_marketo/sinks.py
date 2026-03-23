"""Marketo target sink classes."""

from __future__ import annotations

from typing import Any, List

from target_marketo.client import MarketoSink

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

        if body.get("success") is False and not results:
            return {"state_updates": self._error_state_updates(records, body)}

        state_updates: List[dict] = []
        for i, rec in enumerate(records):
            row = results[i] if i < len(results) else None
            state_updates.append(self._state_from_result_row(rec, row))

        return {"state_updates": state_updates}

    def _error_state_updates(self, records: List[dict], body: dict) -> List[dict]:
        error_items: Any = body.get("errors") or [body]
        if not isinstance(error_items, list):
            error_items = [error_items]

        state_updates: List[dict] = []
        for i, rec in enumerate(records):
            error_item = error_items[i] if i < len(error_items) else (error_items[0] if error_items else {})
            state_updates.append(self._failed_state_from_item(rec, error_item, generic_error="Marketo request failed"))
        return state_updates

    def _state_from_result_row(self, record: dict, row: dict | None) -> dict:
        if row is None:
            return self._failed_state(record)

        status = row.get("status")
        if status in ("updated", "created"):
            return self._success_state(record, row, status)
        if status == "skipped":
            return self._skipped_state(record, row)

        reasons = row.get("reasons") or []
        reason = reasons[0] if reasons else {}
        return self._failed_state_from_item(
            record,
            reason,
            generic_error="Marketo returned an unknown status without reason details",
        )

    def _success_state(self, record: dict, row: dict, status: str) -> dict:
        state: dict = {
            "hash": self.build_record_hash(record),
            "success": True,
            "id": row.get("id"),
        }
        if status == "updated":
            state["is_updated"] = True
        ext = record.get("externalId")
        if ext is not None:
            state["externalId"] = ext
        return state

    def _skipped_state(self, record: dict, row: dict) -> dict:
        state: dict = {
            "hash": self.build_record_hash(record),
            "success": False,
            "id": row.get("id"),
            "is_duplicate": True,
        }
        ext = record.get("externalId")
        if ext is not None:
            state["externalId"] = ext
        return state

    def _failed_state_from_item(self, record: dict, error_item: Any, generic_error: str) -> dict:
        item = error_item if isinstance(error_item, dict) else {}
        code = item.get("code")
        try:
            error_code = int(code) if code is not None else None
        except (TypeError, ValueError):
            error_code = None
        return self._failed_state(
            record,
            error_code=error_code,
            error_message=item.get("message") or "",
            generic_error=generic_error,
        )

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
        return st
