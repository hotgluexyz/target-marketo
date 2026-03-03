"""Marketo target class."""

from __future__ import annotations

from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.target_sdk.target import TargetHotglue

from target_marketo.sinks import (
    EventsSink,
)


class TargetMarketo(TargetHotglue):
    """Sample target for Marketo."""

    name = "target-marketo"

    SINK_TYPES = [EventsSink]
    MAX_PARALLELISM = 1

    config_jsonschema = th.PropertiesList(
        th.Property("account_id", th.StringType, required=True),
        th.Property("event_key", th.StringType, required=True),
    ).to_dict()

if __name__ == "__main__":
    TargetMarketo.cli()
