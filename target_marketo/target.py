"""Marketo target class."""

from __future__ import annotations

from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.target_sdk.target import TargetHotglue

from target_marketo.sinks import (
    LeadsSink,
)


class TargetMarketo(TargetHotglue):
    """Sample target for Marketo."""

    name = "target-marketo"

    SINK_TYPES = [LeadsSink]
    MAX_PARALLELISM = 1

    config_jsonschema = th.PropertiesList(
        th.Property("base_url", th.StringType, required=True),
        th.Property("identity_url", th.StringType, required=False),
        th.Property("client_id", th.StringType, required=True),
        th.Property("client_secret", th.StringType, required=True),
        th.Property("access_token", th.StringType, required=False),
        th.Property("expires_in", th.IntegerType, required=False),
    ).to_dict()

if __name__ == "__main__":
    TargetMarketo.cli()
