"""Applovin tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_applovin import streams


class Tapapplovin(Tap):
    """Applovin tap class."""

    name = "tap-applovin"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            secret=True,
            title="API Key",
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "report_range_days",
            th.IntegerType,
            required=False,
            title="Report Range Days",
            description="Defines the number of days in the past to include when generating reports. This value determines the starting point for data retrieval, relative to the current date.",
        ),
        th.Property(
            "start_date",
            th.StringType,
            required=False,
            title="Start Date",
            description="The start date for data extraction in YYYY-MM-DD format. If specified, this overrides the report_range_days parameter.",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.applovinStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.ReportsStream(self),
        ]


if __name__ == "__main__":
    Tapapplovin.cli()
