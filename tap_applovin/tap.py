"""applovin tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_applovin import streams


class Tapapplovin(Tap):
    """applovin tap class."""

    name = "tap-applovin"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            title="API Key",
            description="The token to authenticate against the API service",
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
