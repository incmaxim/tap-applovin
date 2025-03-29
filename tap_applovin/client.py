"""REST client handling, including applovinStream base class."""

from __future__ import annotations

import decimal
import typing as t
from importlib import resources

from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TC002
from singer_sdk.streams import RESTStream
from singer_sdk.exceptions import FatalAPIError

if t.TYPE_CHECKING:
    import requests
    from singer_sdk.helpers.types import Context


# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = resources.files(__package__) / "schemas"


class ApplovinStream(RESTStream):
    """applovin stream class."""

    records_jsonpath = "$.results[*]"

    next_page_token_jsonpath = "$.next_page"  # noqa: S105

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return "https://r.applovin.com/"

    def get_url_params(
        self,
        context: Context | None,  # noqa: ARG002
        next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        if next_page_token:
            params["page"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
        return params

    def validate_response(self, response):
        """Validate HTTP response.

        Args:
            response: The HTTP response object.

        Raises:
            FatalAPIError: If the response contains a fatal error.
            RetriableAPIError: If the response contains a retriable error.
        """
        if 400 <= response.status_code < 500:
            # Print response body to see what's going wrong
            error_msg = f"HTTP Error {response.status_code}: {response.text}"
            self.logger.error(error_msg)
            
            # pokušaj da izvučeš JSON ako postoji
            try:
                error_data = response.json()
                self.logger.error(f"API error details: {error_data}")
            except:
                pass
            
            # Podižemo grešku za sve 4xx odgovore
            msg = (
                f"{response.status_code} Client Error: "
                f"{response.reason} for path: {response.url}"
            )
            raise FatalAPIError(msg)
