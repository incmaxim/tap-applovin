"""Stream type classes for tap-applovin."""

from __future__ import annotations

import typing as t
from importlib import resources

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from singer_sdk import typing as th, metrics  # JSON Schema typing helpers

if t.TYPE_CHECKING:
    import requests
    from singer_sdk.helpers.types import Context

from tap_applovin.client import ApplovinStream

SCHEMAS_DIR = resources.files(__package__) / "schemas"


class ReportsStream(ApplovinStream):
    """Define custom stream."""

    name = "reports"
    path = "report"
    primary_keys: t.ClassVar[list[str]] = ["ad_id", "campaign_id_external", "day", "hour"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "reports.json"  # noqa: ERA001

    columns = [
        "ad",
        "ad_id",
        "campaign",
        "campaign_id_external",
        "cost",
        "country",
        "creative_set",
        "creative_set_id",
        "day",
        "hour",
        "clicks",
        "impressions",
        "platform",
        "sales",
        "chka_0d",
        "chka_1d",
        "chka_2d",
        "chka_3d",
        "chka_7d",
        "chka_usd_0d",
        "chka_usd_1d",
        "chka_usd_2d",
        "chka_usd_3d",
        "chka_usd_7d",
        "roas_0d",
        "roas_1d",
        "roas_2d",
        "roas_3d",
        "roas_7d"
    ]

    @staticmethod
    def date_range(start_date, end_date, interval_in_days=1):
        """
        Generator function that produces an iterable list of days between the two
        dates start_date and end_date as a tuple pair of datetimes.

        Args:
            start_date (datetime): start of period
            end_date (datetime): end of period
            interval_in_days (int): interval of days to iter over

        Yields:
            tuple: daily period
                * datetime: day within range - interval_in_days
                * datetime: day within range + interval_in_days

        """
        current_date = start_date
        while current_date < end_date:
            interval_start = current_date
            interval_end = current_date + timedelta(days=interval_in_days)

            if interval_end > end_date:
                interval_end = end_date

            yield interval_start, interval_end
            current_date = interval_end

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
        params["api_key"] = self.config.get("api_key")
        params["format"] = "json"
        params["report_type"] = "advertiser"
        params["columns"] = ",".join(self.columns)
        if next_page_token:
            params["page"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
        return params
    
    def prepare_request(
        self,
        context: Context | None,
        next_page_token: _TToken | None,
        interval_start: datetime,
        interval_end: datetime,
    ) -> requests.PreparedRequest:
        """Prepare a request object for this stream.

        If partitioning is supported, the `context` object will contain the partition
        definitions. Pagination information can be parsed from `next_page_token` if
        `next_page_token` is not None.

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token, page number or any request argument to request the
                next page of data.

        Returns:
            Build a request with the stream's URL, path, query parameters,
            HTTP headers and authenticator.
        """
        http_method = self.http_method
        url: str = self.get_url(context)
        params: dict | str = self.get_url_params(context, next_page_token)
        request_data = self.prepare_request_payload(context, next_page_token)
        headers = self.http_headers

        params["start"] = interval_start
        params["end"] = interval_end

        prepare_kwargs: dict[str, t.Any] = {
            "method": http_method,
            "url": url,
            "params": params,
            "headers": headers,
        }

        if self.payload_as_json:
            prepare_kwargs["json"] = request_data
        else:
            prepare_kwargs["data"] = request_data

        return self.build_prepared_request(**prepare_kwargs)
    
    def request_records(self, context: Context | None) -> t.Iterable[dict]:
        """Request records from REST endpoint(s), returning response records.

        If pagination is detected, pages will be recursed automatically.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            An item for every record in the response.
        """
        start_date = datetime.now() - relativedelta(days=15)
        end_date = datetime.now()

        for interval_start, interval_end in self.date_range(start_date, end_date):
            interval_start = interval_start.strftime("%Y-%m-%d")
            interval_end = interval_end.strftime("%Y-%m-%d")
            self.logger.info(f"Requesting records from {interval_start} to {interval_end}")

            paginator = self.get_new_paginator()
            decorated_request = self.request_decorator(self._request)
            pages = 0

            with metrics.http_request_counter(self.name, self.path) as request_counter:
                request_counter.context = context

                while not paginator.finished:
                    prepared_request = self.prepare_request(
                        context,
                        next_page_token=paginator.current_value,
                        interval_start=interval_start,
                        interval_end=interval_end
                    )
                    
                    resp = decorated_request(prepared_request, context)
                    request_counter.increment()
                    self.update_sync_costs(prepared_request, resp, context)
                    records = iter(self.parse_response(resp))
                    try:
                        first_record = next(records)
                    except StopIteration:
                        self.logger.info(
                            "Pagination stopped after %d pages because no records were "
                            "found in the last response",
                            pages,
                        )
                        break
                    yield first_record
                    yield from records
                    pages += 1

                paginator.advance(resp)

