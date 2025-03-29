"""Stream type classes for tap-applovin."""

from __future__ import annotations

import typing as t
from importlib import resources

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from singer_sdk import typing as th, metrics

if t.TYPE_CHECKING:
    import requests
    from singer_sdk.helpers.types import Context

from tap_applovin.client import ApplovinStream

SCHEMAS_DIR = resources.files(__package__) / "schemas"


class ReportsStream(ApplovinStream):
    """Uses the Reporting API to get aggregated ad & campaign data in JSON format."""

    name = "reports"
    path = "report"
    primary_keys: t.ClassVar[list[str]] = ["day", "campaign"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "reports.json"  # noqa: ERA001

    columns = [
        # Osnovne kolone
        "day",
        "campaign",
        "campaign_id_external",
        "cost",
        "country",
        "platform",
        "impressions",
        "clicks",
        "ctr",
        "conversions",
        "conversion_rate",
        "sales",
        
        # Dodatne kolone
        "ad",
        "ad_id",
        "ad_type",
        "creative_set",
        "creative_set_id",
        "campaign_type",
        "campaign_roas_goal",
        "device_type",
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
        context: Context | None,
        next_page_token: t.Any | None,
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
        
        # Koristi kolone iz konfiguracije ako postoje, inače koristi default
        configured_columns = self.config.get("columns")
        if configured_columns:
            if isinstance(configured_columns, list):
                params["columns"] = ",".join(configured_columns)
            else:
                # Pretpostavljamo da je string, već formatiran kao comma-separated
                params["columns"] = configured_columns
        else:
            # Koristi default kolone ako ništa nije konfigurisano
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
        next_page_token: t.Any | None,
        interval_start: str,
        interval_end: str,
    ) -> requests.PreparedRequest:
        """Prepare a request object for this stream."""
        http_method = self.http_method
        url: str = self.get_url(context)
        params: dict | str = self.get_url_params(context, next_page_token)
        request_data = self.prepare_request_payload(context, next_page_token)
        headers = self.http_headers

        params["start"] = interval_start
        
        # Ako je krajnji datum današnji, koristi "now" umesto datuma
        today = datetime.now().strftime("%Y-%m-%d")
        if interval_end == today or interval_end == "now":
            params["end"] = "now"
        else:
            params["end"] = interval_end

        self.logger.info(f"Sending request with params: {params}")

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

        prepared_request = self.build_prepared_request(**prepare_kwargs)
        
        # Dodajemo logiranje kompletnog URL-a
        self.logger.info(f"Full API request URL: {prepared_request.url}")
        
        return prepared_request
    
    def request_records(self, context: Context | None) -> t.Iterable[dict]:
        """Request records from REST endpoint(s), returning response records.

        If pagination is detected, pages will be recursed automatically.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            An item for every record in the response.
        """
        # Ako je definisan start_date, koristi njega, inače izračunaj na osnovu report_range_days
        start_date_str = self.config.get("start_date")
        if start_date_str:
            try:
                start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            except ValueError:
                self.logger.warning(f"Invalid start_date format: {start_date_str}. Using report_range_days instead.")
                start_date = datetime.now() - relativedelta(days=self.config.get("report_range_days", 30))
        else:
            start_date = datetime.now() - relativedelta(days=self.config.get("report_range_days", 30))
        
        # Postavi end_date na današnji datum
        end_date = datetime.now()

        # Umesto da vraćamo podatke za svaki pojedinačni dan, možemo direktno zatražiti ceo period
        interval_start = start_date.strftime("%Y-%m-%d")
        interval_end = "now"  # Uvek koristi "now" za kraj
        
        self.logger.info(f"Requesting records from {interval_start} to {interval_end}")

        paginator = self.get_new_paginator()
        decorated_request = self.request_decorator(self._request)
        
        pages = 0

        with metrics.http_request_counter(self.name, self.path) as request_counter:
            request_counter.context = context
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
            
            pages += 1
            
            try:
                first_record = next(records)
            except StopIteration:
                self.logger.info(
                    "Pagination stopped after %d pages because no records were "
                    "found in the last response",
                    pages,
                )
                return
            yield first_record
            yield from records

            paginator.advance(resp)
