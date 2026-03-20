"""REST client handling, including BigMarkerStream base class."""

import hashlib
import json
import logging
from pathlib import Path
from pickle import NONE
from typing import Any, Callable, Dict, Generator, Iterable, Optional

import backoff
import requests
import requests_random_user_agent
from decimal import Decimal

from memoization import cached
from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from urllib.parse import parse_qs, parse_qsl, urlparse


SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
from singer_sdk.exceptions import RetriableAPIError

class BigMarkerStream(RESTStream):
    """BigMarker stream class."""

    # When enabled, wrap each extracted API record into a generic structure:
    # {id: <stable sha256>, <context keys...>, raw: <full original payload>}
    # This allows “full coverage” without per-endpoint schemas.
    wrap_raw: bool = False

    # curr_page_token_jsonpath = "$.page"
    # totl_page_token_jsonpath = "$.total_pages"
    per_page = 10
    page_key = "page"
    has_pagination = True
    backoff_max_tries = 9999
    _LOG_REQUEST_METRIC_URLS = True
    tolerated_http_errors = [401]

    # Known BigMarker 404 payloads that indicate optional/missing webinar resources
    # rather than misconfiguration. These should not abort the whole ELT run.
    _KNOWN_OPTIONAL_404_SNIPPETS = (
        "conference_not_found",
        "conference you are requesting is not found",
        "this is not a recurring conference",
        "does not have any recordings",
    )

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        api_url = self.config["api_url"]
        if isinstance(api_url, str):
            api_url = api_url.strip()
            if (
                (api_url.startswith('"') and api_url.endswith('"'))
                or (api_url.startswith("'") and api_url.endswith("'"))
            ):
                api_url = api_url[1:-1]
        # Helpful for debugging which BigMarker base URL is actually in use.
        # (Do not log the API key itself.)
        self.logger.debug("BigMarker api_url=%r", api_url)
        return api_url

    @property
    def authenticator(self) -> APIKeyAuthenticator:
        """Return a new authenticator object."""
        api_key = self.config.get("api_key")
        if isinstance(api_key, str):
            api_key = api_key.strip()
            if (
                (api_key.startswith('"') and api_key.endswith('"'))
                or (api_key.startswith("'") and api_key.endswith("'"))
            ):
                api_key = api_key[1:-1]
        self.logger.debug("BigMarker api_key present=%s", bool(api_key))
        return APIKeyAuthenticator.create_for_stream(
            self,
            key="API-KEY",
            value=api_key,
            location="header"
        )

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        if not self.has_pagination:
            return None

        payload = response.json()
        if isinstance(payload, dict):
            # Prefer explicit pagination metadata when present. This prevents
            # endless paging if the API keeps returning non-empty repeated pages.
            current_page = payload.get("current_page") or payload.get("page")
            total_pages = payload.get("total_pages")
            if current_page is not None and total_pages is not None:
                try:
                    current_page_i = int(current_page)
                    total_pages_i = int(total_pages)
                    if current_page_i >= total_pages_i:
                        return None
                    return current_page_i + 1
                except (TypeError, ValueError):
                    pass

            # Secondary metadata path: derive total pages from total_entries.
            total_entries = payload.get("total_entries") or payload.get("total_count")
            per_page_meta = payload.get("per_page")
            if current_page is not None and total_entries is not None:
                try:
                    current_page_i = int(current_page)
                    per_page_i = int(per_page_meta or self.per_page or 0)
                    total_entries_i = int(total_entries)
                    if per_page_i > 0:
                        total_pages_i = (total_entries_i + per_page_i - 1) // per_page_i
                        if current_page_i >= total_pages_i:
                            return None
                        return current_page_i + 1
                except (TypeError, ValueError):
                    pass

        len_path = self.records_jsonpath.replace("[*]", "") + ".`len`"

        all_matches = extract_jsonpath(len_path, payload)
        match_len = next(iter(all_matches), 0)

        # Last-line safety: if the returned record count is smaller than the
        # requested page size, we've reached the last page.
        try:
            if self.per_page and int(match_len) < int(self.per_page):
                return None
        except (TypeError, ValueError):
            pass

        if match_len > 0:
            return int(previous_token or "1") + 1

        return None

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        # Ensure any state Singer might have stored from previous runs is JSON-safe.
        # This avoids loader failures when it attempts to json.dumps the final state.
        try:
            state = self.get_context_state(context)
            has_decimal = any(isinstance(v, Decimal) for v in state.values())
            if has_decimal:
                self.logger.debug(
                    "BigMarker state contains Decimal before normalization: %s",
                    {k: type(v).__name__ for k, v in state.items() if isinstance(v, Decimal)},
                )
            for k, v in list(state.items()):
                if isinstance(v, Decimal):
                    # Use plain Python ints when possible to avoid downstream
                    # loader issues (some JSON parsers deserialize floats as
                    # `Decimal`).
                    state[k] = int(v)
            state.pop("starting_replication_value", None)
        except Exception:
            # If state isn't available for some reason, ignore and proceed.
            pass

        # Singer SDK 0.48+ uses `_http_method` instead of `rest_method`.
        http_method = (
            getattr(self, "rest_method", None)
            or getattr(self, "http_method", None)
            or getattr(self, "_http_method", None)
        )
        if str(http_method or "").upper() == "GET":
            if next_page_token:
                params[self.page_key] = next_page_token
            if self.per_page:
                params["per_page"] = self.per_page

        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        if response.status_code in self.tolerated_http_errors:
            return []
        try:
            payload = response.json()
        except ValueError:
            # BigMarker sometimes returns non-JSON (HTML/empty body) when the API URL is wrong.
            # Surface a helpful error instead of a cryptic JSONDecodeError.
            body_snippet = (response.text or "")[:500]
            raise FatalAPIError(
                "Expected JSON response but got non-JSON body. "
                f"url={response.url!r} status={response.status_code} "
                f"body_snippet={body_snippet!r}"
            )

        yield from extract_jsonpath(self.records_jsonpath, input=payload)

    def _normalize_decimals(self, value: Any) -> Any:
        """Convert any Decimal values into JSON-safe primitives."""
        if isinstance(value, Decimal):
            # BigMarker values that come through as Decimal should still be whole
            # numbers for our use-cases (timestamps/integers).
            return int(value)
        if isinstance(value, dict):
            return {k: self._normalize_decimals(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self._normalize_decimals(v) for v in value]
        return value

    def _stable_id_for_record(self, record: Any) -> str:
        """Compute a deterministic id for any JSON-serializable record."""
        normalized = self._normalize_decimals(record)
        blob = json.dumps(
            normalized,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            default=str,
        )
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()

    def post_process(self, record: Any, context: Optional[dict] = None) -> Any:
        """
        Optionally wrap each record with a generic `raw` envelope.

        Singer SDK calls this once per extracted record.
        """
        if not self.wrap_raw:
            return super().post_process(record, context)

        wrapped: Dict[str, Any] = {
            "id": self._stable_id_for_record(record),
            "raw": self._normalize_decimals(record),
        }

        # Backward-compatible identity fields for existing warehouse tables
        # that were created before the raw-wrapper migration.
        if isinstance(record, dict):
            for legacy_key in ("bmid", "admin_id", "presenter_id", "member_id"):
                legacy_val = record.get(legacy_key)
                if legacy_val is not None:
                    wrapped[legacy_key] = legacy_val

        # Copy a few commonly-used context keys for debugging/joins.
        context = context or {}
        for k in (
            "conference_id",
            "channel_id",
            "custom_email_id",
            "meeting_space_id",
            "download_bmid",
        ):
            if context.get(k) is not None:
                wrapped[k] = context[k]
                continue
            # For top-level streams (no parent context), fall back to config.
            cfg_val = self.config.get(k)
            if cfg_val is not None:
                wrapped[k] = cfg_val

        return wrapped

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response.

        Checks for error status codes and wether they are fatal or retriable.

        In case an error is deemed transient and can be safely retried, then this
        method should raise an :class:`singer_sdk.exceptions.RetriableAPIError`.
        By default this applies to 5xx error codes, along with values set in:
        :attr:`~singer_sdk.RESTStream.extra_retry_statuses`

        In case an error is unrecoverable raises a
        :class:`singer_sdk.exceptions.FatalAPIError`. By default, this applies to
        4xx errors, excluding values found in:
        :attr:`~singer_sdk.RESTStream.extra_retry_statuses`

        Tap developers are encouraged to override this method if their APIs use HTTP
        status codes in non-conventional ways, or if they communicate errors
        differently (e.g. in the response body).

        .. image:: ../images/200.png

        Args:
            response: A `requests.Response`_ object.

        Raises:
            FatalAPIError: If the request is not retriable.
            RetriableAPIError: If the request is retriable.

        .. _requests.Response:
            https://docs.python-requests.org/en/latest/api/#requests.Response
        """
        if response.status_code in self.tolerated_http_errors:
            full_path = urlparse(response.url).path
            query = urlparse(response.url).query
            body_snippet = (response.text or "")[:250]
            www_auth = response.headers.get("www-authenticate")
            msg = (
                f"{response.status_code} Tolerated Status Code "
                f"(Reason: {response.reason}) for path: {full_path}"
                f"Query: {query} "
                f"www-authenticate: {www_auth!r} "
                f"body_snippet={body_snippet!r}"
            )
            self.logger.warn(msg)
            return
        if response.status_code == 404:
            body_lc = (response.text or "").lower()
            if any(snippet in body_lc for snippet in self._KNOWN_OPTIONAL_404_SNIPPETS):
                full_path = urlparse(response.url).path
                query = urlparse(response.url).query
                msg = (
                    "404 optional resource missing; continuing sync. "
                    f"path={full_path} query={query} body_snippet={body_lc[:250]!r}"
                )
                self.logger.warning(msg)
                return
        if (
            response.status_code in self.extra_retry_statuses
            or 500 <= response.status_code < 600
        ):
            msg = self.response_error_message(response)
            raise RetriableAPIError(msg, response)
        elif 400 <= response.status_code < 500:
            msg = self.response_error_message(response)
            raise FatalAPIError(msg)

    def backoff_wait_generator(self) -> Callable[..., Generator[int, Any, None]]:
        return backoff.constant(interval=10)  # type: ignore # ignore 'Returning Any'

    def request_decorator(self, func: Callable) -> Callable:
        decorator: Callable = backoff.on_exception(
            self.backoff_wait_generator,
            (
                RetriableAPIError,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError,
            ),
            max_tries=self.backoff_max_tries,
            on_backoff=self.backoff_handler,
        )(func)
        return decorator

    def backoff_handler(self, details: dict) -> None:
        if details["tries"] > 5:
            logging.info("resetting session")
            self._requests_session.close()
            self._requests_session = None
        return super().backoff_handler(details)