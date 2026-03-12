from __future__ import annotations

from collections.abc import AsyncGenerator
from json import JSONDecodeError
import logging
from typing import Any
from urllib.parse import parse_qs, urljoin, urlparse

import httpx
from pydantic import BaseModel, ConfigDict, Field, model_validator
from tenacity import AsyncRetrying, RetryError, retry_if_exception_type, stop_after_attempt, wait_exponential
from tenacity.wait import wait_base

OLX_SITE_URL = "https://www.olx.uz"
OLX_API_PATH = "/api/v1/offers/"
DEFAULT_PAGE_SIZE = 40
RETRYABLE_STATUS_CODES = frozenset({403, 429})
LOGGER = logging.getLogger(__name__)

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/134.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9,uz;q=0.8,ru;q=0.7",
    "Sec-Fetch-Dest": "empty",
}


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        digits = value.replace(" ", "").replace(",", ".")
        try:
            return float(digits)
        except ValueError:
            return None
    return None


def _stringify_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, dict):
        if "label" in value:
            return _stringify_value(value["label"])
        if "value" in value:
            return _stringify_value(value["value"])
    if isinstance(value, list):
        parts = [_stringify_value(item) for item in value]
        return ", ".join(part for part in parts if part)
    return str(value)


class OlxPrice(BaseModel):
    model_config = ConfigDict(extra="ignore")

    value: float | None = None
    currency: str | None = None

    @model_validator(mode="before")
    @classmethod
    def normalize_price(cls, data: Any) -> Any:
        if data is None:
            return {}
        if isinstance(data, (int, float, str)):
            return {"value": _coerce_float(data)}
        if not isinstance(data, dict):
            return data

        if data.get("arrange") is True:
            return {"value": None, "currency": data.get("currency")}

        raw_value = data.get("value", data.get("amount", data.get("price")))
        currency = data.get("currency")

        if isinstance(raw_value, dict):
            currency = raw_value.get("currency", currency)
            raw_value = raw_value.get("value", raw_value.get("label"))

        return {"value": _coerce_float(raw_value), "currency": currency}


class OlxParam(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: str
    value: str | None = None

    @model_validator(mode="before")
    @classmethod
    def normalize_param(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        return {
            "name": data.get("name", data.get("key")),
            "value": _stringify_value(data.get("value")),
        }


def _extract_price_from_params(params: Any) -> dict[str, Any] | None:
    if not isinstance(params, list):
        return None

    for param in params:
        if not isinstance(param, dict) or param.get("name") != "Цена":
            continue

        raw_value = param.get("value")
        if not isinstance(raw_value, dict):
            return {"value": _coerce_float(raw_value), "currency": None}

        price_value = raw_value.get("value", raw_value.get("label"))
        return {
            "value": _coerce_float(price_value),
            "currency": raw_value.get("currency"),
        }

    return None


class OlxListing(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: int
    url: str
    title: str
    price: OlxPrice | None = None
    params: list[OlxParam] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def normalize_listing(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        normalized = dict(data)
        normalized["id"] = data.get("id", data.get("sku"))

        listing_url = data.get("url") or data.get("full_url")
        if listing_url is None and isinstance(data.get("relative_url"), str):
            listing_url = urljoin(OLX_SITE_URL, data["relative_url"])
        normalized["url"] = listing_url

        if data.get("price") is None:
            normalized["price"] = _extract_price_from_params(data.get("params"))

        return normalized


class OlxListingsResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    data: list[OlxListing] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class RetryableStatusError(Exception):
    def __init__(self, status_code: int) -> None:
        super().__init__(f"Retryable OLX status: {status_code}")
        self.status_code = status_code


def _is_retryable_status(status_code: int) -> bool:
    return status_code in RETRYABLE_STATUS_CODES or 500 <= status_code < 600


def _normalize_limit(value: object, default: int) -> int:
    if isinstance(value, int):
        return value
    if not isinstance(value, str):
        return default

    try:
        return int(value)
    except ValueError:
        return default


class AsyncOLXClient:
    def __init__(
        self,
        *,
        client: httpx.AsyncClient | None = None,
        timeout: float = 20.0,
        page_size: int = DEFAULT_PAGE_SIZE,
        headers: dict[str, str] | None = None,
        retry_wait: wait_base = wait_exponential(multiplier=2, min=2, max=8),
    ) -> None:
        merged_headers = {**DEFAULT_HEADERS, **(headers or {})}
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(timeout=timeout, headers=merged_headers)
        self._headers = merged_headers
        self._page_size = page_size
        self._retry_wait = retry_wait

    async def __aenter__(self) -> "AsyncOLXClient":
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        if self._owns_client:
            await self._client.aclose()

    def _build_request(
        self,
        category_url: str | None,
        page: int,
        *,
        category_id: int | None = None,
    ) -> tuple[str, dict[str, str | int]]:
        parsed = urlparse(category_url or "")
        base_url = f"{parsed.scheme or 'https'}://{parsed.netloc or 'www.olx.uz'}{OLX_API_PATH}"

        params: dict[str, str | int] = {
            key: values[-1]
            for key, values in parse_qs(parsed.query, keep_blank_values=True).items()
            if values
        }

        params["page"] = page
        limit_value = params.get("limit", self._page_size)
        limit = _normalize_limit(limit_value, self._page_size)
        params.setdefault("offset", (page - 1) * limit)
        params.setdefault("limit", limit)

        if category_id is not None:
            params["category_id"] = category_id
        elif not parsed.path.startswith("/api/"):
            raise ValueError("category_id is required for non-API OLX category URLs")

        return base_url, params

    async def _get_page(
        self,
        category_url: str | None,
        page: int,
        *,
        category_id: int | None = None,
    ) -> OlxListingsResponse:
        request_url, params = self._build_request(category_url, page, category_id=category_id)

        async for attempt in AsyncRetrying(
            retry=retry_if_exception_type((RetryableStatusError, httpx.TransportError)),
            stop=stop_after_attempt(3),
            wait=self._retry_wait,
            reraise=True,
        ):
            with attempt:
                response = await self._client.get(request_url, params=params, headers=self._headers)
                if _is_retryable_status(response.status_code):
                    raise RetryableStatusError(response.status_code)
                response.raise_for_status()
                return OlxListingsResponse.model_validate(response.json())

    async def fetch_category_listings(
        self,
        category_url: str | None,
        max_pages: int,
        *,
        category_id: int | None = None,
    ) -> AsyncGenerator[OlxListing, None]:
        seen_listing_ids: set[int] = set()

        for page in range(1, max_pages + 1):
            try:
                payload = await self._get_page(category_url, page, category_id=category_id)
            except (RetryError, RetryableStatusError, httpx.HTTPError, JSONDecodeError, ValueError) as error:
                LOGGER.warning("Stopping OLX fetch for %s on page %s: %s", category_url, page, error)
                return

            if not payload.data:
                return

            page_has_new_ids = False
            for listing in payload.data:
                if listing.id in seen_listing_ids:
                    continue

                page_has_new_ids = True
                seen_listing_ids.add(listing.id)
                yield listing

            if not page_has_new_ids:
                LOGGER.info("Stopping OLX fetch for %s on page %s due to repeated listing IDs", category_url, page)
                return


async def fetch_category_listings(
    category_url: str | None,
    max_pages: int,
    *,
    category_id: int | None = None,
) -> AsyncGenerator[OlxListing, None]:
    """Yield validated OLX listings for the provided category page or category ID."""
    async with AsyncOLXClient() as client:
        async for listing in client.fetch_category_listings(
            category_url,
            max_pages,
            category_id=category_id,
        ):
            yield listing
