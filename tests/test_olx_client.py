from __future__ import annotations

import asyncio

import httpx
from tenacity import wait_none

from olx_client import AsyncOLXClient, OlxListing


def test_listing_model_ignores_extra_fields() -> None:
    listing = OlxListing.model_validate(
        {
            "id": 101,
            "relative_url": "/d/obyavlenie/test-ID101.html",
            "title": "Yangi квартира",
            "params": [
                {
                    "name": "Цена",
                    "value": {"value": "125000", "currency": "USD", "label": "125 000"},
                    "extra_field": "ignored",
                },
                {"name": "Площадь", "value": {"label": "78 м²"}},
            ],
            "seller": {"name": "ignored"},
        }
    )

    assert listing.id == 101
    assert listing.url == "https://www.olx.uz/d/obyavlenie/test-ID101.html"
    assert listing.title == "Yangi квартира"
    assert [(param.name, param.value) for param in listing.params] == [
        ("Цена", "125 000"),
        ("Площадь", "78 м²"),
    ]


def test_listing_model_extracts_price_from_params() -> None:
    listing = OlxListing.model_validate(
        {
            "id": 101,
            "relative_url": "/d/obyavlenie/test-ID101.html",
            "title": "Yangi квартира",
            "params": [
                {
                    "name": "Цена",
                    "value": {"value": "125000", "currency": "USD", "label": "125 000"},
                }
            ],
        }
    )

    assert listing.price is not None
    assert listing.price.value == 125000.0
    assert listing.price.currency == "USD"


def test_listing_model_accepts_arranged_and_zero_prices() -> None:
    arranged = OlxListing.model_validate(
        {
            "id": 202,
            "relative_url": "/d/obyavlenie/arranged-ID202.html",
            "title": "Negotiable",
            "price": {"arrange": True},
            "params": [],
        }
    )
    zero_price = OlxListing.model_validate(
        {
            "id": 203,
            "relative_url": "/d/obyavlenie/zero-ID203.html",
            "title": "Nol narx",
            "price": {"value": 0, "currency": "UZS"},
            "params": [],
        }
    )

    assert arranged.price is not None
    assert arranged.price.value is None
    assert arranged.price.currency is None
    assert zero_price.price is not None
    assert zero_price.price.value == 0.0
    assert zero_price.price.currency == "UZS"


def test_build_request_uses_category_id_parameter() -> None:
    client = AsyncOLXClient()

    try:
        request_url, params = client._build_request(
            "https://www.olx.uz/nedvizhimost/kvartiry/arenda-kvartir/?currency=USD",
            2,
            category_id=123,
        )
    finally:
        asyncio.run(client.__aexit__(None, None, None))

    assert request_url == "https://www.olx.uz/api/v1/offers/"
    assert params["category_id"] == 123
    assert params["currency"] == "USD"
    assert params["page"] == 2
    assert params["offset"] == 40
    assert "category_slug" not in params
    assert "category_path" not in params


def test_async_client_retry_and_headers() -> None:
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(1)
        assert request.headers["User-Agent"].startswith("Mozilla/5.0")
        assert request.headers["Accept"] == "application/json, text/plain, */*"
        assert request.headers["Accept-Language"].startswith("en-US")
        assert request.headers["Sec-Fetch-Dest"] == "empty"

        if len(attempts) < 2:
            return httpx.Response(429, json={"error": "slow down"})

        return httpx.Response(
            200,
            json={
                "data": [
                    {
                        "id": 7,
                        "relative_url": "/d/obyavlenie/item-7",
                        "title": "Retry worked",
                        "price": {"value": "70000", "currency": "UZS"},
                        "params": [],
                    }
                ]
            },
        )

    async def collect() -> list[OlxListing]:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as raw_client:
            client = AsyncOLXClient(client=raw_client, retry_wait=wait_none())
            return [
                listing
                async for listing in client.fetch_category_listings(
                    "https://www.olx.uz/nedvizhimost/kvartiry/",
                    max_pages=1,
                    category_id=1147,
                )
            ]

    listings = asyncio.run(collect())

    assert len(attempts) == 2
    assert [listing.id for listing in listings] == [7]


def test_async_client_yields_listings_for_multiple_pages() -> None:
    requests_seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(str(request.url))
        page = int(request.url.params["page"])

        if page == 1:
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": 1,
                            "relative_url": "/d/obyavlenie/item-1",
                            "title": "First",
                            "price": {"value": "100000", "currency": "UZS"},
                            "params": [{"name": "Количество комнат", "value": {"label": "3"}}],
                        }
                    ]
                },
            )

        if page == 2:
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": 2,
                            "relative_url": "/d/obyavlenie/item-2",
                            "title": "Second",
                            "price": {"value": "2000", "currency": "USD"},
                            "params": [{"name": "Площадь", "value": {"label": "70 м²"}}],
                        }
                    ]
                },
            )

        return httpx.Response(200, json={"data": []})

    async def collect() -> list[OlxListing]:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as raw_client:
            client = AsyncOLXClient(client=raw_client, retry_wait=wait_none())
            return [
                listing
                async for listing in client.fetch_category_listings(
                    "https://www.olx.uz/nedvizhimost/kvartiry/?currency=USD",
                    max_pages=3,
                    category_id=1147,
                )
            ]

    listings = asyncio.run(collect())

    assert [listing.id for listing in listings] == [1, 2]
    assert listings[0].params[0].value == "3"
    assert listings[1].price is not None
    assert listings[1].price.currency == "USD"
    assert "page=1" in requests_seen[0]
    assert "offset=0" in requests_seen[0]
    assert "category_id=1147" in requests_seen[0]
    assert "page=2" in requests_seen[1]
    assert "offset=40" in requests_seen[1]


def test_async_client_stops_when_page_only_contains_duplicates() -> None:
    requests_seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(str(request.url))
        page = int(request.url.params["page"])

        if page == 1:
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": 99,
                            "relative_url": "/d/obyavlenie/item-99",
                            "title": "Original",
                            "price": {"value": "99000", "currency": "UZS"},
                            "params": [],
                        }
                    ]
                },
            )

        if page == 2:
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": 99,
                            "relative_url": "/d/obyavlenie/item-99",
                            "title": "Original duplicate",
                            "price": {"value": "99000", "currency": "UZS"},
                            "params": [],
                        }
                    ]
                },
            )

        return httpx.Response(
            200,
            json={
                "data": [
                    {
                        "id": 100,
                        "relative_url": "/d/obyavlenie/item-100",
                        "title": "Should not be reached",
                        "price": {"value": "100000", "currency": "UZS"},
                        "params": [],
                    }
                ]
            },
        )

    async def collect() -> list[OlxListing]:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as raw_client:
            client = AsyncOLXClient(client=raw_client, retry_wait=wait_none())
            return [
                listing
                async for listing in client.fetch_category_listings(
                    "https://www.olx.uz/nedvizhimost/kvartiry/",
                    max_pages=5,
                    category_id=1147,
                )
            ]

    listings = asyncio.run(collect())

    assert [listing.id for listing in listings] == [99]
    assert len(requests_seen) == 2
