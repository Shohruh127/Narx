from __future__ import annotations

import asyncio

import httpx
from tenacity import wait_none

from olx_client import AsyncOLXClient, OlxListing


def test_listing_model_ignores_extra_fields_and_extracts_price_from_params() -> None:
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
    assert listing.price is not None
    assert listing.price.value == 125000.0
    assert listing.price.currency == "USD"
    assert [(param.name, param.value) for param in listing.params] == [
        ("Цена", "125 000"),
        ("Площадь", "78 м²"),
    ]


def test_async_client_retries_retryable_statuses_and_sends_browser_headers() -> None:
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
                        "url": "https://www.olx.uz/d/obyavlenie/item-7",
                        "title": "Retry worked",
                        "price": {"value": 70000, "currency": "UZS"},
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
                            "url": "https://www.olx.uz/d/obyavlenie/item-1",
                            "title": "First",
                            "price": {"value": "100000", "currency": "UZS"},
                            "params": [{"name": "Rooms", "value": {"label": "3"}}],
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
                            "url": "https://www.olx.uz/d/obyavlenie/item-2",
                            "title": "Second",
                            "price": {"value": 2000, "currency": "USD"},
                            "params": [{"name": "Area", "value": {"label": "70 m²"}}],
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
                )
            ]

    listings = asyncio.run(collect())

    assert [listing.id for listing in listings] == [1, 2]
    assert listings[0].params[0].value == "3"
    assert listings[1].price is not None
    assert listings[1].price.currency == "USD"
    assert "page=1" in requests_seen[0]
    assert "offset=0" in requests_seen[0]
    assert "category_slug=kvartiry" in requests_seen[0]
    assert "page=2" in requests_seen[1]
    assert "offset=40" in requests_seen[1]
