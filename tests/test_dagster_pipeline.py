from __future__ import annotations

import asyncio

import httpx
import pandas as pd
from tenacity import wait_none

from dagster_pipeline import (
    ListingGeocodeCandidate,
    ListingGeocodeResult,
    build_listing_upsert_query,
    deactivate_olx_source_ids,
    clean_listings_dataframe,
    fetch_geocode_candidates,
    fetch_active_olx_source_ids,
    fetch_raw_olx_data,
    find_deactivated_olx_source_ids,
    geocode_listing_candidate,
    geocode_listings,
    load_dataframe_to_postgres,
    mark_listing_geocode_not_found,
    update_listing_geocode,
)
from olx_client import OlxListing

NUM_SUCCESSFUL_OLX_IDS = 25


def test_fetch_raw_olx_data_collects_pydantic_listings(monkeypatch) -> None:
    async def mock_fetch_category_listings(category_url: str | None, max_pages: int, *, category_id: int | None = None):
        assert category_url == "https://www.olx.uz/nedvizhimost/kvartiry/"
        assert max_pages == 2
        assert category_id == 1147
        yield OlxListing.model_validate(
            {
                "id": 1,
                "relative_url": "/d/obyavlenie/flat-1",
                "title": "78 m2 квартира",
                "price": {"value": "120000", "currency": "USD"},
                "params": [],
            }
        )

    monkeypatch.setattr("dagster_pipeline.fetch_category_listings", mock_fetch_category_listings)

    listings = asyncio.run(
        fetch_raw_olx_data(
            category_url="https://www.olx.uz/nedvizhimost/kvartiry/",
            category_id=1147,
            max_pages=2,
        )
    )

    assert len(listings) == 1
    assert isinstance(listings[0], OlxListing)
    assert listings[0].id == 1


def test_clean_listings_dataframe_normalizes_prices_and_areas() -> None:
    dataframe = clean_listings_dataframe(
        [
            OlxListing.model_validate(
                {
                    "id": 15,
                    "relative_url": "/d/obyavlenie/house-15",
                    "title": "Uy 4 sotix",
                    "price": {"value": "50000", "currency": "USD"},
                    "params": [
                        {"name": "Площадь", "value": {"label": "120 m2"}},
                        {"name": "Расположение", "value": {"label": "Yunusobod, Tashkent"}},
                    ],
                }
            )
        ],
        property_type="house",
        default_status="active",
        usd_to_uzs_rate=12500,
    )

    row = dataframe.iloc[0].to_dict()
    assert list(dataframe.columns) == [
        "source",
        "source_id",
        "property_type",
        "title",
        "price_original",
        "currency_original",
        "price_uzs_normalized",
        "land_area_sotix",
        "living_area_m2",
        "address_raw",
        "status",
    ]
    assert row["source"] == "olx"
    assert row["source_id"] == "15"
    assert row["property_type"] == "house"
    assert row["price_original"] == 50000.0
    assert row["currency_original"] == "USD"
    assert row["price_uzs_normalized"] == 625000000.0
    assert row["land_area_sotix"] == 4.0
    assert row["living_area_m2"] == 120.0
    assert row["address_raw"] == "Yunusobod, Tashkent"
    assert row["status"] == "active"


def test_build_listing_upsert_query_updates_only_expected_columns() -> None:
    query = build_listing_upsert_query()

    assert "INSERT INTO listings" in query
    assert "ON CONFLICT (source, source_id) DO UPDATE" in query
    assert "address_raw = COALESCE(EXCLUDED.address_raw, listings.address_raw)" in query
    assert "price_original = EXCLUDED.price_original" in query
    assert "price_uzs_normalized = EXCLUDED.price_uzs_normalized" in query
    assert "status = EXCLUDED.status" in query
    assert "updated_at = CURRENT_TIMESTAMP" in query
    assert "currency_original = EXCLUDED.currency_original" not in query
    assert "title = EXCLUDED.title" not in query


def test_load_dataframe_to_postgres_executes_upsert(monkeypatch) -> None:
    executed: dict[str, object] = {}

    class FakeConnection:
        async def executemany(self, query: str, records: list[tuple[object, ...]]) -> None:
            executed["query"] = query
            executed["records"] = records

        async def close(self) -> None:
            executed["closed"] = True

    async def fake_connect(dsn: str) -> FakeConnection:
        executed["dsn"] = dsn
        return FakeConnection()

    monkeypatch.setattr("dagster_pipeline.asyncpg.connect", fake_connect)

    dataframe = pd.DataFrame(
        [
            {
                "source": "olx",
                "source_id": "22",
                "property_type": "apartment",
                "title": "Test listing",
                "price_original": 25000.0,
                "currency_original": "USD",
                "price_uzs_normalized": 312500000.0,
                "land_area_sotix": None,
                "living_area_m2": 65.0,
                "address_raw": "Yashnobod, Tashkent",
                "status": "active",
            }
        ]
    )

    loaded_rows = asyncio.run(
        load_dataframe_to_postgres(
            dataframe,
            postgres_dsn="postgresql://user:pass@localhost:5432/narx",
        )
    )

    assert loaded_rows == 1
    assert executed["dsn"] == "postgresql://user:pass@localhost:5432/narx"
    assert "ON CONFLICT (source, source_id) DO UPDATE" in executed["query"]
    assert executed["records"] == [
        (
            "olx",
            "22",
            "apartment",
            "Test listing",
            25000.0,
            "USD",
            312500000.0,
            None,
            65.0,
            "Yashnobod, Tashkent",
            "active",
        )
    ]
    assert executed["closed"] is True


def test_fetch_active_olx_source_ids_reads_only_active_olx_rows(monkeypatch) -> None:
    executed: dict[str, object] = {}

    class FakeConnection:
        async def fetch(self, query: str) -> list[dict[str, str]]:
            executed["query"] = query
            return [{"source_id": "11"}, {"source_id": "22"}]

        async def close(self) -> None:
            executed["closed"] = True

    async def fake_connect(dsn: str) -> FakeConnection:
        executed["dsn"] = dsn
        return FakeConnection()

    monkeypatch.setattr("dagster_pipeline.asyncpg.connect", fake_connect)

    source_ids = asyncio.run(
        fetch_active_olx_source_ids(postgres_dsn="postgresql://user:pass@localhost:5432/narx")
    )

    assert source_ids == ["11", "22"]
    assert executed["dsn"] == "postgresql://user:pass@localhost:5432/narx"
    assert "WHERE source = 'olx' AND status = 'active'" in executed["query"]
    assert executed["closed"] is True


def test_find_deactivated_olx_source_ids_retries_429_and_limits_concurrency() -> None:
    attempts_by_id: dict[str, int] = {}
    active_requests = 0
    max_active_requests = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active_requests, max_active_requests
        source_id = request.url.path.rsplit("/", maxsplit=1)[-1]
        attempts_by_id[source_id] = attempts_by_id.get(source_id, 0) + 1
        active_requests += 1
        max_active_requests = max(max_active_requests, active_requests)
        await asyncio.sleep(0.01)
        active_requests -= 1

        if source_id == "404-after-429" and attempts_by_id[source_id] == 1:
            return httpx.Response(429, json={"error": "slow down"})
        if source_id == "404-after-429":
            return httpx.Response(404, json={"error": "missing"})
        return httpx.Response(200, json={"data": {"id": source_id}})

    async def run_check() -> list[str]:
        source_ids = [str(index) for index in range(NUM_SUCCESSFUL_OLX_IDS)] + ["404-after-429"]
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await find_deactivated_olx_source_ids(
                source_ids,
                client=client,
                retry_wait=wait_none(),
            )

    deactivated_ids = asyncio.run(run_check())

    assert deactivated_ids == ["404-after-429"]
    assert attempts_by_id["404-after-429"] == 2
    assert all(str(index) not in deactivated_ids for index in range(NUM_SUCCESSFUL_OLX_IDS))
    assert max_active_requests <= 15


def test_deactivate_olx_source_ids_uses_single_bulk_update(monkeypatch) -> None:
    executed: dict[str, object] = {}

    class FakeConnection:
        async def execute(self, query: str, source_ids: list[str]) -> str:
            executed["query"] = query
            executed["source_ids"] = source_ids
            return "UPDATE 2"

        async def close(self) -> None:
            executed["closed"] = True

    async def fake_connect(dsn: str) -> FakeConnection:
        executed["dsn"] = dsn
        return FakeConnection()

    monkeypatch.setattr("dagster_pipeline.asyncpg.connect", fake_connect)

    updated_rows = asyncio.run(
        deactivate_olx_source_ids(
            ["101", "202"],
            postgres_dsn="postgresql://user:pass@localhost:5432/narx",
        )
    )

    assert updated_rows == 2
    assert executed["dsn"] == "postgresql://user:pass@localhost:5432/narx"
    assert "source_id = ANY($1::text[])" in executed["query"]
    assert "status = 'inactive'" in executed["query"]
    assert executed["source_ids"] == ["101", "202"]
    assert executed["closed"] is True


def test_fetch_geocode_candidates_reads_only_active_olx_rows_without_location(monkeypatch) -> None:
    executed: dict[str, object] = {}

    class FakeConnection:
        async def fetch(self, query: str) -> list[dict[str, str | None]]:
            executed["query"] = query
            return [
                {"id": "listing-1", "title": "Chilonzor kvartira", "address_raw": "Chilonzor 5, Tashkent"},
                {"id": "listing-2", "title": "Yakkasaroy uy", "address_raw": None},
            ]

        async def close(self) -> None:
            executed["closed"] = True

    async def fake_connect(dsn: str) -> FakeConnection:
        executed["dsn"] = dsn
        return FakeConnection()

    monkeypatch.setattr("dagster_pipeline.asyncpg.connect", fake_connect)

    candidates = asyncio.run(
        fetch_geocode_candidates(postgres_dsn="postgresql://user:pass@localhost:5432/narx")
    )

    assert candidates == [
        ListingGeocodeCandidate(
            listing_id="listing-1",
            title="Chilonzor kvartira",
            address_raw="Chilonzor 5, Tashkent",
        ),
        ListingGeocodeCandidate(listing_id="listing-2", title="Yakkasaroy uy", address_raw=None),
    ]
    assert executed["dsn"] == "postgresql://user:pass@localhost:5432/narx"
    assert "location IS NULL" in executed["query"]
    assert "source = 'olx'" in executed["query"]
    assert "status = 'active'" in executed["query"]
    assert "geocode_status" in executed["query"]
    assert executed["closed"] is True


def test_geocode_listing_candidate_falls_back_to_tashkent_and_waits(monkeypatch) -> None:
    requests: list[httpx.Request] = []
    sleeps: list[int] = []

    async def fake_sleep(delay: int) -> None:
        sleeps.append(delay)

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.url.params["q"] == "Chilonzor 5, Tashkent, Uzbekistan"
        return httpx.Response(
            200,
            json=[
                {
                    "lat": "41.2756",
                    "lon": "69.2034",
                    "importance": 0.7,
                    "display_name": "Chilonzor 5, Chilonzor tumani, Tashkent, Uzbekistan",
                    "address": {
                        "suburb": "Chilonzor tumani",
                        "city": "Tashkent",
                        "country_code": "uz",
                    },
                }
            ],
        )

    monkeypatch.setattr("dagster_pipeline.asyncio.sleep", fake_sleep)

    async def run_geocode() -> ListingGeocodeResult | None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await geocode_listing_candidate(
                ListingGeocodeCandidate(
                    listing_id="listing-1",
                    title="3 xonali kvartira",
                    address_raw="Chilonzor 5",
                ),
                client=client,
                semaphore=asyncio.Semaphore(1),
            )

    geocode_result = asyncio.run(run_geocode())

    assert geocode_result == ListingGeocodeResult(
        listing_id="listing-1",
        lon=69.2034,
        lat=41.2756,
        district="Chilonzor tumani",
    )
    assert len(requests) == 1
    assert sleeps == [1]


def test_update_listing_geocode_and_not_found_use_expected_queries() -> None:
    executed: list[tuple[str, object]] = []

    class FakeConnection:
        async def execute(self, query: str, *params: object) -> str:
            executed.append((query, params))
            return "UPDATE 1"

    async def run_queries() -> None:
        connection = FakeConnection()
        await update_listing_geocode(
            "listing-1",
            lon=69.2034,
            lat=41.2756,
            district="Chilonzor tumani",
            connection=connection,
        )
        await mark_listing_geocode_not_found("listing-2", connection=connection)

    asyncio.run(run_queries())

    assert "ST_SetSRID(ST_MakePoint($2, $3), 4326)" in executed[0][0]
    assert "district = COALESCE($4, district)" in executed[0][0]
    assert "geocode_status = 'geocoded'" in executed[0][0]
    assert executed[0][1] == ("listing-1", 69.2034, 41.2756, "Chilonzor tumani")
    assert "geocode_status = 'not_found'" in executed[1][0]
    assert executed[1][1] == ("listing-2",)


def test_geocode_listings_asset_marks_success_and_not_found(monkeypatch) -> None:
    updates: list[tuple[str, object]] = []

    class FakeConnection:
        async def execute(self, query: str, *params: object) -> str:
            updates.append((query, params))
            return "UPDATE 1"

        async def close(self) -> None:
            updates.append(("closed", ()))

    async def fake_fetch_geocode_candidates(*, postgres_dsn: str) -> list[ListingGeocodeCandidate]:
        assert postgres_dsn == "postgresql://user:pass@localhost:5432/narx"
        return [
            ListingGeocodeCandidate(
                listing_id="listing-1",
                title="Chilonzor kvartira",
                address_raw="Chilonzor 5",
            ),
            ListingGeocodeCandidate(
                listing_id="listing-2",
                title="No address listing",
                address_raw=None,
            ),
        ]

    async def fake_geocode_listing_candidate(
        candidate: ListingGeocodeCandidate,
        *,
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
    ) -> ListingGeocodeResult | None:
        assert isinstance(client, httpx.AsyncClient)
        assert semaphore.locked() is False
        if candidate.listing_id == "listing-1":
            return ListingGeocodeResult(
                listing_id="listing-1",
                lon=69.2034,
                lat=41.2756,
                district="Chilonzor tumani",
            )
        return None

    async def fake_connect(dsn: str) -> FakeConnection:
        assert dsn == "postgresql://user:pass@localhost:5432/narx"
        return FakeConnection()

    monkeypatch.setattr("dagster_pipeline.fetch_geocode_candidates", fake_fetch_geocode_candidates)
    monkeypatch.setattr("dagster_pipeline.geocode_listing_candidate", fake_geocode_listing_candidate)
    monkeypatch.setattr("dagster_pipeline.asyncpg.connect", fake_connect)

    summary = asyncio.run(
        geocode_listings.op.compute_fn.decorated_fn(
            type(
                "Config",
                (),
                {"postgres_dsn": "postgresql://user:pass@localhost:5432/narx"},
            )()
        )
    )

    assert summary == {"listings_checked": 2, "rows_geocoded": 1, "rows_marked_not_found": 1}
    assert any("geocode_status = 'geocoded'" in query for query, _ in updates if query != "closed")
    assert any("geocode_status = 'not_found'" in query for query, _ in updates if query != "closed")
    assert updates[-1] == ("closed", ())
