from __future__ import annotations

import asyncio

import pandas as pd

from dagster_pipeline import (
    build_listing_upsert_query,
    clean_listings_dataframe,
    fetch_raw_olx_data,
    load_dataframe_to_postgres,
)
from olx_client import OlxListing


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
                    "params": [{"name": "Площадь", "value": {"label": "120 m2"}}],
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
    assert row["status"] == "active"


def test_build_listing_upsert_query_updates_only_expected_columns() -> None:
    query = build_listing_upsert_query()

    assert "INSERT INTO listings" in query
    assert "ON CONFLICT (source, source_id) DO UPDATE" in query
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
        ("olx", "22", "apartment", "Test listing", 25000.0, "USD", 312500000.0, None, 65.0, "active")
    ]
    assert executed["closed"] is True
