from __future__ import annotations

from collections.abc import Iterable

import asyncpg
import pandas as pd
from dagster import ConfigurableResource, Definitions, EnvVar, asset

from olx_client import OlxListing, fetch_category_listings
from text_cleaning import normalize_price, parse_area

INSERT_COLUMNS = (
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
)


class OLXPipelineConfig(ConfigurableResource):
    postgres_dsn: str
    category_url: str = "https://www.olx.uz/nedvizhimost/kvartiry/"
    category_id: int | None = 1147
    max_pages: int = 1
    property_type: str = "apartment"
    default_status: str = "active"
    usd_to_uzs_rate: float | None = None


def _price_to_uzs(amount: float | None, currency: str | None, usd_to_uzs_rate: float | None) -> float | None:
    if amount is None or currency is None:
        return None
    if currency == "UZS":
        return amount
    if currency == "USD" and usd_to_uzs_rate is not None:
        return amount * usd_to_uzs_rate
    return None


def _listing_area_text(listing: OlxListing) -> str:
    param_values = [str(param.value) for param in listing.params if param.value is not None]
    return " ".join([listing.title, *param_values]).strip()


async def fetch_raw_olx_data(
    *,
    category_url: str | None,
    category_id: int | None,
    max_pages: int,
) -> list[OlxListing]:
    return [
        listing
        async for listing in fetch_category_listings(
            category_url,
            max_pages,
            category_id=category_id,
        )
    ]


def clean_listings_dataframe(
    raw_listings: Iterable[OlxListing],
    *,
    property_type: str,
    default_status: str,
    usd_to_uzs_rate: float | None,
) -> pd.DataFrame:
    records: list[dict[str, object]] = []

    for listing in raw_listings:
        raw_price_text = "" if listing.price is None or listing.price.value is None else str(listing.price.value)
        raw_currency_text = "" if listing.price is None or listing.price.currency is None else listing.price.currency
        price_original, currency_original = normalize_price(raw_price_text, raw_currency_text)
        parsed_area = parse_area(_listing_area_text(listing), property_type)

        records.append(
            {
                "source": "olx",
                "source_id": str(listing.id),
                "property_type": property_type,
                "title": listing.title,
                "price_original": price_original,
                "currency_original": currency_original,
                "price_uzs_normalized": _price_to_uzs(price_original, currency_original, usd_to_uzs_rate),
                "land_area_sotix": parsed_area["land_area_sotix"],
                "living_area_m2": parsed_area["living_area_m2"],
                "status": default_status,
            }
        )

    return pd.DataFrame.from_records(records, columns=list(INSERT_COLUMNS))


def build_listing_upsert_query() -> str:
    placeholders = ", ".join(f"${index}" for index in range(1, len(INSERT_COLUMNS) + 1))
    insert_columns = ", ".join(INSERT_COLUMNS)
    return f"""
        INSERT INTO listings ({insert_columns})
        VALUES ({placeholders})
        ON CONFLICT (source, source_id) DO UPDATE
        SET
            price_original = EXCLUDED.price_original,
            price_uzs_normalized = EXCLUDED.price_uzs_normalized,
            status = EXCLUDED.status,
            updated_at = CURRENT_TIMESTAMP
    """


def prepare_listing_records(dataframe: pd.DataFrame) -> list[tuple[object, ...]]:
    records: list[tuple[object, ...]] = []
    for row in dataframe.loc[:, list(INSERT_COLUMNS)].itertuples(index=False, name=None):
        normalized_row = tuple(None if pd.isna(value) else value for value in row)
        records.append(normalized_row)
    return records


async def load_dataframe_to_postgres(dataframe: pd.DataFrame, *, postgres_dsn: str) -> int:
    records = prepare_listing_records(dataframe)
    if not records:
        return 0

    connection = await asyncpg.connect(postgres_dsn)
    try:
        await connection.executemany(build_listing_upsert_query(), records)
    finally:
        await connection.close()

    return len(records)


@asset
async def raw_olx_data(olx_pipeline_config: OLXPipelineConfig) -> list[OlxListing]:
    return await fetch_raw_olx_data(
        category_url=olx_pipeline_config.category_url,
        category_id=olx_pipeline_config.category_id,
        max_pages=olx_pipeline_config.max_pages,
    )


@asset
def clean_real_estate_data(
    raw_olx_data: list[OlxListing],
    olx_pipeline_config: OLXPipelineConfig,
) -> pd.DataFrame:
    return clean_listings_dataframe(
        raw_olx_data,
        property_type=olx_pipeline_config.property_type,
        default_status=olx_pipeline_config.default_status,
        usd_to_uzs_rate=olx_pipeline_config.usd_to_uzs_rate,
    )


@asset
async def load_to_postgres(
    clean_real_estate_data: pd.DataFrame,
    olx_pipeline_config: OLXPipelineConfig,
) -> dict[str, int]:
    loaded_rows = await load_dataframe_to_postgres(
        clean_real_estate_data,
        postgres_dsn=olx_pipeline_config.postgres_dsn,
    )
    return {"rows_loaded": loaded_rows}


defs = Definitions(
    assets=[raw_olx_data, clean_real_estate_data, load_to_postgres],
    resources={
        "olx_pipeline_config": OLXPipelineConfig(
            postgres_dsn=EnvVar("POSTGRES_CONNECTION_STRING"),
        )
    },
)
