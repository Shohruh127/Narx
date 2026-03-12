from __future__ import annotations

import asyncio
from collections.abc import Iterable
from dataclasses import dataclass
import logging
import re

import asyncpg
import httpx
import pandas as pd
from dagster import ConfigurableResource, Definitions, EnvVar, asset
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_exponential
from tenacity.wait import wait_base

from olx_client import DEFAULT_HEADERS, OlxListing, fetch_category_listings
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
    "address_raw",
    "status",
)
ACTIVE_OLX_SOURCE_IDS_QUERY = """
    SELECT source_id
    FROM listings
    WHERE source = 'olx' AND status = 'active'
"""
DEACTIVATE_OLX_LISTINGS_QUERY = """
    UPDATE listings
    SET status = 'inactive', updated_at = CURRENT_TIMESTAMP
    WHERE source = 'olx' AND status = 'active' AND source_id = ANY($1::text[])
"""
GEOCODE_CANDIDATES_QUERY = """
    SELECT id, title, address_raw
    FROM listings
    WHERE location IS NULL
      AND source = 'olx'
      AND status = 'active'
      AND COALESCE(geocode_status, '') <> 'not_found'
"""
UPDATE_LISTING_GEOCODE_QUERY = """
    UPDATE listings
    SET
        location = ST_SetSRID(ST_MakePoint($2, $3), 4326),
        district = COALESCE($4, district),
        geocode_status = 'geocoded',
        updated_at = CURRENT_TIMESTAMP
    WHERE id = $1
"""
MARK_LISTING_GEOCODE_NOT_FOUND_QUERY = """
    UPDATE listings
    SET geocode_status = 'not_found', updated_at = CURRENT_TIMESTAMP
    WHERE id = $1 AND location IS NULL
"""
OLX_OFFER_API_URL = "https://www.olx.uz/api/v1/offers"
OLX_CHECK_CONCURRENCY = 15
OLX_CHECK_RETRY_WAIT: wait_base = wait_exponential(multiplier=1, min=1, max=8)
NOMINATIM_SEARCH_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_HEADERS = {
    "User-Agent": "NarxDagsterGeocoder/1.0",
    "Accept-Language": "uz,en",
}
NOMINATIM_RATE_LIMIT_SECONDS = 1
UZBEK_CITY_HINTS = {
    "tashkent": "Tashkent",
    "toshkent": "Tashkent",
    "ташкент": "Tashkent",
    "samarkand": "Samarkand",
    "samarqand": "Samarkand",
    "самарканд": "Samarkand",
    "bukhara": "Bukhara",
    "buxoro": "Bukhara",
    "бухара": "Bukhara",
    "andijan": "Andijan",
    "andijon": "Andijan",
    "андижан": "Andijan",
    "namangan": "Namangan",
    "fergana": "Fergana",
    "fargona": "Fergana",
    "farg'ona": "Fergana",
    "фаргана": "Fergana",
    "qarshi": "Qarshi",
    "karshi": "Qarshi",
    "қарши": "Qarshi",
    "jizzakh": "Jizzakh",
    "jizzax": "Jizzakh",
    "gulistan": "Gulistan",
    "guliston": "Gulistan",
    "термез": "Termez",
    "termez": "Termez",
    "nukus": "Nukus",
    "ургенч": "Urgench",
    "urgench": "Urgench",
    "urganch": "Urgench",
    "navoi": "Navoi",
    "navoiy": "Navoi",
    "xiva": "Khiva",
    "khiva": "Khiva",
}
GEOCODE_TOKEN_PATTERN = re.compile(r"[0-9A-Za-zА-Яа-яЁёЎўҚқҒғҲҳʼ'`-]+")
GEOCODE_STOPWORDS = frozenset(
    {
        "uzbekistan",
        "o",
        "ozbekiston",
        "o'zbekiston",
        "tashkent",
        "toshkent",
        "samarkand",
        "samarqand",
        "bukhara",
        "buxoro",
        "apartment",
        "kvartira",
        "квартира",
        "uy",
        "дом",
        "house",
        "sale",
        "rent",
        "xonali",
        "комнат",
        "комната",
        "room",
    }
)
ASYNC_PG_UPDATE_TAG_FORMAT = 'Expected format: "UPDATE <count>".'
LOGGER = logging.getLogger(__name__)


class OLXPipelineConfig(ConfigurableResource):
    postgres_dsn: str
    category_url: str = "https://www.olx.uz/nedvizhimost/kvartiry/"
    category_id: int | None = 1147
    max_pages: int = 1
    property_type: str = "apartment"
    default_status: str = "active"
    usd_to_uzs_rate: float | None = None


class OLXRetryableStatusError(Exception):
    pass


@dataclass(frozen=True)
class ListingGeocodeCandidate:
    listing_id: str
    title: str
    address_raw: str | None


@dataclass(frozen=True)
class ListingGeocodeResult:
    listing_id: str
    lon: float
    lat: float
    district: str | None


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


def _extract_address_from_params(listing: OlxListing) -> str | None:
    for param in listing.params:
        normalized_name = param.name.strip().lower()
        if not any(
            keyword in normalized_name
            for keyword in ("располож", "местополож", "адрес", "manzil", "address", "location")
        ):
            continue
        if param.value:
            return param.value.strip()
    return None


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
                "address_raw": _extract_address_from_params(listing),
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
            address_raw = COALESCE(EXCLUDED.address_raw, listings.address_raw),
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


async def fetch_active_olx_source_ids(*, postgres_dsn: str) -> list[str]:
    connection = await asyncpg.connect(postgres_dsn)
    try:
        rows = await connection.fetch(ACTIVE_OLX_SOURCE_IDS_QUERY)
    finally:
        await connection.close()

    return [str(row["source_id"]) for row in rows]


async def fetch_geocode_candidates(*, postgres_dsn: str) -> list[ListingGeocodeCandidate]:
    connection = await asyncpg.connect(postgres_dsn)
    try:
        rows = await connection.fetch(GEOCODE_CANDIDATES_QUERY)
    finally:
        await connection.close()

    return [
        ListingGeocodeCandidate(
            listing_id=str(row["id"]),
            title="" if row["title"] is None else str(row["title"]),
            address_raw=None if row["address_raw"] is None else str(row["address_raw"]),
        )
        for row in rows
    ]


def _detect_city_hint(*texts: str | None) -> str:
    for text in texts:
        if not text:
            continue
        normalized_text = text.casefold()
        for variant, city in UZBEK_CITY_HINTS.items():
            if variant in normalized_text:
                return city
    return "Tashkent"


def _build_geocode_query_text(candidate: ListingGeocodeCandidate) -> str | None:
    base_text = (candidate.address_raw or candidate.title).strip()
    if not base_text:
        return None

    city = _detect_city_hint(candidate.address_raw, candidate.title)
    normalized_text = base_text.casefold()
    parts = [base_text]

    if city.casefold() not in normalized_text:
        parts.append(city)
    if "uzbekistan" not in normalized_text and "o'zbekiston" not in normalized_text:
        parts.append("Uzbekistan")

    return ", ".join(parts)


def _tokenize_geocode_text(value: str) -> set[str]:
    normalized_tokens = set()
    for match in GEOCODE_TOKEN_PATTERN.findall(value.casefold()):
        token = match.strip("-'`ʼ")
        if len(token) < 3 or token in GEOCODE_STOPWORDS:
            continue
        normalized_tokens.add(token)
    return normalized_tokens


def _extract_district(result: dict[str, object]) -> str | None:
    address = result.get("address")
    if not isinstance(address, dict):
        return None

    for key in ("city_district", "suburb", "borough", "county", "town", "city"):
        value = address.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _is_confident_geocode_result(query_text: str, result: dict[str, object]) -> bool:
    address = result.get("address")
    if not isinstance(address, dict):
        return False
    country_code = address.get("country_code")
    if isinstance(country_code, str) and country_code.lower() != "uz":
        return False

    importance = result.get("importance")
    try:
        importance_value = float(importance) if isinstance(importance, (int, float, str)) else None
    except ValueError:
        importance_value = None
    if importance_value is not None and importance_value < 0.05:
        return False

    query_tokens = _tokenize_geocode_text(query_text)
    if not query_tokens:
        return False

    display_name = result.get("display_name")
    if not isinstance(display_name, str):
        return False

    return bool(query_tokens & _tokenize_geocode_text(display_name))


async def geocode_listing_candidate(
    candidate: ListingGeocodeCandidate,
    *,
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
) -> ListingGeocodeResult | None:
    query_text = _build_geocode_query_text(candidate)
    if query_text is None:
        return None

    async with semaphore:
        try:
            response = await client.get(
                NOMINATIM_SEARCH_URL,
                params={
                    "q": query_text,
                    "format": "jsonv2",
                    "limit": 1,
                    "addressdetails": 1,
                    "countrycodes": "uz",
                },
            )
            response.raise_for_status()
            payload = response.json()
        finally:
            await asyncio.sleep(NOMINATIM_RATE_LIMIT_SECONDS)

    if not isinstance(payload, list) or not payload:
        return None
    result = payload[0]
    if not isinstance(result, dict) or not _is_confident_geocode_result(query_text, result):
        return None

    try:
        lon = float(result["lon"])
        lat = float(result["lat"])
    except (KeyError, TypeError, ValueError):
        return None

    return ListingGeocodeResult(
        listing_id=candidate.listing_id,
        lon=lon,
        lat=lat,
        district=_extract_district(result),
    )


async def update_listing_geocode(
    listing_id: str,
    *,
    lon: float,
    lat: float,
    district: str | None,
    connection: object,
) -> None:
    await connection.execute(UPDATE_LISTING_GEOCODE_QUERY, listing_id, lon, lat, district)


async def mark_listing_geocode_not_found(listing_id: str, *, connection: object) -> None:
    await connection.execute(MARK_LISTING_GEOCODE_NOT_FOUND_QUERY, listing_id)


async def check_olx_listing_is_deactivated(
    source_id: str,
    *,
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    retry_wait: wait_base | None = None,
) -> bool:
    resolved_retry_wait = retry_wait or OLX_CHECK_RETRY_WAIT
    try:
        async for attempt in AsyncRetrying(
            retry=retry_if_exception_type((OLXRetryableStatusError, httpx.TransportError)),
            stop=stop_after_attempt(3),
            wait=resolved_retry_wait,
            reraise=True,
        ):
            with attempt:
                async with semaphore:
                    response = await client.get(f"{OLX_OFFER_API_URL}/{source_id}")
                if response.status_code == 404:
                    return True
                if response.status_code == 429:
                    raise OLXRetryableStatusError(source_id)
                if 500 <= response.status_code < 600:
                    raise OLXRetryableStatusError(source_id)
                response.raise_for_status()
                return False
    except (OLXRetryableStatusError, httpx.HTTPError):
        LOGGER.warning("Skipping OLX status deactivation check for source_id=%s", source_id, exc_info=True)
        return False


async def find_deactivated_olx_source_ids(
    source_ids: Iterable[str],
    *,
    client: httpx.AsyncClient,
    concurrency: int = OLX_CHECK_CONCURRENCY,
    retry_wait: wait_base | None = None,
) -> list[str]:
    semaphore = asyncio.Semaphore(concurrency)
    resolved_retry_wait = retry_wait or OLX_CHECK_RETRY_WAIT

    async def check_source_id(source_id: str) -> str | None:
        if await check_olx_listing_is_deactivated(
            source_id,
            client=client,
            semaphore=semaphore,
            retry_wait=resolved_retry_wait,
        ):
            return source_id
        return None

    results = await asyncio.gather(*(check_source_id(source_id) for source_id in source_ids))
    return [source_id for source_id in results if source_id is not None]


def _parse_updated_rows(command: str) -> int:
    operation, _, raw_count = command.partition(" ")
    if operation != "UPDATE":
        raise ValueError(
            f"Unexpected asyncpg command tag: {command!r}. "
            f"{ASYNC_PG_UPDATE_TAG_FORMAT}"
        )
    if not raw_count:
        raise ValueError(
            f"Missing updated row count in asyncpg command tag: {command!r}. "
            f"{ASYNC_PG_UPDATE_TAG_FORMAT}"
        )
    if not raw_count.isdigit():
        raise ValueError(
            f"Unexpected asyncpg command tag: {command!r}. "
            f"{ASYNC_PG_UPDATE_TAG_FORMAT}"
        )
    return int(raw_count)


async def deactivate_olx_source_ids(source_ids: list[str], *, postgres_dsn: str) -> int:
    if not source_ids:
        return 0

    connection = await asyncpg.connect(postgres_dsn)
    try:
        command = await connection.execute(DEACTIVATE_OLX_LISTINGS_QUERY, source_ids)
    finally:
        await connection.close()

    return _parse_updated_rows(command)


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


@asset
async def deactivate_deleted_olx_listings(olx_pipeline_config: OLXPipelineConfig) -> dict[str, int]:
    active_source_ids = await fetch_active_olx_source_ids(postgres_dsn=olx_pipeline_config.postgres_dsn)
    if not active_source_ids:
        return {"active_ids_checked": 0, "rows_deactivated": 0}

    async with httpx.AsyncClient(headers=DEFAULT_HEADERS, timeout=20.0) as client:
        deactivated_ids = await find_deactivated_olx_source_ids(
            active_source_ids,
            client=client,
        )

    deactivated_rows = await deactivate_olx_source_ids(
        deactivated_ids,
        postgres_dsn=olx_pipeline_config.postgres_dsn,
    )
    return {
        "active_ids_checked": len(active_source_ids),
        "rows_deactivated": deactivated_rows,
    }


@asset(name="geocode_listings")
async def geocode_listings(olx_pipeline_config: OLXPipelineConfig) -> dict[str, int]:
    candidates = await fetch_geocode_candidates(postgres_dsn=olx_pipeline_config.postgres_dsn)
    if not candidates:
        return {"listings_checked": 0, "rows_geocoded": 0, "rows_marked_not_found": 0}

    rows_geocoded = 0
    rows_marked_not_found = 0
    semaphore = asyncio.Semaphore(1)

    async with httpx.AsyncClient(headers=NOMINATIM_HEADERS, timeout=30.0) as client:
        connection = await asyncpg.connect(olx_pipeline_config.postgres_dsn)
        try:
            for candidate in candidates:
                geocode_result = await geocode_listing_candidate(
                    candidate,
                    client=client,
                    semaphore=semaphore,
                )
                if geocode_result is None:
                    await mark_listing_geocode_not_found(candidate.listing_id, connection=connection)
                    rows_marked_not_found += 1
                    continue

                await update_listing_geocode(
                    geocode_result.listing_id,
                    lon=geocode_result.lon,
                    lat=geocode_result.lat,
                    district=geocode_result.district,
                    connection=connection,
                )
                rows_geocoded += 1
        finally:
            await connection.close()

    return {
        "listings_checked": len(candidates),
        "rows_geocoded": rows_geocoded,
        "rows_marked_not_found": rows_marked_not_found,
    }


defs = Definitions(
    assets=[raw_olx_data, clean_real_estate_data, load_to_postgres, deactivate_deleted_olx_listings, geocode_listings],
    resources={
        "olx_pipeline_config": OLXPipelineConfig(
            postgres_dsn=EnvVar("POSTGRES_CONNECTION_STRING"),
        )
    },
)
