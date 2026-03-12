from __future__ import annotations

from text_cleaning import normalize_price, parse_area


def test_parse_area_house_extracts_both_area_types() -> None:
    result = parse_area("2.5 sotix, 150 kv.m", "house")
    assert result == {"land_area_sotix": 2.5, "living_area_m2": 150.0}


def test_parse_area_land_recognizes_cyrillic_sotka() -> None:
    result = parse_area("Участок 6 сотка", "land")
    assert result["land_area_sotix"] == 6.0
    assert result["living_area_m2"] is None


def test_parse_area_land_recognizes_cyrillic_plural_sotok() -> None:
    result = parse_area("Участок 6 соток", "land")
    assert result["land_area_sotix"] == 6.0
    assert result["living_area_m2"] is None


def test_parse_area_house_recognizes_m2_alias() -> None:
    result = parse_area("Maydoni 120 m2", "house")
    assert result["living_area_m2"] == 120.0
    assert result["land_area_sotix"] is None


def test_parse_area_house_recognizes_cyrillic_m2_alias() -> None:
    result = parse_area("Площадь 90 м2", "house")
    assert result["living_area_m2"] == 90.0
    assert result["land_area_sotix"] is None


def test_parse_area_apartment_prefers_likely_area_number() -> None:
    result = parse_area("3 xonali, 78.5", "apartment")
    assert result["living_area_m2"] == 78.5
    assert result["land_area_sotix"] is None


def test_parse_area_apartment_does_not_use_room_or_floor_numbers_as_area() -> None:
    result = parse_area("3 xonali, 2-etaj", "apartment")
    assert result["living_area_m2"] is None
    assert result["land_area_sotix"] is None


def test_parse_area_apartment_keeps_area_when_room_count_is_last() -> None:
    result = parse_area("78.5, 3 xonali", "apartment")
    assert result["living_area_m2"] == 78.5
    assert result["land_area_sotix"] is None


def test_parse_area_unknown_property_type_returns_none_fields() -> None:
    result = parse_area("7 sotix 200 kv.m", "commercial")
    assert result == {"land_area_sotix": None, "living_area_m2": None}


def test_parse_area_invalid_text_does_not_raise_and_returns_none() -> None:
    result = parse_area("Kelishiladi", "house")
    assert result == {"land_area_sotix": None, "living_area_m2": None}


def test_normalize_price_plain_number_and_currency_symbol() -> None:
    amount, currency = normalize_price("650 000", "$")
    assert amount == 650000.0
    assert currency == "USD"


def test_normalize_price_handles_single_dot_thousand_separator() -> None:
    amount, currency = normalize_price("65.000", "$")
    assert amount == 65000.0
    assert currency == "USD"


def test_normalize_price_thousand_word_in_uzbek() -> None:
    amount, currency = normalize_price("50 ming", "uzs")
    assert amount == 50000.0
    assert currency == "UZS"


def test_normalize_price_thousand_word_in_russian() -> None:
    amount, currency = normalize_price("12 тыс", "сум")
    assert amount == 12000.0
    assert currency == "UZS"


def test_normalize_price_applies_thousand_multiplier_from_currency_text() -> None:
    amount, currency = normalize_price("50", "ming so'm")
    assert amount == 50000.0
    assert currency == "UZS"


def test_normalize_price_detects_currency_from_price_text_when_currency_text_empty() -> None:
    amount, currency = normalize_price("50 000 у.е.", "")
    assert amount == 50000.0
    assert currency == "USD"


def test_normalize_price_unreadable_text_returns_none_amount() -> None:
    amount, currency = normalize_price("Kelishiladi", "USD")
    assert amount is None
    assert currency == "USD"


def test_normalize_price_unknown_currency_returns_none_currency() -> None:
    amount, currency = normalize_price("100000", "eur")
    assert amount == 100000.0
    assert currency is None


def test_normalize_price_parses_european_number_format() -> None:
    amount, currency = normalize_price("1.234,56", "usd")
    assert amount == 1234.56
    assert currency == "USD"


def test_normalize_price_returns_none_for_malformed_number() -> None:
    amount, currency = normalize_price("1,234.56.78", "usd")
    assert amount is None
    assert currency == "USD"
