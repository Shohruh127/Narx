from __future__ import annotations

import re


_NUMBER_RE = re.compile(r"\d+(?:[\s.,]\d+)*")
_SOTIX_RE = re.compile(r"(\d+(?:[\s.,]\d+)*)\s*(?:sotix|sotka|сотка)\b", re.IGNORECASE)
_M2_RE = re.compile(r"(\d+(?:[\s.,]\d+)*)\s*(?:kv\.?\s*m|кв\.?\s*м|m2)\b", re.IGNORECASE)
_THOUSAND_RE = re.compile(r"\b(?:ming|тыс)\b", re.IGNORECASE)


def _parse_number(raw_number: str) -> float | None:
    value = raw_number.strip()
    if not value:
        return None

    cleaned = value.replace(" ", "")
    if not re.fullmatch(r"[0-9.,]+", cleaned):
        return None

    comma_count = cleaned.count(",")
    dot_count = cleaned.count(".")

    if comma_count and dot_count:
        if cleaned.rfind(",") > cleaned.rfind("."):
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    elif comma_count:
        if comma_count > 1:
            if re.fullmatch(r"\d{1,3}(,\d{3})+", cleaned):
                cleaned = cleaned.replace(",", "")
            else:
                return None
        else:
            left, right = cleaned.split(",")
            if len(right) == 3 and left.isdigit():
                cleaned = left + right
            else:
                cleaned = left + "." + right
    elif dot_count > 1:
        if re.fullmatch(r"\d{1,3}(\.\d{3})+", cleaned):
            cleaned = cleaned.replace(".", "")
        else:
            return None

    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_area(text: str, property_type: str) -> dict[str, float | None]:
    result: dict[str, float | None] = {"land_area_sotix": None, "living_area_m2": None}
    if not text:
        return result

    property_kind = property_type.lower().strip()

    if property_kind in {"house", "land"}:
        sotix_match = _SOTIX_RE.search(text)
        if sotix_match:
            result["land_area_sotix"] = _parse_number(sotix_match.group(1))

        m2_match = _M2_RE.search(text)
        if m2_match:
            result["living_area_m2"] = _parse_number(m2_match.group(1))

        return result

    if property_kind == "apartment":
        m2_match = _M2_RE.search(text)
        if m2_match:
            result["living_area_m2"] = _parse_number(m2_match.group(1))
            return result

        number_matches = list(_NUMBER_RE.finditer(text))
        if number_matches:
            result["living_area_m2"] = _parse_number(number_matches[-1].group(0))

    return result


def normalize_price(price_text: str, currency_text: str) -> tuple[float | None, str | None]:
    normalized_currency: str | None = None
    currency_candidate = " ".join(part for part in [currency_text.strip(), price_text.strip()] if part).lower()

    if re.search(r"(у\.?е\.?|\$|\bye\b|\busd\b)", currency_candidate, flags=re.IGNORECASE):
        normalized_currency = "USD"
    elif re.search(r"(sum|so['’`]?m|сум|\buzs\b)", currency_candidate, flags=re.IGNORECASE):
        normalized_currency = "UZS"

    if not price_text:
        return None, normalized_currency

    number_match = _NUMBER_RE.search(price_text)
    if not number_match:
        return None, normalized_currency

    amount = _parse_number(number_match.group(0))
    if amount is None:
        return None, normalized_currency

    if _THOUSAND_RE.search(price_text):
        amount *= 1000

    return amount, normalized_currency
