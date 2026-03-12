from __future__ import annotations
import re

# Tozalash uchun oson va kuchli Regex'lar
_NUMBER_RE = re.compile(r"\d+(?:[\s.,]\d+)*")
_SOTIX_RE = re.compile(r"(\d+(?:[\s.,]\d+)*)\s*(?:sotix|sotka|sot|сотка|соток|сотки)\b", re.IGNORECASE)
_M2_RE = re.compile(r"(\d+(?:[\s.,]\d+)*)\s*(?:kv\.?\s*m|кв\.?\s*м|m2|м2)\b", re.IGNORECASE)
_THOUSAND_RE = re.compile(r"\b(?:ming|тыс\.?)\b", re.IGNORECASE)

def _parse_number(raw_number: str) -> float | None:
    if not raw_number:
        return None
        
    cleaned = raw_number.strip()
    # Faqat raqam, nuqta, vergul va probel qolsin
    cleaned = re.sub(r"[^\d., ]", "", cleaned)
    
    # 1. Probeldagi "minglik" ajratkichlarni olib tashlaymiz (masalan: 120 000 -> 120000)
    cleaned = cleaned.replace(" ", "")
    if not cleaned:
         return None

    # 2. Agar ham vergul, ham nuqta bo'lsa: 1,234.56 yoki 1.234,56
    if '.' in cleaned and ',' in cleaned:
        last_dot = cleaned.rfind('.')
        last_comma = cleaned.rfind(',')
        if last_dot > last_comma:
            # Nuqta kasr ajratuvchi: 1,234.56 -> 1234.56
            cleaned = cleaned.replace(',', '')
        else:
            # Vergul kasr ajratuvchi: 1.234,56 -> 1234.56
            cleaned = cleaned.replace('.', '').replace(',', '.')
            
    # 3. Agar faqat nuqta bo'lsa
    elif '.' in cleaned:
        # Agar "65.000" kabi bo'lsa (nuqtadan keyin roppa-rosa 3 ta raqam va faqat bitta nuqta)
        if cleaned.count('.') == 1:
            left, right = cleaned.split('.')
            if len(right) == 3:
                cleaned = left + right # 65000
                
    # 4. Agar faqat vergul bo'lsa
    elif ',' in cleaned:
         # Xuddi shunday, "65,000" tekshiruvi, lekin odatda vergul o'nlik kasr sifatida ko'p ishlatiladi (2,5)
         if cleaned.count(',') == 1:
            left, right = cleaned.split(',')
            if len(right) == 3:
                cleaned = left + right # 65000
            else:
                cleaned = left + '.' + right # 2.5
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

    # Kvartira bo'ladimi yoki Uy bo'ladimi, kvadrat metrni izlaymiz
    m2_match = _M2_RE.search(text)
    if m2_match:
        result["living_area_m2"] = _parse_number(m2_match.group(1))
        return result

    # FALLBACK (Faqat kvartira va "m2" umuman topilmaganda ishlaydi)
    if property_kind == "apartment":
        # Barcha raqamlarni topamiz
        numbers_str = _NUMBER_RE.findall(text)
        if numbers_str:
            parsed_numbers = [_parse_number(n) for n in numbers_str]
            valid_numbers = [n for n in parsed_numbers if n is not None and 10 <= n <= 500]
            if valid_numbers:
                # Kvartira uchun maydon odatda matndagi qolgan raqamlardan (xonalar, qavatlar) katta bo'ladi
                result["living_area_m2"] = max(valid_numbers)

    return result

def normalize_price(price_text: str, currency_text: str) -> tuple[float | None, str | None]:
    normalized_currency: str | None = None
    
    # "50 ming" dagi ming so'zini tutib qolish uchun, ikkala textni birlashtiramiz
    full_text = f"{price_text} {currency_text}".lower()

    if re.search(r"(у\.?е\.?|\$|\bye\b|\busd\b)", full_text):
        normalized_currency = "USD"
    elif re.search(r"(sum|so['’`]?m|сум|\buzs\b)", full_text):
        normalized_currency = "UZS"

    if not price_text:
        return None, normalized_currency

    number_match = _NUMBER_RE.search(price_text)
    if not number_match:
        return None, normalized_currency

    amount = _parse_number(number_match.group(0))
    if amount is None:
        return None, normalized_currency

    # Agar full_text (yoki price yoki currency) da "ming" qatnashgan bo'lsa
    if _THOUSAND_RE.search(full_text):
        amount *= 1000

    return amount, normalized_currency
