import re

CURRENCY_SYMBOLS = {
    "$": "USD",
    "€": "EUR",
    "£": "GBP",
    "¥": "JPY",
}

CURRENCY_CODE_PATTERN = re.compile(
    r"(USD|EUR|GBP|JPY)\b",
    re.IGNORECASE,
)

MULTIPLIERS = {
    "b": 1_000_000_000, "bn": 1_000_000_000, "billion": 1_000_000_000,
    "m": 1_000_000, "mm": 1_000_000, "million": 1_000_000,
    "k": 1_000, "thousand": 1_000,
}

NUMBER_PATTERN = re.compile(
    r"([\d.]+)\s*(billion|million|thousand|bn|mm|b|m|k)?",
    re.IGNORECASE,
)


def _extract_currency(text: str) -> tuple[str | None, str]:
    for symbol, code in CURRENCY_SYMBOLS.items():
        if symbol in text:
            return code, text.replace(symbol, "")
    match = CURRENCY_CODE_PATTERN.search(text)
    if match:
        return match.group(1).upper(), CURRENCY_CODE_PATTERN.sub("", text, count=1)
    return None, text


def _parse_amount(text: str) -> float | None:
    text = CURRENCY_CODE_PATTERN.sub("", text)
    text = text.replace(",", "").strip()
    match = NUMBER_PATTERN.search(text)
    if not match:
        return None
    number_str, unit = match.groups()
    try:
        number = float(number_str)
    except ValueError:
        return None
    return number * MULTIPLIERS.get((unit or "").lower(), 1)


def normalize_revenue(revenue: str | None) -> list:
    """[revenue_currency, revenue_actual, revenue_min_currency, revenue_max_currency]"""
    if not revenue or not revenue.strip():
        return []

    text = revenue.strip()
    currency, remainder = _extract_currency(text)
    if currency is None:
        return []

    if " - " in text:
        low_part, high_part = text.split(" - ", 1)
        _, low_remainder = _extract_currency(low_part)
        _, high_remainder = _extract_currency(high_part)
        low_value = _parse_amount(low_remainder)
        high_value = _parse_amount(high_remainder)
        if low_value is None or high_value is None:
            return []
        revenue_min, revenue_max = sorted((low_value, high_value))
        return [currency, None, revenue_min, revenue_max]

    value = _parse_amount(remainder)
    if value is None:
        return []
    return [currency, value, None, None]