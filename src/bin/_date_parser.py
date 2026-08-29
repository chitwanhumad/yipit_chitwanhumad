"""Parse mixed date strings into a date, or None if invalid."""

from datetime import date

from dateutil import parser as dateparser


def parse_date(value: str | None) -> date | None:
    if not value or not value.strip():
        return None
    try:
        return dateparser.parse(value, dayfirst=False, fuzzy=True).date()
    except (ValueError, OverflowError):
        return None
