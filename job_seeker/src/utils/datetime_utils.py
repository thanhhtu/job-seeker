from __future__ import annotations

import os
from datetime import date, datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

DEFAULT_APP_TZ = "Asia/Ho_Chi_Minh"
APP_TZ = ZoneInfo(os.getenv("APP_TIMEZONE", DEFAULT_APP_TZ))
UTC_TZ = timezone.utc
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
_SYSTEM_TZ_SENTINEL = object()


def system_tz():
    """Runtime local timezone of the current system."""
    return datetime.now().astimezone().tzinfo or UTC_TZ


def now() -> datetime:
    """Canonical current time for storage/computation (UTC aware)."""
    return datetime.now(UTC_TZ)


def now_str() -> str:
    """Timezone-explicit timestamp to avoid ambiguous naive strings."""
    return now().isoformat(timespec="seconds")


def to_local(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=APP_TZ)
    return dt.astimezone(APP_TZ)


def to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=APP_TZ)
    return dt.astimezone(UTC_TZ)


def parse_datetime(val: Any) -> datetime | None:
    """Parse crawler/JSON values into an aware UTC datetime."""
    if val is None or val == "":
        return None

    if isinstance(val, datetime):
        return to_utc(val)

    if isinstance(val, (int, float)):
        # Support both seconds and milliseconds epoch values.
        ts = float(val)
        if abs(ts) >= 1_000_000_000_000:
            ts /= 1000
        return datetime.fromtimestamp(ts, tz=UTC_TZ)

    text = str(val).strip()
    if not text:
        return None

    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        try:
            d = date.fromisoformat(text)
            return datetime(d.year, d.month, d.day, tzinfo=APP_TZ).astimezone(UTC_TZ)
        except ValueError:
            pass

    # Legacy JSON used "Z" on local wall-clock values; keep backward compatibility.
    if text.endswith("Z"):
        local_text = text[:-1]
        if "." in local_text:
            local_text = local_text.split(".", 1)[0]
        try:
            legacy_local = datetime.fromisoformat(local_text)
            return legacy_local.replace(tzinfo=APP_TZ).astimezone(UTC_TZ)
        except ValueError:
            pass

    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        try:
            dt = datetime.strptime(text, DATETIME_FORMAT)
        except ValueError:
            return None

    if dt.tzinfo is None:
        # Naive historical values are assumed to be app local time.
        dt = dt.replace(tzinfo=APP_TZ)
    return dt.astimezone(UTC_TZ)


def normalize_datetime_value(val: Any) -> str | None:
    """Normalize to a timezone-explicit UTC string for JSON schema files."""
    dt = parse_datetime(val)
    return dt.isoformat(timespec="seconds") if dt else None


def format_datetime(
    dt: datetime | None,
    *,
    tz: ZoneInfo | timezone | None | object = _SYSTEM_TZ_SENTINEL,
) -> str | None:
    if dt is None:
        return None
    dt = to_utc(dt)
    if tz is _SYSTEM_TZ_SENTINEL:
        tz = system_tz()
    if tz is None:
        return dt.isoformat(timespec="seconds")
    return dt.astimezone(tz).strftime(DATETIME_FORMAT)


def to_db_datetime(val: Any) -> datetime | None:
    """UTC-aware datetime for PostgreSQL timestamptz."""
    return parse_datetime(val)
