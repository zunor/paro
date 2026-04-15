# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Normalization helpers for SQL test result comparison."""

from __future__ import annotations

import datetime as dt
import math
import re
from decimal import Decimal
from typing import Any, Iterable, Sequence

_VECTOR_LITERAL_RE = re.compile(r"^\s*\[(.*)\]\s*$")


class NormalizeError(ValueError):
    """Raised when normalization fails for a value."""


def normalize_float(value: float, precision: int = 6) -> str:
    """Normalize floating-point values with fixed precision."""
    if math.isnan(value):
        return "nan"
    if math.isinf(value):
        return "inf" if value > 0 else "-inf"

    rounded = round(float(value), precision)
    # Avoid '-0.000000' noise in snapshots.
    if rounded == 0:
        rounded = 0.0
    return f"{rounded:.{precision}f}"


def normalize_decimal(value: Decimal) -> str:
    """Normalize Decimal without lossy float conversion."""
    if value.is_nan():
        return "nan"
    if value.is_infinite():
        return "-inf" if value.is_signed() else "inf"
    if value.is_zero():
        return format(abs(value), "f")
    return format(value, "f")


def normalize_timestamp(value: dt.datetime | dt.date) -> str:
    """Normalize datetime/date to UTC timestamp string."""
    if isinstance(value, dt.date) and not isinstance(value, dt.datetime):
        value = dt.datetime.combine(value, dt.time.min)

    assert isinstance(value, dt.datetime)
    if value.tzinfo is not None:
        value = value.astimezone(dt.timezone.utc).replace(tzinfo=None)

    return value.strftime("%Y-%m-%d %H:%M:%S")


def _is_numeric_sequence(value: Sequence[Any]) -> bool:
    if not isinstance(value, (list, tuple)):
        return False
    for item in value:
        if isinstance(item, bool) or not isinstance(item, (int, float, Decimal)):
            return False
    return True


def normalize_vector(values: Sequence[float], precision: int = 6) -> str:
    """Normalize numeric sequence to pgvector-like literal."""
    normalized = [normalize_float(float(item), precision) for item in values]
    return "[" + ", ".join(normalized) + "]"


def _try_normalize_vector_literal(value: str, precision: int) -> str | None:
    match = _VECTOR_LITERAL_RE.match(value)
    if match is None:
        return None

    inner = match.group(1).strip()
    if not inner:
        return "[]"

    parts = [part.strip() for part in inner.split(",")]
    numbers: list[float] = []
    for part in parts:
        if not part:
            return None
        try:
            numbers.append(float(part))
        except ValueError:
            return None

    return normalize_vector(numbers, precision)


def normalize_value(value: Any, precision: int = 6) -> str:
    """Normalize one cell value to canonical text form."""
    if value is None:
        return "NULL"

    if isinstance(value, bool):
        return "true" if value else "false"

    if isinstance(value, Decimal):
        return normalize_decimal(value)

    if isinstance(value, float):
        return normalize_float(float(value), precision)

    if isinstance(value, (dt.datetime, dt.date)):
        return normalize_timestamp(value)

    if _is_numeric_sequence(value):
        return normalize_vector(value, precision)

    if isinstance(value, str):
        if value == "":
            return "(empty)"

        maybe_vector = _try_normalize_vector_literal(value, precision)
        if maybe_vector is not None:
            return maybe_vector

        return value

    return str(value)


def normalize_row(row: Sequence[Any], precision: int = 6) -> list[str]:
    """Normalize one row."""
    return [normalize_value(cell, precision) for cell in row]


def normalize_rows(rows: Iterable[Sequence[Any]], precision: int = 6) -> list[list[str]]:
    """Normalize rows."""
    return [normalize_row(row, precision) for row in rows]


__all__ = [
    "NormalizeError",
    "normalize_decimal",
    "normalize_float",
    "normalize_timestamp",
    "normalize_value",
    "normalize_row",
    "normalize_rows",
    "normalize_vector",
]
