# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Versioned, exact benchmark result serialization.

This module freezes the exact normalization already used by the harness as
protocol v1. Digest protocols are immutable once named: a future
representation change must add a new function and validation mode instead of
silently changing existing workload oracles.
"""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
import hashlib
import json
from typing import Any, Iterable


def normalize_value_v1(value: Any) -> Any:
    """Normalize a SQL value for the exact JSON v1 result protocol."""

    if isinstance(value, Decimal):
        # JSON numbers and Python floats cannot represent DECIMAL exactly.
        # Preserve both the fixed-point value and its scale.
        return format(value, "f")
    if isinstance(value, (date, datetime, time)):
        return value.isoformat()
    if isinstance(value, memoryview):
        return value.tobytes().hex()
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, tuple):
        return [normalize_value_v1(item) for item in value]
    if isinstance(value, list):
        return [normalize_value_v1(item) for item in value]
    if isinstance(value, dict):
        return {key: normalize_value_v1(item) for key, item in value.items()}
    return value


def normalize_row_v1(row: Iterable[Any]) -> list[Any]:
    return [normalize_value_v1(value) for value in row]


def ordered_rows_digest_v1(rows: Iterable[Iterable[Any]]) -> str:
    """Hash ordered rows using the immutable exact JSON v1 protocol."""

    payload = json.dumps(
        [normalize_row_v1(row) for row in rows],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()
