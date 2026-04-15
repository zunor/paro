# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import datetime as dt
from decimal import Decimal

from harness.normalize import (
    normalize_decimal,
    normalize_row,
    normalize_rows,
    normalize_value,
)


def test_normalize_null_bool_empty_string() -> None:
    assert normalize_value(None) == "NULL"
    assert normalize_value(True) == "true"
    assert normalize_value(False) == "false"
    assert normalize_value("") == "(empty)"


def test_normalize_float_precision() -> None:
    assert normalize_value(0.100000001) == "0.100000"
    assert normalize_value(12.3456789) == "12.345679"
    assert normalize_value(-0.0) == "0.000000"


def test_normalize_decimal_preserves_exact_text() -> None:
    assert normalize_decimal(Decimal("123.40")) == "123.40"
    assert normalize_decimal(Decimal("0.0001")) == "0.0001"
    assert (
        normalize_value(Decimal("170141183460469231731687303715884105727"))
        == "170141183460469231731687303715884105727"
    )


def test_normalize_timestamp_to_utc() -> None:
    ts = dt.datetime(2024, 1, 1, 8, 0, 0, tzinfo=dt.timezone(dt.timedelta(hours=8)))
    assert normalize_value(ts) == "2024-01-01 00:00:00"

    date_value = dt.date(2024, 1, 2)
    assert normalize_value(date_value) == "2024-01-02 00:00:00"


def test_normalize_vector_literals_and_sequences() -> None:
    assert normalize_value("[0.100000001, 0.2]") == "[0.100000, 0.200000]"
    assert normalize_value([1, 2.1234567, 3]) == "[1.000000, 2.123457, 3.000000]"


def test_non_vector_string_is_preserved() -> None:
    assert normalize_value("hello") == "hello"
    assert normalize_value("[abc, def]") == "[abc, def]"


def test_normalize_rows() -> None:
    rows = [(1, None, "", 1.2), (2, True, "x", 3.1415926535)]
    assert normalize_rows(rows, precision=4) == [
        ["1", "NULL", "(empty)", "1.2000"],
        ["2", "true", "x", "3.1416"],
    ]
    assert normalize_row(rows[0], precision=2) == ["1", "NULL", "(empty)", "1.20"]
