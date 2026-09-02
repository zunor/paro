#!/usr/bin/env python3
"""Typed result and peer-order contracts for cross-engine TPC-DS checks."""

from __future__ import annotations

import hashlib
import math
import re
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Sequence


class ResultContractError(AssertionError):
    pass


@dataclass(frozen=True)
class ColumnContract:
    name: str
    family: str
    engine_type: str


@dataclass(frozen=True)
class OrderKey:
    column: int
    descending: bool
    nulls: str | None


PARO_TYPE_FAMILIES = {
    16: "boolean",
    17: "bytes",
    18: "string",
    19: "string",
    20: "integer",
    21: "integer",
    23: "integer",
    25: "string",
    26: "integer",
    700: "float",
    701: "float",
    1043: "string",
    1082: "date",
    1083: "time",
    1114: "timestamp",
    1184: "timestamp",
    1186: "interval",
    1266: "time",
    1700: "decimal",
    2950: "uuid",
}


def paro_schema(description: Sequence[Any]) -> tuple[ColumnContract, ...]:
    columns = []
    for column in description:
        oid = int(column.type_code)
        family = PARO_TYPE_FAMILIES.get(oid)
        if family is None:
            raise ResultContractError(f"unsupported Paro result OID {oid} for {column.name}")
        columns.append(ColumnContract(_result_column_name(column.name), family, str(oid)))
    return tuple(columns)


def duckdb_schema(description: Sequence[Any]) -> tuple[ColumnContract, ...]:
    columns = []
    for column in description:
        engine_type = str(column[1]).upper()
        if engine_type == "BOOLEAN":
            family = "boolean"
        elif any(token in engine_type for token in ("INT", "UTINY", "USMALL", "UINTEGER")):
            family = "integer"
        elif engine_type.startswith(("DECIMAL", "NUMERIC")):
            family = "decimal"
        elif engine_type.startswith(("FLOAT", "DOUBLE", "REAL")):
            family = "float"
        elif engine_type.startswith(("VARCHAR", "CHAR", "STRING")):
            family = "string"
        elif engine_type.startswith("DATE"):
            family = "date"
        elif engine_type.startswith("TIMESTAMP"):
            family = "timestamp"
        elif engine_type.startswith("TIME"):
            family = "time"
        elif engine_type.startswith(("BLOB", "BYTEA")):
            family = "bytes"
        elif engine_type.startswith("INTERVAL"):
            family = "interval"
        elif engine_type.startswith("UUID"):
            family = "uuid"
        else:
            raise ResultContractError(
                f"unsupported DuckDB result type {engine_type} for {column[0]}"
            )
        columns.append(ColumnContract(_result_column_name(column[0]), family, engine_type))
    return tuple(columns)


def _result_column_name(name: Any) -> str:
    """Compare result identity independently of an engine's qualifier display."""
    return str(name).rsplit(".", 1)[-1].strip('"').lower()


def assert_compatible_schema(
    paro: Sequence[ColumnContract], duckdb: Sequence[ColumnContract]
) -> None:
    if len(paro) != len(duckdb):
        raise ResultContractError(f"schema arity mismatch: Paro={len(paro)}, DuckDB={len(duckdb)}")
    for index, (actual, expected) in enumerate(zip(paro, duckdb)):
        if actual.name != expected.name or actual.family != expected.family:
            raise ResultContractError(
                "schema mismatch at column "
                f"{index}: Paro=({actual.name},{actual.family},{actual.engine_type}) "
                f"DuckDB=({expected.name},{expected.family},{expected.engine_type})"
            )


def canonicalize_rows(
    rows: Sequence[Sequence[Any]], schema: Sequence[ColumnContract]
) -> list[tuple[Any, ...]]:
    result = []
    for row_index, row in enumerate(rows):
        if len(row) != len(schema):
            raise ResultContractError(
                f"row {row_index} has {len(row)} columns, expected {len(schema)}"
            )
        result.append(
            tuple(_canonical_value(value, column.family) for value, column in zip(row, schema))
        )
    return result


def _canonical_value(value: Any, family: str) -> Any:
    if value is None:
        return None
    if family == "boolean":
        return bool(value)
    if family == "integer":
        return int(value)
    if family == "decimal":
        decimal = value if isinstance(value, Decimal) else Decimal(str(value))
        return Decimal(0) if decimal.is_zero() else decimal.normalize()
    if family == "float":
        number = float(value)
        if math.isnan(number):
            return "NaN"
        if math.isinf(number):
            return "+Inf" if number > 0 else "-Inf"
        return 0.0 if number == 0 else number
    if family in {"date", "time", "timestamp"}:
        return value.isoformat() if isinstance(value, (date, datetime, time)) else str(value)
    if family == "bytes":
        return bytes(value).hex()
    return str(value)


def multiset_digest(rows: Sequence[tuple[Any, ...]]) -> str:
    return _counter_digest(Counter(rows))


def assert_same_multiset(
    actual: Sequence[tuple[Any, ...]], expected: Sequence[tuple[Any, ...]]
) -> None:
    actual_counter = Counter(actual)
    expected_counter = Counter(expected)
    if actual_counter == expected_counter:
        return
    missing = expected_counter - actual_counter
    unexpected = actual_counter - expected_counter
    raise ResultContractError(
        f"row multiset mismatch: missing={sum(missing.values())}, "
        f"unexpected={sum(unexpected.values())}"
    )


def _counter_digest(counter: Counter[tuple[Any, ...]]) -> str:
    digest = hashlib.sha256()
    for row, count in sorted(counter.items(), key=lambda item: repr(item[0])):
        digest.update(repr(row).encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(count).encode("ascii"))
        digest.update(b"\n")
    return digest.hexdigest()


def parse_order_contract(query: str, schema: Sequence[ColumnContract]) -> tuple[OrderKey, ...]:
    clause = _top_level_order_clause(query)
    if clause is None:
        return ()
    names = {column.name: index for index, column in enumerate(schema)}
    keys = []
    for raw in _split_top_level(clause):
        expression = raw.strip()
        nulls = None
        null_match = re.search(r"(?is)\s+nulls\s+(first|last)\s*$", expression)
        if null_match:
            nulls = null_match.group(1).lower()
            expression = expression[: null_match.start()].strip()
        descending = False
        direction = re.search(r"(?is)\s+(asc|desc)\s*$", expression)
        if direction:
            descending = direction.group(1).lower() == "desc"
            expression = expression[: direction.start()].strip()
        if nulls is None:
            # The harness configures DuckDB to Paro/PostgreSQL semantics:
            # descending defaults to NULLS FIRST, ascending to NULLS LAST.
            nulls = "first" if descending else "last"
        if expression.isdigit():
            column = int(expression) - 1
        elif re.fullmatch(r'(?is)(?:"[^"]+"|[a-z_][a-z0-9_]*)(?:\.(?:"[^"]+"|[a-z_][a-z0-9_]*))*', expression):
            name = expression.rsplit(".", 1)[-1].strip('"').lower()
            column = names.get(name, -1)
        else:
            column = -1
        if not 0 <= column < len(schema):
            raise ResultContractError(
                f"top-level ORDER BY expression is not a result column: {raw.strip()!r}"
            )
        keys.append(OrderKey(column, descending, nulls))
    if not keys:
        raise ResultContractError("top-level ORDER BY has no keys")
    return tuple(keys)


def assert_peer_order(
    rows: Sequence[tuple[Any, ...]], keys: Sequence[OrderKey]
) -> str | None:
    if not keys:
        return None
    for index in range(1, len(rows)):
        if _compare_order(rows[index - 1], rows[index], keys) > 0:
            raise ResultContractError(f"result violates ORDER BY at rows {index - 1}/{index}")
    digest = hashlib.sha256()
    for row in rows:
        digest.update(repr(tuple(row[key.column] for key in keys)).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def _compare_order(left: tuple[Any, ...], right: tuple[Any, ...], keys: Sequence[OrderKey]) -> int:
    for key in keys:
        lhs, rhs = left[key.column], right[key.column]
        if lhs == rhs:
            continue
        if lhs is None or rhs is None:
            if lhs is None:
                return -1 if key.nulls == "first" else 1
            return 1 if key.nulls == "first" else -1
        comparison = -1 if lhs < rhs else 1
        return -comparison if key.descending else comparison
    return 0


def _top_level_order_clause(query: str) -> str | None:
    tokens = _top_level_tokens(query)
    for index in range(len(tokens) - 1):
        if tokens[index][0] == "order" and tokens[index + 1][0] == "by":
            start = tokens[index + 1][2]
            end = len(query)
            for token, token_start, _ in tokens[index + 2 :]:
                if token in {"limit", "offset", "fetch", "for"}:
                    end = token_start
                    break
            return query[start:end].strip().rstrip(";")
    return None


def _top_level_tokens(query: str) -> list[tuple[str, int, int]]:
    tokens = []
    depth = 0
    index = 0
    while index < len(query):
        char = query[index]
        following = query[index + 1] if index + 1 < len(query) else ""
        if char in {"'", '"'}:
            quote = char
            index += 1
            while index < len(query):
                if query[index] == quote:
                    if index + 1 < len(query) and query[index + 1] == quote:
                        index += 2
                        continue
                    index += 1
                    break
                index += 1
            continue
        if char == "-" and following == "-":
            newline = query.find("\n", index + 2)
            index = len(query) if newline < 0 else newline + 1
            continue
        if char == "/" and following == "*":
            end = query.find("*/", index + 2)
            index = len(query) if end < 0 else end + 2
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth = max(0, depth - 1)
        elif depth == 0 and (char.isalpha() or char == "_"):
            end = index + 1
            while end < len(query) and (query[end].isalnum() or query[end] == "_"):
                end += 1
            tokens.append((query[index:end].lower(), index, end))
            index = end
            continue
        index += 1
    return tokens


def _split_top_level(clause: str) -> list[str]:
    parts = []
    depth = 0
    start = 0
    quote = None
    index = 0
    while index < len(clause):
        char = clause[index]
        if quote is not None:
            if char == quote:
                if index + 1 < len(clause) and clause[index + 1] == quote:
                    index += 2
                    continue
                quote = None
        elif char in {"'", '"'}:
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth = max(0, depth - 1)
        elif char == "," and depth == 0:
            parts.append(clause[start:index])
            start = index + 1
        index += 1
    parts.append(clause[start:])
    return parts
