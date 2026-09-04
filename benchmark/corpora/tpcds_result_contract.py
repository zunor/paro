#!/usr/bin/env python3
# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

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
    logical_type: str
    engine_type: str


@dataclass(frozen=True)
class OrderKey:
    column: int
    descending: bool
    nulls: str | None


PARO_EXACT_TYPES = {
    16: "boolean",
    17: "bytes",
    18: "char",
    19: "name",
    20: "int64",
    21: "int16",
    23: "int32",
    25: "string",
    26: "uint32",
    700: "float32",
    701: "float64",
    1043: "string",
    1082: "date",
    1083: "time",
    1114: "timestamp",
    1184: "timestamptz",
    1186: "interval",
    1266: "timetz",
    2950: "uuid",
}


def paro_schema(description: Sequence[Any]) -> tuple[ColumnContract, ...]:
    columns = []
    for column in description:
        oid = int(column.type_code)
        if oid == 1700:
            precision = getattr(column, "precision", None)
            scale = getattr(column, "scale", None)
            logical_type = (
                f"decimal({int(precision)},{int(scale)})"
                if precision not in (None, 0) and scale is not None
                else "numeric"
            )
        else:
            logical_type = PARO_EXACT_TYPES.get(oid)
        if logical_type is None:
            raise ResultContractError(f"unsupported Paro result OID {oid} for {column.name}")
        columns.append(
            ColumnContract(_result_column_name(column.name), logical_type, str(oid))
        )
    return tuple(columns)


def duckdb_schema(description: Sequence[Any]) -> tuple[ColumnContract, ...]:
    columns = []
    for column in description:
        engine_type = str(column[1]).upper()
        if engine_type == "BOOLEAN":
            logical_type = "boolean"
        elif engine_type == "TINYINT":
            logical_type = "int8"
        elif engine_type == "SMALLINT":
            logical_type = "int16"
        elif engine_type in {"INTEGER", "INT"}:
            logical_type = "int32"
        elif engine_type == "BIGINT":
            logical_type = "int64"
        elif engine_type == "HUGEINT":
            logical_type = "int128"
        elif engine_type == "UTINYINT":
            logical_type = "uint8"
        elif engine_type == "USMALLINT":
            logical_type = "uint16"
        elif engine_type == "UINTEGER":
            logical_type = "uint32"
        elif engine_type == "UBIGINT":
            logical_type = "uint64"
        elif engine_type == "UHUGEINT":
            logical_type = "uint128"
        elif re.fullmatch(r"(?:DECIMAL|NUMERIC)\(\d+,\d+\)", engine_type):
            logical_type = engine_type.lower()
        elif engine_type in {"DECIMAL", "NUMERIC"}:
            logical_type = "numeric"
        elif engine_type in {"FLOAT", "REAL"}:
            logical_type = "float32"
        elif engine_type == "DOUBLE":
            logical_type = "float64"
        elif engine_type.startswith(("VARCHAR", "CHAR", "STRING")):
            logical_type = "string"
        elif engine_type == "DATE":
            logical_type = "date"
        elif engine_type in {"TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ"}:
            logical_type = "timestamptz"
        elif engine_type.startswith("TIMESTAMP"):
            logical_type = "timestamp"
        elif engine_type in {"TIME WITH TIME ZONE", "TIMETZ"}:
            logical_type = "timetz"
        elif engine_type.startswith("TIME"):
            logical_type = "time"
        elif engine_type.startswith(("BLOB", "BYTEA")):
            logical_type = "bytes"
        elif engine_type.startswith("INTERVAL"):
            logical_type = "interval"
        elif engine_type.startswith("UUID"):
            logical_type = "uuid"
        else:
            raise ResultContractError(
                f"unsupported DuckDB result type {engine_type} for {column[0]}"
            )
        columns.append(
            ColumnContract(_result_column_name(column[0]), logical_type, engine_type)
        )
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
        if actual.name != expected.name or actual.logical_type != expected.logical_type:
            raise ResultContractError(
                "schema mismatch at column "
                f"{index}: Paro=({actual.name},{actual.logical_type},{actual.engine_type}) "
                f"DuckDB=({expected.name},{expected.logical_type},{expected.engine_type})"
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
            tuple(
                _canonical_value(value, column.logical_type)
                for value, column in zip(row, schema)
            )
        )
    return result


def _canonical_value(value: Any, logical_type: str) -> Any:
    if value is None:
        return None
    if logical_type == "boolean":
        return bool(value)
    if logical_type.startswith(("int", "uint")):
        return int(value)
    if logical_type.startswith(("decimal", "numeric")):
        decimal = value if isinstance(value, Decimal) else Decimal(str(value))
        return Decimal(0) if decimal.is_zero() else decimal.normalize()
    if logical_type.startswith("float"):
        number = float(value)
        if math.isnan(number):
            return "NaN"
        if math.isinf(number):
            return "+Inf" if number > 0 else "-Inf"
        return 0.0 if number == 0 else number
    if logical_type in {"date", "time", "timetz", "timestamp", "timestamptz"}:
        return value.isoformat() if isinstance(value, (date, datetime, time)) else str(value)
    if logical_type == "bytes":
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
    names: dict[str, list[int]] = {}
    for index, column in enumerate(schema):
        names.setdefault(column.name, []).append(index)
    select_expressions = _top_level_select_expressions(query, schema)
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
            matches = names.get(name, [])
            if not matches:
                matches = select_expressions.get(_normalize_sql_expression(expression), [])
            if len(matches) > 1:
                expression_matches = select_expressions.get(
                    _normalize_sql_expression(expression), []
                )
                # Repeated projections of the same source expression induce
                # identical peer groups. Either ordinal is therefore a valid
                # witness for ORDER BY, while genuinely ambiguous aliases
                # still fail closed.
                if expression_matches != matches:
                    raise ResultContractError(
                        "top-level ORDER BY expression is ambiguous in the result schema: "
                        f"{expression!r}"
                    )
                matches = matches[:1]
            column = matches[0] if matches else -1
        else:
            matches = select_expressions.get(_normalize_sql_expression(expression), [])
            # Every entry under one normalized expression is peer-equivalent,
            # so duplicate projections may use their first ordinal.
            if len(matches) > 1:
                matches = matches[:1]
            column = matches[0] if matches else -1
        if not 0 <= column < len(schema):
            raise ResultContractError(
                f"top-level ORDER BY expression is not a result column: {raw.strip()!r}"
            )
        keys.append(OrderKey(column, descending, nulls))
    if not keys:
        raise ResultContractError("top-level ORDER BY has no keys")
    return tuple(keys)


def _top_level_select_expressions(
    query: str, schema: Sequence[ColumnContract]
) -> dict[str, list[int]]:
    """Bind ORDER BY source expressions to their projected result ordinals."""
    tokens = _top_level_tokens(query)
    select_index = next(
        (index for index, (token, _, _) in enumerate(tokens) if token == "select"),
        None,
    )
    if select_index is None:
        return {}
    from_token = next(
        (entry for entry in tokens[select_index + 1 :] if entry[0] == "from"),
        None,
    )
    if from_token is None:
        return {}

    select_start = tokens[select_index][2]
    items = _split_top_level(query[select_start : from_token[1]])
    if len(items) != len(schema):
        return {}

    expressions: dict[str, list[int]] = {}
    for index, (raw_item, column) in enumerate(zip(items, schema)):
        item = raw_item.strip()
        item_tokens = _top_level_tokens(item)
        expression = item

        for token_index in range(len(item_tokens) - 1, -1, -1):
            token, token_start, _ = item_tokens[token_index]
            if token == "as" and token_index + 1 == len(item_tokens) - 1:
                expression = item[:token_start].strip()
                break
        else:
            if item_tokens:
                alias, alias_start, _ = item_tokens[-1]
                if (
                    alias == column.name
                    and alias_start > 0
                    and item[alias_start - 1].isspace()
                ):
                    expression = item[:alias_start].strip()

        normalized = _normalize_sql_expression(expression)
        if normalized:
            expressions.setdefault(normalized, []).append(index)
    return expressions


def _normalize_sql_expression(expression: str) -> str:
    """Normalize SQL outside quoted literals and identifiers only."""
    result: list[str] = []
    quote: str | None = None
    index = 0
    while index < len(expression):
        character = expression[index]
        if quote is not None:
            result.append(character)
            if character == quote:
                if index + 1 < len(expression) and expression[index + 1] == quote:
                    result.append(expression[index + 1])
                    index += 1
                else:
                    quote = None
        elif character in {"'", '"'}:
            quote = character
            result.append(character)
        elif not character.isspace():
            result.append(character.lower())
        index += 1
    return "".join(result)


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
