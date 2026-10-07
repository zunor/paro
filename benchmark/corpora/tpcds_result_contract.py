#!/usr/bin/env python3
# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Typed result and peer-order contracts for cross-engine TPC-DS checks."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Any, Sequence

from exact_result_value import ResultContractError, evidence_bytes, validate_exact


RESULT_CONTRACT_VERSION = "typed-result-v4"


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
    expression: tuple | None = None


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
    """Preserve wire identity, including case, dots and explicit aliases."""
    return str(name)


def assert_compatible_schema(
    paro: Sequence[ColumnContract], duckdb: Sequence[ColumnContract],
    *, query: str | None = None,
) -> None:
    if len(paro) != len(duckdb):
        raise ResultContractError(f"schema arity mismatch: Paro={len(paro)}, DuckDB={len(duckdb)}")
    if query is not None:
        import duckdb as duckdb_module
        from bound_result_contract import BoundResult
        with duckdb_module.connect() as parser:
            bound = BoundResult(query, parser)
            bound.check_identity(paro, "paro")
            bound.check_identity(duckdb, "duckdb")
            bound.check_types(paro, duckdb)
        return
    for index, (actual, expected) in enumerate(zip(paro, duckdb)):
        if actual != expected:
            raise ResultContractError(f"same-engine wire schema mismatch at column {index}: {actual}/{expected}")


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
        if type(value) is not bool:
            raise ResultContractError("boolean wire value must be bool")
        return value
    if logical_type.startswith(("int", "uint", "decimal", "numeric")):
        try:
            return validate_exact(value, logical_type)
        except ValueError as error:
            raise ResultContractError(f"{logical_type}: {error}") from error
    if logical_type.startswith("float"):
        if type(value) is not float:
            raise ResultContractError("floating wire value must be float")
        number = value
        if not math.isfinite(number):
            raise ResultContractError("nonfinite floating result requires an explicit contract")
        return 0.0 if number == 0 else number
    if logical_type in {"date", "time", "timetz", "timestamp", "timestamptz"}:
        return value.isoformat() if isinstance(value, (date, datetime, time)) else str(value)
    if logical_type == "bytes":
        return bytes(value).hex()
    return str(value)


def multiset_digest(rows: Sequence[tuple[Any, ...]]) -> str:
    return _counter_digest(Counter(rows))


def sequence_digest(rows):
    digest = hashlib.sha256()
    for row in rows:
        digest.update(evidence_bytes(row))
        digest.update(b"\n")
    return digest.hexdigest()


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
    for row, count in sorted(counter.items(), key=lambda item: evidence_bytes(item[0])):
        digest.update(evidence_bytes(row))
        digest.update(b"\0")
        digest.update(str(count).encode("ascii"))
        digest.update(b"\n")
    return digest.hexdigest()


def parse_order_contract(query: str, schema: Sequence[ColumnContract]) -> tuple[OrderKey, ...]:
    import duckdb
    from bound_result_contract import BoundResult
    with duckdb.connect() as parser:
        bound = BoundResult(query, parser)
        if len(bound.outputs) != len(schema):
            raise ResultContractError("ORDER binding/output arity mismatch")
        return tuple(OrderKey(expr[1] if expr[0] == "column" else -1, desc, nulls, expr)
                     for expr, desc, nulls in bound.bind_order())


def peer_key_values(rows: Sequence[tuple[Any, ...]], keys: Sequence[OrderKey]):
    from bound_result_contract import order_values
    return order_values(rows, [(key.expression or ("column", key.column),
                               key.descending, key.nulls) for key in keys])


def assert_peer_order(
    rows: Sequence[tuple[Any, ...]], keys: Sequence[OrderKey]
) -> str | None:
    if not keys:
        return None
    return sequence_digest(peer_key_values(rows, keys))


def assert_same_order(actual, expected, keys):
    # Hash equality is never a substitute for semantic key-sequence equality.
    if peer_key_values(actual, keys) != peer_key_values(expected, keys):
        raise ResultContractError("ordered peer-key sequences differ")
