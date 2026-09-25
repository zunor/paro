# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Typed ORDER-only binary64 casts. Never change exact output/bag identity.

Paro: decimal_casts::decimal_to_float_cast (integer coefficient / 10f64.powi).
DuckDB 1.5.5: TryCastDecimalToFloatingPoint + CastBigintToFloating.
Scales above 22 are deliberately outside the shared certified power domain.
"""
import math
import re
from exact_result_value import ExactNumber, ResultContractError


def finite(value):
    if not math.isfinite(value):
        raise ResultContractError("non-finite ORDER arithmetic is not certified")
    return value


def duck_hugeint(value):
    upper, lower = divmod(value, 2**64)
    if upper == -1:
        return -float(2**64-1-lower)-1.0
    return float(lower) + float(upper)*float(2**64)


def to_double(value, source, engine):
    if value is None:
        return None
    if source == "float64":
        if type(value) is not float:
            raise ResultContractError("DOUBLE ORDER input is not binary64")
        return finite(value)
    if not isinstance(value, ExactNumber):
        raise ResultContractError("exact ORDER cast input required")
    match = re.fullmatch(r"decimal\((\d+),(\d+)\)", source)
    if match:
        precision, scale = map(int, match.groups())
        if not 0 <= scale <= min(precision, 22) or precision > 38:
            raise ValueError("uncertified decimal ORDER cast scale/precision")
    elif re.fullmatch(r"int(8|16|32|64|128)", source):
        precision, scale = (38 if source == "int128" else 18), 0
    else:
        raise ValueError("unbound exact ORDER cast type: " + source)
    scaled = value.value * 10**scale
    if scaled.denominator != 1:
        raise ResultContractError("ORDER input violates declared decimal scale")
    coefficient = scaled.numerator
    if match and abs(coefficient) >= 10**precision:
        raise ResultContractError("ORDER coefficient overflow")
    if engine not in {"paro", "duckdb"}:
        raise ValueError("ORDER cast requires an engine contract")
    denominator = float(10**scale)  # exact for 0..22, no platform pow ambiguity
    cast_integer = duck_hugeint if engine == "duckdb" and precision > 18 else float
    if engine == "paro" or abs(coefficient) <= 2**53 or scale == 0:
        return finite(cast_integer(coefficient) / denominator)
    quotient = abs(coefficient) // 10**scale
    if coefficient < 0:
        quotient = -quotient
    remainder = coefficient - quotient * 10**scale
    return finite(cast_integer(quotient) + cast_integer(remainder) / denominator)


def bind_numeric_order(expr, schema, engine):
    """Annotate the already-bound fragment, without binding SQL again."""
    op = expr[0]
    if op == "column":
        return expr, schema[expr[1]].logical_type
    if op == "literal":
        return expr, "int128" if isinstance(expr[1], ExactNumber) else "other"
    if op == "case":
        checks = [(bind_numeric_order(p, schema, engine)[0], bind_numeric_order(v, schema, engine))
                  for p, v in expr[1]]
        otherwise, kind = bind_numeric_order(expr[2], schema, engine)
        kinds = {t for _, (_, t) in checks if t != "other"}
        if kind != "other":
            kinds.add(kind)
        if len(kinds) > 1:
            raise ValueError("mixed CASE ORDER result needs explicit coercion proof")
        return ("case", tuple((p,v) for p,(v,_) in checks), otherwise), next(iter(kinds), "other")
    a, at = bind_numeric_order(expr[1], schema, engine)
    b, bt = bind_numeric_order(expr[2], schema, engine)
    if op in {"+", "-", "*"} and "float64" in {at, bt}:
        def cast(child, kind):
            if kind == "float64":
                return child
            if not re.fullmatch(r"decimal\((\d+),(\d+)\)|int(8|16|32|64|128)", kind):
                raise ValueError("ORDER arithmetic lacks an exact-to-DOUBLE cast contract")
            return ("to_double", child, kind, engine)
        return (op, cast(a, at), cast(b, bt)), "float64"
    return (op, a, b), "boolean" if op.startswith("COMPARE_") else "unresolved_exact_expression"
