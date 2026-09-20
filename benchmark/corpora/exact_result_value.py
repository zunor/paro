"""Lossless scalar values after validation of each engine's declared type.

An exact rational is a comparison representation, not permission to map two
SQL types. The bound output contract must separately authorize that mapping.
No Decimal context operation, float conversion or rounded string is used.
"""
from dataclasses import dataclass
from decimal import Decimal
from fractions import Fraction
import json
import re


@dataclass(frozen=True, order=True)
class ExactNumber:
    value: Fraction


def exact_number(value):
    if type(value) is int:
        return ExactNumber(Fraction(value))
    if type(value) is Decimal and value.is_finite():
        sign, digits, exponent = value.as_tuple()
        coefficient = 0
        for digit in digits:
            coefficient = coefficient * 10 + digit
        if sign:
            coefficient = -coefficient
        return ExactNumber(Fraction(coefficient * 10**max(exponent, 0),
                                    10**max(-exponent, 0)))
    raise ValueError("exact numeric value requires an integer or finite Decimal, not bool/float")


def validate_exact(value, logical_type):
    integer = re.fullmatch(r"(u?)int(8|16|32|64|128)", logical_type)
    if integer:
        if type(value) is not int:
            raise ValueError("integer wire value must be int, not a coerced value")
        bits = int(integer[2])
        low, high = (0, 2**bits - 1) if integer[1] else (-2**(bits-1), 2**(bits-1)-1)
        if not low <= value <= high:
            raise ValueError("integer value outside declared width")
        return exact_number(value)
    if type(value) is not Decimal:
        raise ValueError("decimal wire value must be Decimal")
    number = exact_number(value)
    fixed = re.fullmatch(r"(?:decimal|numeric)\((\d+),(\d+)\)", logical_type)
    if fixed:
        precision, scale = map(int, fixed.groups())
        if not 0 <= scale <= precision or precision == 0:
            raise ValueError("invalid decimal precision/scale")
        units = number.value * 10**scale
        if units.denominator != 1 or abs(units.numerator) >= 10**precision:
            raise ValueError("decimal value outside declared precision/scale")
    elif logical_type not in {"numeric", "decimal"}:
        raise ValueError("unsupported exact numeric type")
    return number


def evidence_value(value):
    """Canonical evidence encoding; never used instead of semantic equality."""
    if value is None:
        return ["null"]
    if type(value) is bool:
        return ["boolean", value]
    if isinstance(value, ExactNumber) or type(value) in {int, Decimal}:
        number = value if isinstance(value, ExactNumber) else exact_number(value)
        return ["exact", str(number.value.numerator), str(number.value.denominator)]
    if type(value) is float:
        return ["binary-float", value.hex() if value else "0x0.0p+0"]
    if isinstance(value, (tuple, list)):
        return ["row", [evidence_value(v) for v in value]]
    if type(value) is str:
        return ["text", value]
    raise ValueError("unsupported evidence scalar: " + type(value).__name__)


def evidence_bytes(value):
    return json.dumps(evidence_value(value), ensure_ascii=False,
                      separators=(",", ":")).encode("utf-8")
