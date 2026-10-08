# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

from decimal import Decimal, localcontext
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "corpora"))
from exact_result_value import evidence_bytes, exact_number, validate_exact
from tpcds_result_contract import (
    ColumnContract, OrderKey, ResultContractError, assert_peer_order,
    assert_same_multiset, canonicalize_rows, multiset_digest,
)


class ExactResultValueTests(unittest.TestCase):
    def test_38_digit_adjacent_values_are_distinct_under_any_context(self):
        a, b = Decimal("12345678901234567890123456789012345678"), Decimal("12345678901234567890123456789012345679")
        for precision in [2, 28, 60]:
            with localcontext() as context:
                context.prec = precision
                rows = canonicalize_rows([(a,), (b,)], [ColumnContract("x", "decimal(38,0)", "1700")])
                self.assertNotEqual(rows[0], rows[1])
                self.assertNotEqual(multiset_digest(rows[:1]), multiset_digest(rows[1:]))
                with self.assertRaises(ResultContractError):
                    assert_same_multiset(rows[:1], rows[1:])

    def test_scale_exponent_zero_and_fraction_are_exact(self):
        for values in [(1000, Decimal("1E+3"), Decimal("1000.000")),
                       (0, Decimal("-0E-100"), Decimal("0E+100"))]:
            self.assertEqual(len({exact_number(v) for v in values}), 1)
            self.assertEqual(len({evidence_bytes(v) for v in values}), 1)
        self.assertLess(exact_number(Decimal("0.1")), exact_number(Decimal("0.10000000000000000000000000001")))

    def test_width_scale_truncation_and_bool_coercion_are_rejected(self):
        for kind, bits in [("int8", 8), ("int64", 64), ("int128", 128)]:
            validate_exact(-2**(bits-1), kind)
            validate_exact(2**(bits-1)-1, kind)
            for value in [-2**(bits-1)-1, 2**(bits-1), True, 1.2, Decimal("1")]:
                with self.assertRaises(ValueError):
                    validate_exact(value, kind)
        for value in [Decimal("1000"), Decimal("0.001"), Decimal("NaN"), Decimal("Infinity"), 1, True]:
            with self.assertRaises(ValueError):
                validate_exact(value, "decimal(5,2)")
        validate_exact(Decimal("999.99"), "decimal(5,2)")
        with self.assertRaises(ValueError):
            validate_exact(-1, "uint32")
        with self.assertRaises(ResultContractError):
            canonicalize_rows([(1,)], [ColumnContract("b", "boolean", "16")])
        self.assertNotEqual(exact_number(1), True)

    def test_authorized_exact_mapping_has_same_semantics_and_evidence(self):
        a = canonicalize_rows([(Decimal("-2.0"),), (Decimal("0.00"),), (Decimal("99"),)], [ColumnContract("x", "numeric", "1700")])
        b = canonicalize_rows([(-2,), (0,), (99,)], [ColumnContract("x", "int128", "HUGEINT")])
        assert_same_multiset(a, b)
        self.assertEqual(multiset_digest(a), multiset_digest(b))
        keys = [OrderKey(0, False, "last")]
        self.assertEqual(assert_peer_order(a, keys), assert_peer_order(b, keys))
        with self.assertRaises(ResultContractError):
            assert_peer_order(list(reversed(a)), keys)
