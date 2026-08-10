# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

from decimal import Decimal
from pathlib import Path
import unittest

from benchmark.harness.executor import _normalize_value
from benchmark.harness.loader import QueryDef
from benchmark.harness.validator import (
    STRONG_VALIDATE_MODES,
    BenchmarkValidator,
    ordered_rows_digest,
)


class ResultNormalizationTests(unittest.TestCase):
    def test_decimal_normalization_is_exact_and_preserves_scale(self) -> None:
        self.assertEqual(_normalize_value(Decimal("123141078.2283")), "123141078.2283")
        self.assertEqual(_normalize_value(Decimal("37734107.00")), "37734107.00")

    def test_ordered_digest_preserves_order_and_value_boundaries(self) -> None:
        rows = [["ab", "c"], [1, None]]
        self.assertEqual(
            ordered_rows_digest(rows),
            "5140ee4de11a3233a4d8de72ad3cc382470b849eb0e4f88d091ffc428c587de0",
        )
        self.assertNotEqual(ordered_rows_digest(rows), ordered_rows_digest(list(reversed(rows))))
        self.assertIn("ordered_digest", STRONG_VALIDATE_MODES)

    def test_ordered_digest_failure_reports_rows_and_preview(self) -> None:
        query = QueryDef(
            id="q",
            file=Path("q.sql"),
            sql="SELECT 1",
            validate="ordered_digest",
            expected="0" * 64,
        )
        outcome = BenchmarkValidator(lambda: None, 1).validate_query(
            query, [[1, "first"], [2, "second"]]
        )

        self.assertEqual(outcome.status, "FAIL")
        self.assertIn("row_count=2", outcome.detail or "")
        self.assertIn("first_rows=[[1, 'first'], [2, 'second']]", outcome.detail or "")


if __name__ == "__main__":
    unittest.main()
