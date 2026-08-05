# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

from decimal import Decimal
import unittest

from benchmark.harness.executor import _normalize_value


class ResultNormalizationTests(unittest.TestCase):
    def test_decimal_normalization_is_exact_and_preserves_scale(self) -> None:
        self.assertEqual(_normalize_value(Decimal("123141078.2283")), "123141078.2283")
        self.assertEqual(_normalize_value(Decimal("37734107.00")), "37734107.00")


if __name__ == "__main__":
    unittest.main()
