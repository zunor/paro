# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

from decimal import Decimal
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest


CORPORA = Path(__file__).resolve().parents[1] / "corpora"
sys.path.insert(0, str(CORPORA))

from benchmark_evidence import (  # noqa: E402
    hierarchical_abba_ratio,
    paired_order_balanced_ratio,
)
from tpcds_result_contract import (  # noqa: E402
    ColumnContract,
    ResultContractError,
    assert_peer_order,
    assert_compatible_schema,
    canonicalize_rows,
    duckdb_schema,
    parse_order_contract,
    paro_schema,
)


class TpcdsResultContractTests(unittest.TestCase):
    def test_schema_identity_ignores_engine_specific_qualifier_display(self) -> None:
        paro = paro_schema(
            [SimpleNamespace(name="dt.d_year", type_code=20, precision=None, scale=None)]
        )
        duckdb = duckdb_schema([("d_year", "BIGINT")])

        assert_compatible_schema(paro, duckdb)

    def test_q03_order_contract_uses_result_columns_and_default_null_order(self) -> None:
        schema = (
            ColumnContract("d_year", "int32", "INTEGER"),
            ColumnContract("brand_id", "int32", "INTEGER"),
            ColumnContract("brand", "string", "VARCHAR"),
            ColumnContract("sum_agg", "decimal", "DECIMAL(38,2)"),
        )
        keys = parse_order_contract(
            "SELECT 1 ORDER BY dt.d_year, sum_agg DESC, brand_id LIMIT 100;",
            schema,
        )

        self.assertEqual([key.column for key in keys], [0, 3, 1])
        self.assertEqual([key.descending for key in keys], [False, True, False])
        self.assertEqual([key.nulls for key in keys], ["last", "first", "last"])

    def test_peer_rows_may_change_order_without_changing_order_digest(self) -> None:
        schema = (
            ColumnContract("key", "int32", "INTEGER"),
            ColumnContract("payload", "string", "VARCHAR"),
        )
        keys = parse_order_contract("SELECT key, payload ORDER BY key", schema)
        left = [(1, "a"), (1, "b"), (2, "c")]
        right = [(1, "b"), (1, "a"), (2, "c")]

        self.assertEqual(assert_peer_order(left, keys), assert_peer_order(right, keys))

    def test_order_validation_is_numeric_and_honors_null_placement(self) -> None:
        schema = (ColumnContract("amount", "decimal", "DECIMAL(18,2)"),)
        rows = canonicalize_rows(
            [(None,), (Decimal("1000.00"),), (Decimal("99.00"),)], schema
        )
        descending = parse_order_contract("SELECT amount ORDER BY amount DESC", schema)
        assert_peer_order(rows, descending)

        with self.assertRaises(ResultContractError):
            assert_peer_order(list(reversed(rows)), descending)

    def test_one_sided_measurement_order_remains_a_valid_paired_estimate(self) -> None:
        result = paired_order_balanced_ratio(
            [9.0, 10.0, 11.0],
            [10.0, 10.0, 10.0],
            [True, True, True],
            bootstrap_samples=100,
        )

        self.assertEqual(result["samples"], {"paro_first": 3, "paro_second": 0})
        self.assertGreater(result["paired_confidence_interval_95"][0], 0)

    def test_schema_rejects_integer_width_and_timestamp_zone_drift(self) -> None:
        with self.assertRaises(ResultContractError):
            assert_compatible_schema(
                (ColumnContract("key", "int32", "23"),),
                (ColumnContract("key", "int64", "BIGINT"),),
            )
        with self.assertRaises(ResultContractError):
            assert_compatible_schema(
                (ColumnContract("ts", "timestamp", "1114"),),
                (ColumnContract("ts", "timestamptz", "TIMESTAMPTZ"),),
            )

    def test_duplicate_result_names_make_order_binding_fail_closed(self) -> None:
        schema = (
            ColumnContract("key", "int32", "INTEGER"),
            ColumnContract("key", "int32", "INTEGER"),
        )
        with self.assertRaises(ResultContractError):
            parse_order_contract("SELECT 1 ORDER BY key", schema)

    def test_hierarchical_abba_resamples_fresh_process_blocks(self) -> None:
        result = hierarchical_abba_ratio(
            [
                {"paro_ms": [9.0, 9.2], "duckdb_ms": [10.0, 10.1]},
                {"paro_ms": [9.5, 9.4], "duckdb_ms": [10.2, 10.0]},
            ],
            bootstrap_samples=100,
        )
        self.assertEqual(result["process_blocks"], 2)
        self.assertEqual(result["samples_per_engine"], 4)
        self.assertGreater(result["hierarchical_confidence_interval_95"][0], 0)


if __name__ == "__main__":
    unittest.main()
