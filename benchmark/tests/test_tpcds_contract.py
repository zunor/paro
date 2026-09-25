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
from tpcds_compare import (  # noqa: E402
    hierarchical_cold_ratio, normal_cell_evidence, configure_paro, corpus_impact_summary,
)
from bound_result_contract import BoundResult  # noqa: E402
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
    def test_worker_construction_cancellation_closes_unentered_worker(self) -> None:
        from unittest.mock import MagicMock, patch
        from tpcds_compare import DuckDBProcess
        context, parent, child = MagicMock(), MagicMock(), MagicMock()
        context.Pipe.return_value = (parent, child)
        parent.recv.side_effect = KeyboardInterrupt
        with patch("tpcds_compare.multiprocessing.get_context", return_value=context), \
             patch.object(DuckDBProcess, "close") as close:
            with self.assertRaises(KeyboardInterrupt):
                DuckDBProcess(Path("unused.duckdb"), 4, "2GB")
        close.assert_called_once()
        child.close.assert_called_once()

    def test_corpus_impact_preserves_failures_and_ranks_absolute_excess(self) -> None:
        def measured(query, paro, duck):
            return {"query": query, "status": "passed", "warmup_and_steady_state": {
                "paro": {"median_ms": paro}, "duckdb": {"median_ms": duck},
            }}
        result = corpus_impact_summary([
            measured("01", 1000, 500), measured("02", 20, 1), measured("03", 1, 2),
            {"query": "04", "status": "failed", "error": "timeout"},
            measured("05", float("nan"), 1), measured("06", 1, 0),
        ])
        self.assertFalse(result["complete_measured_coverage"])
        self.assertEqual(result["measured_queries"], 3)
        self.assertEqual(result["sum_of_measured_warm_medians_ms"], 1021)
        self.assertEqual([r["query"] for r in result["ranked_by_excess_warm_ms"]], ["01", "02", "03"])
        self.assertEqual([r["query"] for r in result["uncovered"]], ["04", "05", "06"])
        self.assertTrue(result["ranked_by_excess_warm_ms"][1]["execution_diagnosis_recommended"])
        self.assertIsNone(corpus_impact_summary([])["top_five_measured_warm_share"])
        with self.assertRaisesRegex(ValueError, "distinct"):
            corpus_impact_summary([measured("01", 1, 1), measured("01", 2, 1)])

    def test_verifier_is_explicit_without_retired_search_settings(self) -> None:
        from unittest.mock import MagicMock
        for verify, literal in (("on", "true"), ("off", "false")):
            connection = MagicMock()
            args = SimpleNamespace(optimizer_verify=verify,
                                   threads=4, memory_limit="2GB", statement_timeout_seconds=30)
            configure_paro(connection, args)
            statements = [call.args[0].as_string() for call in
                          connection.cursor.return_value.__enter__.return_value.execute.call_args_list]
            self.assertEqual(statements[0], f"SET optimizer_verify = {literal}")
            self.assertFalse(any("optimizer_search_policy" in statement or
                                 "disabled_optimizer_rules" in statement for statement in statements))

    def test_engine_identity_is_not_a_diagnostic_label(self) -> None:
        schema = duckdb_schema([("x", "VARCHAR")])[0]
        BoundResult._check_wire_schema(schema, "duckdb")
        with self.assertRaisesRegex(ResultContractError, "unknown result engine identity"):
            BoundResult._check_wire_schema(schema, "duckdb cold statement")

    def test_normal_cell_retains_cold_and_warm_receipts_once(self) -> None:
        cold = {"status": "Uncovered", "reason": "cold"}
        warm = {"status": "Uncovered", "reason": "warm"}
        source = {"process_blocks": [{"cold_miss_evidence": cold,
                   "paro_receipt_associations": [warm], "paro_ms": [1.5]}],
                  "cold_statement": {"cold_miss_evidence": {"samples": [cold]},
                    "normal_receipt_coverage": {"associations": [warm]}}}
        payload, receipts = normal_cell_evidence(source, 2)
        self.assertEqual(receipts, [cold, warm])
        self.assertEqual(payload["process_blocks"][0]["cold_receipt_index"], 0)
        self.assertEqual(payload["process_blocks"][0]["warm_receipt_indices"], [1])
        self.assertNotIn("cold_miss_evidence", payload["process_blocks"][0])
        self.assertEqual(source["process_blocks"][0]["cold_miss_evidence"], cold)
        with self.assertRaisesRegex(ValueError, "exceed registered"):
            normal_cell_evidence(source, 1)

    def test_failed_cell_preserves_error_and_registered_missing_samples(self) -> None:
        payload, receipts = normal_cell_evidence({"status": "failed", "error": "query failed"}, 4)
        self.assertEqual(payload["error"], "query failed")
        self.assertEqual(len(receipts), 4)
        self.assertTrue(all(item["status"] == "Uncovered" for item in receipts))

    def test_schema_identity_ignores_engine_specific_qualifier_display(self) -> None:
        paro = paro_schema(
            [SimpleNamespace(name="dt.d_year", type_code=20, precision=None, scale=None)]
        )
        duckdb = duckdb_schema([("d_year", "BIGINT")])

        assert_compatible_schema(paro, duckdb, query="SELECT dt.d_year FROM dt")

    def test_derived_label_requires_the_same_parsed_expression(self) -> None:
        a = (ColumnContract("round(x / y, 2)", "float64", "701"),)
        b = (ColumnContract("round((x / y), 2)", "float64", "DOUBLE"),)
        assert_compatible_schema(a, b, query="SELECT round(x/y,2) FROM t")
        with self.assertRaises(ResultContractError):
            assert_compatible_schema(a, b)  # wire identity remains strict
        for query in ["SELECT round(y/x,2) FROM t", 'SELECT round(x/y,2) AS "Identity" FROM t']:
            with self.assertRaises(ResultContractError):
                assert_compatible_schema(a, b, query=query)

    def test_explicit_alias_case_dots_order_and_type_are_strict(self) -> None:
        a = (ColumnContract("A.b", "float64", "701"),)
        assert_compatible_schema(a, (ColumnContract("A.b", "float64", "DOUBLE"),), query='SELECT x AS "A.b" FROM t')
        for name, kind in [("b", "float64"), ("a.b", "float64"), ("A.b", "float32")]:
            with self.assertRaises(ResultContractError):
                assert_compatible_schema(a, (ColumnContract(name, kind, "other"),),
                                         query='SELECT x AS "A.b" FROM t')
        with self.assertRaises(ResultContractError):
            assert_compatible_schema(a, a, query='SELECT x AS "Other" FROM t')

    def test_parentheses_are_not_erased_algebraically(self) -> None:
        a = (ColumnContract("(x+y)*z", "float64", "701"),)
        b = (ColumnContract("x+(y*z)", "float64", "DOUBLE"),)
        with self.assertRaises(ResultContractError):
            assert_compatible_schema(a, b, query="SELECT (x+y)*z FROM t")

    def test_q03_order_contract_uses_result_columns_and_default_null_order(self) -> None:
        schema = (
            ColumnContract("d_year", "int32", "INTEGER"),
            ColumnContract("brand_id", "int32", "INTEGER"),
            ColumnContract("brand", "string", "VARCHAR"),
            ColumnContract("sum_agg", "decimal", "DECIMAL(38,2)"),
        )
        keys = parse_order_contract(
            "SELECT dt.d_year, brand_id, brand, sum_agg FROM dt ORDER BY dt.d_year, sum_agg DESC, brand_id LIMIT 100;",
            schema,
        )

        self.assertEqual([key.column for key in keys], [0, 3, 1])
        self.assertEqual([key.descending for key in keys], [False, True, False])
        self.assertEqual([key.nulls for key in keys], ["last", "first", "last"])

    def test_q06_order_contract_binds_projected_source_expression(self) -> None:
        schema = (
            ColumnContract("state", "string", "VARCHAR"),
            ColumnContract("cnt", "int64", "BIGINT"),
        )

        keys = parse_order_contract(
            """
            SELECT a.ca_state state, count(*) cnt
            FROM customer_address a
            ORDER BY cnt NULLS FIRST, a.ca_state NULLS FIRST
            LIMIT 100
            """,
            schema,
        )

        self.assertEqual([key.column for key in keys], [1, 0])
        self.assertEqual([key.nulls for key in keys], ["first", "first"])

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

    def test_duplicate_peer_equivalent_projection_can_bind_order_key(self) -> None:
        schema = (
            ColumnContract("key", "int32", "INTEGER"),
            ColumnContract("key", "int32", "INTEGER"),
        )

        keys = parse_order_contract("SELECT key, key FROM t ORDER BY key", schema)

        self.assertEqual([key.column for key in keys], [0])

    def test_expression_normalization_preserves_quoted_contents(self) -> None:
        schema = (
            ColumnContract("literal", "string", "VARCHAR"),
            ColumnContract("quoted", "int32", "INTEGER"),
        )

        literal = parse_order_contract(
            "SELECT 'A B' AS literal, \"My Col\" AS quoted FROM t ORDER BY 'A B'",
            schema,
        )
        quoted = parse_order_contract(
            'SELECT \'A B\' AS literal, "My Col" AS quoted FROM t ORDER BY "My Col"',
            schema,
        )

        self.assertEqual([key.column for key in literal], [0])
        self.assertEqual([key.column for key in quoted], [1])

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

    def test_hierarchical_abba_accepts_multiple_rounds_per_process(self) -> None:
        result = hierarchical_abba_ratio(
            [
                {
                    "paro_ms": [9.0, 9.2, 9.1, 9.3, 9.0, 9.2],
                    "duckdb_ms": [10.0, 10.1, 10.2, 10.0, 10.1, 10.2],
                },
                {
                    "paro_ms": [9.5, 9.4, 9.3, 9.5, 9.4, 9.3],
                    "duckdb_ms": [10.2, 10.0, 10.1, 10.2, 10.0, 10.1],
                },
            ],
            bootstrap_samples=100,
        )

        self.assertEqual(result["process_blocks"], 2)
        self.assertEqual(result["samples_per_engine"], 12)

    def test_hierarchical_cold_ratio_uses_one_sample_per_fresh_block(self) -> None:
        result = hierarchical_cold_ratio(
            [
                {"cold_statement_ms": {"paro": 90.0, "duckdb": 100.0}},
                {"cold_statement_ms": {"paro": 110.0, "duckdb": 100.0}},
            ],
            bootstrap_samples=100,
        )

        self.assertEqual(result["process_blocks"], 2)
        self.assertEqual(result["samples_per_engine"], 2)
        self.assertEqual(result["resampling_unit"], "fresh_process_block_only")
        self.assertAlmostEqual(result["ratio"], (0.9 * 1.1) ** 0.5, places=6)

    def test_hierarchical_cold_ratio_rejects_missing_or_nonpositive_samples(self) -> None:
        with self.assertRaises(ValueError):
            hierarchical_cold_ratio([{"cold_statement_ms": {"paro": 90.0}}])
        with self.assertRaises(ValueError):
            hierarchical_cold_ratio(
                [{"cold_statement_ms": {"paro": 0.0, "duckdb": 100.0}}]
            )


if __name__ == "__main__":
    unittest.main()
