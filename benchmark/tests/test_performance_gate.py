# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import replace
from datetime import date
import json
import os
from pathlib import Path
from types import SimpleNamespace
import sys
import tempfile
import unittest
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from harness.baseline_index import QueryKey, index_queries, params_key  # noqa: E402
from harness.cli import common as gate_common  # noqa: E402
from harness.cli import gate_check  # noqa: E402
from harness.performance_gate import (  # noqa: E402
    BaselineError,
    BaselineMeasurement,
    GateEnforcement,
    GateEntryKind,
    GateFingerprint,
    GateStatus,
    PolicyError,
    aggregate_measurement_runs,
    build_baseline_payload,
    evaluate_gate,
    is_missing_fingerprint_value,
    load_baseline,
    load_policy,
    load_staging_queries,
    project_measurement_payload,
    validate_baseline_for_check,
    validate_bless_fingerprint,
    validate_existing_baseline_for_bless,
)
from harness.performance_gate.entry import GateEntry  # noqa: E402
from harness.performance_gate.metric_accessor import metric_value  # noqa: E402
from harness.performance_gate.outcome import GateOutcome  # noqa: E402
from harness.performance_gate.quorum_statistics import (  # noqa: E402
    estimate_effect_power,
    holm_step_down,
    mann_whitney_one_sided,
    rolling_p95_delta,
)
from harness.sources import SourceContext, SourceRegistry, default_registry  # noqa: E402
from harness.sources.divan_bench import DivanBenchSource, normalize_divan_payload  # noqa: E402


POLICY_PATH = Path(__file__).resolve().parents[1] / "policies" / "operator-runtime.toml"
DIVAN_POLICY_PATH = Path(__file__).resolve().parents[1] / "policies" / "divan-dispatch.toml"


def test_policy(**overrides):
    return replace(load_policy(POLICY_PATH), **overrides)


def payload(*queries: dict, params: dict | None = None) -> dict:
    return {
        "workloads": [
            {
                "name": "w",
                "params": {} if params is None else params,
                "queries": list(queries),
            }
        ]
    }


def query(
    query_id: str,
    *,
    stats: dict | None = None,
    p50: float | None = 10.0,
    p99: float | None = None,
    throughput: float | None = None,
    rss_peak_kb: float | None = None,
    audit: dict | None = None,
    samples: int = 20,
    error: str | None = None,
) -> dict:
    item = {"id": query_id, "error": error, "samples_ms": [10.0] * samples}
    if stats is not None:
        item["stats"] = stats
    elif p50 is not None:
        resolved_p99 = p50 if p99 is None else p99
        resolved_throughput = (1000.0 / p50) if throughput is None else throughput
        item["stats"] = {
            "p50": p50,
            "p99": resolved_p99,
            "throughput_per_second": resolved_throughput,
        }
    if rss_peak_kb is not None:
        item["rss"] = {"peak_kb": rss_peak_kb}
    if audit is not None:
        item["audit"] = audit
    return item


def baseline_payload(*queries: dict) -> dict:
    return payload(*queries)


def calibration_payload(policy, *values: float) -> dict:
    return {
        "observations": [
            {
                "sources": [
                    {
                        "name": policy.sources[0].name,
                        "payload": payload(query("q", p50=value)),
                    }
                ]
            }
            for value in values
        ]
    }


class PerformanceGateTests(unittest.TestCase):
    def test_missing_queries_are_bidirectional_failures(self) -> None:
        baseline = baseline_payload(query("old"))
        current = payload(query("new"))

        outcome = evaluate_gate(
            policy=test_policy(metrics=("p50",)),
            current_payload=current,
            baseline_payload=baseline,
        )

        self.assertTrue(outcome.failed)
        self.assertEqual(outcome.enforcement, GateEnforcement.SHADOW)
        self.assertFalse(outcome.blocking_failed)
        self.assertEqual(
            [(entry.kind.value, entry.query_id, entry.status) for entry in outcome.entries],
            [
                ("MissingBaselineQuery", "new", GateStatus.REGRESS),
                ("MissingCurrentQuery", "old", GateStatus.REGRESS),
            ],
        )

        hard = evaluate_gate(
            policy=replace(test_policy(metrics=("p50",)), enforcement=GateEnforcement.HARD),
            current_payload=current,
            baseline_payload=baseline,
        )
        self.assertTrue(hard.blocking_failed)

    def test_staging_query_missing_baseline_is_diagnostic_until_expiry(self) -> None:
        policy = test_policy(metrics=("p50",))
        current = payload(query("new"))
        baseline = baseline_payload()
        staging = {
            ("w", "new"): SimpleNamespace(
                workload="w",
                query_id="new",
                staging_until=date(2026, 5, 25),
                active_on=lambda today: today <= date(2026, 5, 25),
            )
        }

        active = evaluate_gate(
            policy=policy,
            current_payload=current,
            baseline_payload=baseline,
            staging_queries=staging,
            today=date(2026, 5, 20),
        )
        self.assertFalse(active.failed)
        self.assertEqual(active.entries[0].kind, GateEntryKind.STAGING_QUERY)
        self.assertEqual(active.entries[0].status, GateStatus.UNMEASURABLE)

        expired = evaluate_gate(
            policy=policy,
            current_payload=current,
            baseline_payload=baseline,
            staging_queries=staging,
            today=date(2026, 5, 26),
        )
        self.assertTrue(expired.failed)
        self.assertEqual(expired.entries[0].kind, GateEntryKind.MISSING_BASELINE_QUERY)
        self.assertIn("expired", expired.entries[0].detail)

    def test_missing_metrics_are_bidirectional_failures(self) -> None:
        baseline = baseline_payload(query("q", stats={"p50": 10.0}))
        current = payload(query("q", stats={"p99": 10.0}))

        baseline_missing = evaluate_gate(
            policy=test_policy(metrics=("p99",)),
            current_payload=current,
            baseline_payload=baseline,
        )
        self.assertEqual(baseline_missing.entries[0].kind.value, "MissingBaselineMetric")
        self.assertEqual(baseline_missing.entries[0].status, GateStatus.REGRESS)

        current_missing = evaluate_gate(
            policy=test_policy(metrics=("p99",)),
            current_payload=payload(query("q", stats={})),
            baseline_payload=baseline_payload(query("q", stats={"p99": 10.0})),
        )
        self.assertEqual(current_missing.entries[0].kind.value, "MissingCurrentMetric")
        self.assertEqual(current_missing.entries[0].status, GateStatus.REGRESS)

    def test_query_execution_error_is_gate_failure(self) -> None:
        baseline = baseline_payload(query("q"))
        current = payload(query("q", error="boom"))

        outcome = evaluate_gate(
            policy=test_policy(metrics=("p50",)),
            current_payload=current,
            baseline_payload=baseline,
        )

        self.assertTrue(outcome.failed)
        self.assertEqual(outcome.entries[0].kind.value, "QueryExecutionError")

    def test_policy_file_is_required_and_validated(self) -> None:
        policy = load_policy(POLICY_PATH)
        self.assertEqual(policy.name, "operator-runtime")
        self.assertEqual(policy.metrics, ("p50", "p99", "throughput_per_second", "rss_peak_kb"))
        self.assertEqual(policy.sources[0].name, "operator-runtime-sql")
        self.assertEqual(len(policy.sha256), 64)
        self.assertEqual(policy.statistics.model, "mann_whitney_rolling_p95_holm")
        self.assertEqual(policy.calibration.noise_floor_formula, "directional_rolling_p95_delta")
        self.assertEqual(policy.coverage.new_query_staging_days, 14)

        divan = load_policy(DIVAN_POLICY_PATH)
        self.assertEqual(divan.name, "divan-dispatch")
        self.assertEqual(divan.metrics, ("p50", "p99"))
        self.assertEqual(divan.sources[0].type, "divan_bench")
        self.assertEqual(divan.sources[0].measurement_class, "rust_micro")
        self.assertEqual(divan.throughput_min_ratio, 0.85)
        self.assertEqual(divan.rss_noise_floor_kb, 0.0)

        with tempfile.TemporaryDirectory() as tmp:
            bad_policy = Path(tmp) / "bad.toml"
            bad_policy.write_text('name = "operator-runtime"\n', encoding="utf-8")
            with self.assertRaises(PolicyError):
                load_policy(bad_policy)

    def test_compared_metric_statuses_cover_regress_improve_and_noise(self) -> None:
        policy = test_policy(
            metrics=("p50",),
            latency_regression_percent=15.0,
            latency_noise_floor_ms=0.0,
            short_query_latency_noise_floor_ms=0.0,
        )
        baseline = baseline_payload(query("q", p50=10.0))

        regress = evaluate_gate(
            policy=policy,
            current_payload=payload(query("q", p50=12.0)),
            baseline_payload=baseline,
        )
        self.assertEqual(regress.entries[0].status, GateStatus.REGRESS)

        improve = evaluate_gate(
            policy=policy,
            current_payload=payload(query("q", p50=8.0)),
            baseline_payload=baseline,
        )
        self.assertEqual(improve.entries[0].status, GateStatus.IMPROVE)

        noisy = evaluate_gate(
            policy=test_policy(metrics=("p50",), latency_noise_floor_ms=1.0),
            current_payload=payload(query("q", p50=10.5)),
            baseline_payload=baseline_payload(query("q", p50=10.0)),
        )
        self.assertFalse(noisy.failed)
        self.assertEqual(noisy.entries[0].status, GateStatus.OK)

    def test_short_query_tail_and_throughput_noise_are_filtered(self) -> None:
        tail = evaluate_gate(
            policy=test_policy(
                metrics=("p99",),
                latency_regression_percent=15.0,
                p99_latency_noise_floor_ms=0.0,
                tail_sample_min_count=20,
            ),
            current_payload=payload(query("q", p50=10.0, p99=25.0, samples=2)),
            baseline_payload=baseline_payload(query("q", p50=10.0, p99=10.0)),
        )
        self.assertEqual(tail.entries[0].status, GateStatus.OK)

        throughput = evaluate_gate(
            policy=test_policy(
                metrics=("throughput_per_second",),
                throughput_min_ratio=0.85,
                short_query_latency_noise_floor_ms=2.5,
            ),
            current_payload=payload(query("q", p50=10.0, throughput=83.0)),
            baseline_payload=baseline_payload(query("q", p50=10.0, throughput=100.0)),
        )
        self.assertEqual(throughput.entries[0].status, GateStatus.OK)

    def test_statistical_gate_consumes_calibration_and_holm(self) -> None:
        policy = test_policy(metrics=("p50",), latency_regression_percent=5.0)
        baseline = baseline_payload(query("q", p50=10.0))
        calibration = calibration_payload(policy, *([10.0] * 29 + [10.2]))

        outcome = evaluate_gate(
            policy=policy,
            current_payload=payload(query("q", p50=11.6)),
            baseline_payload=baseline,
            calibration_payload=calibration,
            source_name=policy.sources[0].name,
            confirmation_payloads=(payload(query("q", p50=11.7)),),
            statistical_phase="confirmed",
        )

        compared = next(entry for entry in outcome.entries if entry.kind == GateEntryKind.COMPARED_METRIC)
        self.assertEqual(compared.status, GateStatus.REGRESS)
        self.assertIsNotNone(compared.p_value)
        self.assertIsNotNone(compared.holm_alpha)
        self.assertEqual(compared.calibration_samples, 30)
        self.assertGreaterEqual(compared.noise_floor_abs or 0.0, 0.0)

    def test_first_phase_uses_threshold_and_noise_floor_before_retry(self) -> None:
        policy = test_policy(metrics=("p50",), latency_regression_percent=5.0)
        baseline = baseline_payload(query("q", p50=10.0))
        calibration = calibration_payload(policy, *([10.0] * 30))

        outcome = evaluate_gate(
            policy=policy,
            current_payload=payload(query("q", p50=11.6)),
            baseline_payload=baseline,
            calibration_payload=calibration,
            source_name=policy.sources[0].name,
        )

        compared = next(entry for entry in outcome.entries if entry.kind == GateEntryKind.COMPARED_METRIC)
        self.assertEqual(compared.status, GateStatus.REGRESS)
        self.assertIsNone(compared.p_value)
        self.assertIn("quorum retry", compared.detail)

    def test_statistical_gate_filters_rolling_p95_noise(self) -> None:
        policy = test_policy(metrics=("p50",), latency_regression_percent=5.0)
        baseline = baseline_payload(query("q", p50=10.0))
        calibration = calibration_payload(policy, *([10.0] * 28 + [11.5, 11.6]))

        outcome = evaluate_gate(
            policy=policy,
            current_payload=payload(query("q", p50=11.0)),
            baseline_payload=baseline,
            calibration_payload=calibration,
            source_name=policy.sources[0].name,
        )

        self.assertFalse(outcome.failed)
        self.assertEqual(outcome.entries[0].status, GateStatus.OK)
        self.assertIn("rolling P95", outcome.entries[0].detail)

    def test_confirmed_quorum_requires_retry_sample_to_still_regress(self) -> None:
        policy = test_policy(metrics=("p50",), latency_regression_percent=5.0)
        baseline = baseline_payload(query("q", p50=10.0))
        calibration = calibration_payload(policy, *([10.0] * 30))

        outcome = evaluate_gate(
            policy=policy,
            current_payload=payload(query("q", p50=10.1)),
            baseline_payload=baseline,
            calibration_payload=calibration,
            source_name=policy.sources[0].name,
            confirmation_payloads=(payload(query("q", p50=11.7)),),
            statistical_phase="confirmed",
        )

        self.assertFalse(outcome.failed)
        self.assertEqual(outcome.entries[0].status, GateStatus.OK)

    def test_power_deficit_is_reported_for_underpowered_candidate(self) -> None:
        policy = test_policy(metrics=("p50",), latency_regression_percent=5.0)
        baseline = baseline_payload(query("q", p50=100.0))
        calibration = calibration_payload(policy, *([70.0, 130.0] * 15))

        outcome = evaluate_gate(
            policy=policy,
            current_payload=payload(query("q", p50=170.0)),
            baseline_payload=baseline,
            calibration_payload=calibration,
            source_name=policy.sources[0].name,
            confirmation_payloads=(payload(query("q", p50=171.0)),),
            statistical_phase="confirmed",
        )

        power = next(entry for entry in outcome.entries if entry.kind == GateEntryKind.POWER_DEFICIT)
        self.assertEqual(power.status, GateStatus.UNMEASURABLE)
        self.assertLess(power.statistical_power or 1.0, policy.statistics.true_positive_target)

    def test_rust_micro_calibration_minimum_comes_from_policy(self) -> None:
        policy = replace(load_policy(DIVAN_POLICY_PATH), metrics=("p50",))
        baseline = baseline_payload(query("q", p50=10.0))
        calibration = calibration_payload(policy, *([10.0] * 49))

        outcome = evaluate_gate(
            policy=policy,
            current_payload=payload(query("q", p50=11.0)),
            baseline_payload=baseline,
            calibration_payload=calibration,
            source_name=policy.sources[0].name,
        )

        compared = next(entry for entry in outcome.entries if entry.kind == GateEntryKind.COMPARED_METRIC)
        self.assertEqual(compared.status, GateStatus.UNMEASURABLE)
        self.assertEqual(compared.calibration_samples, 49)
        self.assertIn("49/50", compared.detail or "")

    def test_gate_command_quorum_retry_keeps_only_failed_queries(self) -> None:
        source = SimpleNamespace(name="operator-runtime-sql")
        failed_key = QueryKey("w", "q1", "{}")
        ok_key = QueryKey("w", "q2", "{}")
        first = GateOutcome(
            gate="operator-runtime",
            enforcement=GateEnforcement.SHADOW,
            entries=(
                GateEntry(
                    kind=GateEntryKind.COMPARED_METRIC,
                    key=failed_key,
                    metric="p50",
                    status=GateStatus.REGRESS,
                ),
                GateEntry(
                    kind=GateEntryKind.COMPARED_METRIC,
                    key=ok_key,
                    metric="p50",
                    status=GateStatus.OK,
                ),
            ),
        )

        retry = gate_check.retry_keys_by_source_for([(SimpleNamespace(source=source), first)])

        self.assertEqual(retry, {"operator-runtime-sql": frozenset({failed_key})})

    def test_gate_command_shadow_missing_auto_baseline_is_unmeasurable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            args = SimpleNamespace(gate="operator-runtime")
            missing = root / "baselines" / "operator-runtime" / "linux-amd64.json"

            result = gate_check.report_missing_auto_baseline(
                args,
                root_dir=root,
                policy=test_policy(enforcement=GateEnforcement.SHADOW),
                baseline_path=missing,
            )

            self.assertEqual(result, 0)
            report = json.loads((root / "report" / "gate.json").read_text(encoding="utf-8"))
            entry = report["outcomes"][0]["entries"][0]
            self.assertEqual(entry["kind"], "InvalidBaseline")
            self.assertEqual(entry["status"], "UNMEASURABLE")
            self.assertIn("linux-amd64.json", entry["detail"])

    def test_gate_command_hard_missing_auto_baseline_blocks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            args = SimpleNamespace(gate="operator-runtime")
            missing = root / "baselines" / "operator-runtime" / "linux-amd64.json"

            result = gate_check.report_missing_auto_baseline(
                args,
                root_dir=root,
                policy=test_policy(enforcement=GateEnforcement.HARD),
                baseline_path=missing,
            )

            self.assertEqual(result, 1)

    def test_source_family_alpha_is_split_across_sources(self) -> None:
        policy = test_policy()

        adjusted = gate_common.policy_for_source_family(policy, 2)

        self.assertEqual(adjusted.statistics.family_alpha_first_run, policy.statistics.family_alpha_first_run / 2)
        self.assertEqual(adjusted.statistics.family_alpha_confirmed, policy.statistics.family_alpha_confirmed / 2)

    def test_mann_whitney_matches_scipy_when_available(self) -> None:
        try:
            from scipy.stats import mannwhitneyu  # type: ignore
        except Exception:
            self.skipTest("scipy is not installed")
        current = [12.0, 12.2]
        calibration = [10.0, 10.1, 10.2, 10.3, 10.4]
        expected = float(mannwhitneyu(current, calibration, alternative="greater", method="auto").pvalue)

        actual = mann_whitney_one_sided(
            metric="p50",
            current_values=current,
            calibration_values=calibration,
        )

        self.assertAlmostEqual(actual, expected, places=12)

    def test_holm_step_down_and_null_false_positive_budget(self) -> None:
        decisions = holm_step_down({"a": 0.001, "b": 0.03, "c": 0.04}, 0.05)
        self.assertTrue(decisions["a"].rejected)
        self.assertFalse(decisions["b"].rejected)
        self.assertFalse(decisions["c"].rejected)
        tied = holm_step_down({2: 0.01, 10: 0.01}, 0.05)
        self.assertLessEqual(tied[2].alpha, tied[10].alpha)

        import random

        rng = random.Random(0)
        false_positives = 0
        for _ in range(100):
            p_values = {index: rng.random() for index in range(80)}
            if any(decision.rejected for decision in holm_step_down(p_values, 0.01).values()):
                false_positives += 1
        self.assertLessEqual(false_positives, 2)

    def test_power_estimate_is_conservative_for_low_current_sample_count(self) -> None:
        calibration = [90.0, 95.0, 100.0, 105.0, 110.0] * 6

        low = estimate_effect_power(
            metric="p50",
            baseline=100.0,
            calibration_values=calibration,
            current_sample_count=2,
            alpha=0.01,
        )
        higher = estimate_effect_power(
            metric="p50",
            baseline=100.0,
            calibration_values=calibration,
            current_sample_count=8,
            alpha=0.01,
        )

        self.assertLess(low, 0.95)
        self.assertGreater(higher, low)

    def test_rolling_p95_delta_is_directional(self) -> None:
        latency_abs, latency_pct = rolling_p95_delta("p50", 10.0, [9.0, 10.1, 10.2, 11.0])
        throughput_abs, throughput_pct = rolling_p95_delta("throughput_per_second", 100.0, [101.0, 99.0, 80.0])

        self.assertEqual(latency_abs, 1.0)
        self.assertEqual(latency_pct, 10.0)
        self.assertEqual(throughput_abs, 20.0)
        self.assertEqual(throughput_pct, 20.0)

    def test_absolute_metric_bounds_fail(self) -> None:
        outcome = evaluate_gate(
            policy=test_policy(
                metrics=("rss_peak_kb",),
                resource_regression_percent=1000.0,
                rss_noise_floor_kb=0.0,
                absolute_max={"rss_peak_kb": 100.0},
            ),
            current_payload=payload(query("q", rss_peak_kb=150.0)),
            baseline_payload=baseline_payload(query("q", rss_peak_kb=50.0)),
        )
        self.assertEqual(outcome.entries[0].status, GateStatus.REGRESS)

    def test_query_specific_absolute_metric_bounds_override_global(self) -> None:
        outcome = evaluate_gate(
            policy=test_policy(
                metrics=("p99",),
                latency_regression_percent=1000.0,
                absolute_max={"p99": 1000.0, "q.p99": 90.0},
            ),
            current_payload=payload(query("q", p50=10.0, p99=95.0)),
            baseline_payload=baseline_payload(query("q", p50=10.0, p99=50.0)),
        )
        self.assertEqual(outcome.entries[0].status, GateStatus.REGRESS)
        self.assertIn("absolute max 90", outcome.entries[0].detail or "")

    def test_workload_specific_absolute_metric_bounds_override_query_bound(self) -> None:
        outcome = evaluate_gate(
            policy=test_policy(
                metrics=("throughput_per_second",),
                throughput_min_ratio=0.1,
                absolute_min={
                    "q.throughput_per_second": 1.0,
                    "w.q.throughput_per_second": 20.0,
                },
            ),
            current_payload=payload(query("q", p50=10.0, throughput=15.0)),
            baseline_payload=baseline_payload(query("q", p50=10.0, throughput=10.0)),
        )
        self.assertEqual(outcome.entries[0].status, GateStatus.REGRESS)
        self.assertIn("absolute min 20", outcome.entries[0].detail or "")

    def test_audit_metric_absolute_bound_is_supported(self) -> None:
        outcome = evaluate_gate(
            policy=test_policy(
                metrics=("audit_manifest_publish_bytes",),
                resource_regression_percent=1000.0,
                absolute_max={"audit_manifest_publish_bytes": 1024.0},
            ),
            current_payload=payload(
                query("q", audit={"manifest_publish_bytes": 2048.0})
            ),
            baseline_payload=baseline_payload(
                query("q", audit={"manifest_publish_bytes": 512.0})
            ),
        )
        self.assertEqual(outcome.entries[0].status, GateStatus.REGRESS)
        self.assertIn("absolute max 1024", outcome.entries[0].detail or "")

    def test_zero_baseline_audit_metric_is_valid_when_current_stays_zero(self) -> None:
        outcome = evaluate_gate(
            policy=test_policy(metrics=("audit_search_layer_varlen_fallback_seek_count",)),
            current_payload=payload(
                query("q", audit={"search_layer_varlen_fallback_seek_count": 0.0})
            ),
            baseline_payload=baseline_payload(
                query("q", audit={"search_layer_varlen_fallback_seek_count": 0.0})
            ),
        )

        self.assertEqual(outcome.entries[0].kind, GateEntryKind.COMPARED_METRIC)
        self.assertEqual(outcome.entries[0].status, GateStatus.OK)
        self.assertEqual(outcome.entries[0].change_percent, 0.0)

    def test_zero_baseline_audit_metric_regresses_when_current_becomes_positive(self) -> None:
        outcome = evaluate_gate(
            policy=test_policy(metrics=("audit_search_layer_varlen_fallback_seek_count",)),
            current_payload=payload(
                query("q", audit={"search_layer_varlen_fallback_seek_count": 1.0})
            ),
            baseline_payload=baseline_payload(
                query("q", audit={"search_layer_varlen_fallback_seek_count": 0.0})
            ),
        )

        self.assertEqual(outcome.entries[0].kind, GateEntryKind.COMPARED_METRIC)
        self.assertEqual(outcome.entries[0].status, GateStatus.REGRESS)
        self.assertIn("zero baseline", outcome.entries[0].detail or "")

    def test_index_queries_uses_stable_param_key(self) -> None:
        indexed = index_queries(payload(query("q"), params={"b": 2, "a": 1}))

        self.assertIn(
            QueryKey(workload="w", query_id="q", params_key='{"a": 1, "b": 2}'),
            indexed,
        )
        self.assertEqual(params_key(None), "{}")

    def test_policy_loader_applies_metric_bounds(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            policy_path = Path(tmp) / "operator-runtime.toml"
            policy_path.write_text(
                POLICY_PATH.read_text(encoding="utf-8").replace(
                    "[absolute_min]\n",
                    "[absolute_min]\nthroughput_per_second = 10.0\n",
                ),
                encoding="utf-8",
            )
            policy = load_policy(policy_path)

        self.assertEqual(policy.absolute_min, {"throughput_per_second": 10.0})

    def test_policy_loader_separates_per_run_sample_count_from_tail_noise(self) -> None:
        policy = load_policy(DIVAN_POLICY_PATH)

        self.assertEqual(policy.tail_sample_min_count, 50)
        self.assertEqual(policy.calibration.per_run_sample_count_rust_micro, 50)

        policy = replace(
            policy,
            calibration=replace(policy.calibration, per_run_sample_count_rust_micro=7),
            tail_sample_min_count=99,
        )

        self.assertEqual(gate_common.minimum_sample_count(policy, policy.sources[0].name), 7)

    def test_baseline_schema_carries_policy_and_source_payload(self) -> None:
        policy = test_policy()
        measurement = SimpleNamespace(
            source=policy.sources[0],
            payload=payload(query("q")),
        )
        fingerprint = GateFingerprint(
            system={"os": "Darwin", "arch": "arm64"},
            build={"cargo_profile": "unknown"},
            runtime={"data_dir_mode": "unknown"},
            audit={"runner_run_id": "test"},
        )
        baseline_json = build_baseline_payload(
            gate=policy.name,
            platform_key="macos-arm64",
            policy=policy,
            fingerprint=fingerprint,
            measurements=[measurement],
        )

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "baseline.json"
            path.write_text(json.dumps(baseline_json), encoding="utf-8")
            baseline = load_baseline(path)

        self.assertEqual(baseline.schema_version, 2)
        self.assertEqual(baseline.policy_sha256, policy.sha256)
        self.assertIn("operator-runtime-sql", baseline.payload_by_source)

    def test_baseline_projection_keeps_only_gate_fields(self) -> None:
        full = payload(query("q", p50=10.0, rss_peak_kb=42.0))
        full["workloads"][0]["queries"][0]["validation"] = {"result": "PASS"}
        full["workloads"][0]["queries"][0]["explain_profile"] = {"raw_json": "{}"}
        full["workloads"][0]["queries"][0]["memory_tags"] = {"before": []}

        projected = project_measurement_payload(full)
        projected_query = projected["workloads"][0]["queries"][0]

        self.assertEqual(projected_query["samples_count"], 20)
        self.assertNotIn("samples_ms", projected_query)
        self.assertNotIn("validation", projected_query)
        self.assertNotIn("explain_profile", projected_query)
        self.assertNotIn("memory_tags", projected_query)

    def test_bless_aggregation_uses_median_payload(self) -> None:
        policy = test_policy(metrics=("p50",))
        runs = [
            [SimpleNamespace(source=policy.sources[0], payload=payload(query("q", p50=30.0, samples=10)))],
            [SimpleNamespace(source=policy.sources[0], payload=payload(query("q", p50=10.0, samples=20)))],
            [SimpleNamespace(source=policy.sources[0], payload=payload(query("q", p50=20.0, samples=30)))],
        ]

        aggregated = aggregate_measurement_runs(runs)
        query_payload = aggregated[0].payload["workloads"][0]["queries"][0]

        self.assertIsInstance(aggregated[0], BaselineMeasurement)
        self.assertEqual(query_payload["stats"]["p50"], 20.0)
        self.assertEqual(query_payload["samples_count"], 20)

    def test_bless_aggregation_keeps_param_variants_separate(self) -> None:
        policy = test_policy(metrics=("p50",))
        runs = [
            [
                SimpleNamespace(
                    source=policy.sources[0],
                    payload={
                        "workloads": [
                            {"name": "w", "params": {"scale": 1}, "queries": [query("q", p50=10.0)]},
                            {"name": "w", "params": {"scale": 2}, "queries": [query("q", p50=100.0)]},
                        ]
                    },
                )
            ],
            [
                SimpleNamespace(
                    source=policy.sources[0],
                    payload={
                        "workloads": [
                            {"name": "w", "params": {"scale": 1}, "queries": [query("q", p50=20.0)]},
                            {"name": "w", "params": {"scale": 2}, "queries": [query("q", p50=200.0)]},
                        ]
                    },
                )
            ],
        ]

        aggregated = aggregate_measurement_runs(runs)[0].payload
        indexed = index_queries(aggregated)

        self.assertEqual(indexed[QueryKey("w", "q", '{"scale": 1}')].query["stats"]["p50"], 15.0)
        self.assertEqual(indexed[QueryKey("w", "q", '{"scale": 2}')].query["stats"]["p50"], 150.0)

    def test_baseline_validation_rejects_policy_and_platform_mismatch(self) -> None:
        policy = test_policy()
        fingerprint = GateFingerprint(
            system={},
            build={"cargo_profile": "unknown"},
            runtime={"data_dir_mode": "unknown"},
            audit={},
        )
        measurement = SimpleNamespace(source=policy.sources[0], payload=payload(query("q")))
        baseline_json = build_baseline_payload(
            gate=policy.name,
            platform_key="macos-arm64",
            policy=policy,
            fingerprint=fingerprint,
            measurements=[measurement],
        )

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "baseline.json"
            broken = dict(baseline_json)
            broken["policy"] = dict(baseline_json["policy"], sha256="0" * 64)
            path.write_text(json.dumps(broken), encoding="utf-8")
            baseline = load_baseline(path)

        with self.assertRaisesRegex(BaselineError, "policy reference"):
            validate_baseline_for_check(
                baseline=baseline,
                policy=policy,
                platform_key="macos-arm64",
                fingerprint=fingerprint,
                execution_mode="quorum",
            )
        with self.assertRaisesRegex(BaselineError, "platform"):
            validate_baseline_for_check(
                baseline=replace(baseline, policy_sha256=policy.sha256),
                policy=policy,
                platform_key="linux-amd64",
                fingerprint=fingerprint,
                execution_mode="quorum",
            )

    def test_hard_compare_rejects_missing_fingerprint_coverage(self) -> None:
        policy = replace(test_policy(), enforcement=GateEnforcement.HARD)
        baseline_fingerprint = GateFingerprint(
            system={},
            build={"cargo_profile": "unknown", "rustflags": []},
            runtime={"data_dir_mode": "unknown"},
            audit={},
        )
        current_fingerprint = GateFingerprint(
            system={},
            build={"cargo_profile": "release", "rustflags": ["-C", "target-cpu=native"]},
            runtime={"data_dir_mode": "fresh"},
            audit={},
        )
        measurement = SimpleNamespace(source=policy.sources[0], payload=payload(query("q")))
        baseline_json = build_baseline_payload(
            gate=policy.name,
            platform_key="macos-arm64",
            policy=policy,
            fingerprint=baseline_fingerprint,
            measurements=[measurement],
        )

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "baseline.json"
            path.write_text(json.dumps(baseline_json), encoding="utf-8")
            baseline = load_baseline(path)

        with self.assertRaisesRegex(BaselineError, "fingerprint"):
            validate_baseline_for_check(
                baseline=baseline,
                policy=policy,
                platform_key="macos-arm64",
                fingerprint=current_fingerprint,
                execution_mode="quorum",
            )
        self.assertFalse(is_missing_fingerprint_value([]))
        self.assertFalse(is_missing_fingerprint_value({}))
        self.assertTrue(is_missing_fingerprint_value("unknown"))

    def test_rust_micro_hard_compare_ignores_sql_runtime_fingerprint(self) -> None:
        policy = replace(load_policy(DIVAN_POLICY_PATH), enforcement=GateEnforcement.HARD)
        fingerprint = GateFingerprint(
            system={},
            build={"cargo_profile": "release"},
            runtime={"data_dir_mode": "unknown"},
            audit={},
        )
        measurement = SimpleNamespace(source=policy.sources[0], payload=payload(query("q")))
        baseline_json = build_baseline_payload(
            gate=policy.name,
            platform_key="macos-arm64",
            policy=policy,
            fingerprint=fingerprint,
            measurements=[measurement],
        )

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "baseline.json"
            path.write_text(json.dumps(baseline_json), encoding="utf-8")
            baseline = load_baseline(path)

        validate_baseline_for_check(
            baseline=baseline,
            policy=policy,
            platform_key="macos-arm64",
            fingerprint=fingerprint,
            execution_mode="quorum",
        )

    def test_policy_evolution_requires_version_bump(self) -> None:
        policy = test_policy()
        fingerprint = GateFingerprint(system={}, build={}, runtime={}, audit={})
        measurement = SimpleNamespace(source=policy.sources[0], payload=payload(query("q")))
        baseline_json = build_baseline_payload(
            gate=policy.name,
            platform_key="macos-arm64",
            policy=replace(policy, sha256="old", version=1),
            fingerprint=fingerprint,
            measurements=[measurement],
        )

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "baseline.json"
            path.write_text(json.dumps(baseline_json), encoding="utf-8")
            baseline = load_baseline(path)

        with self.assertRaisesRegex(BaselineError, "policy changed"):
            validate_existing_baseline_for_bless(
                baseline=baseline,
                policy=policy,
                platform_key="macos-arm64",
                policy_evolution=False,
            )
        with self.assertRaisesRegex(BaselineError, "version"):
            validate_existing_baseline_for_bless(
                baseline=baseline,
                policy=policy,
                platform_key="macos-arm64",
                policy_evolution=True,
            )
        validate_existing_baseline_for_bless(
            baseline=baseline,
            policy=replace(policy, version=2),
            platform_key="macos-arm64",
            policy_evolution=True,
        )

    def test_bless_fingerprint_requires_release_and_fresh_data_dir(self) -> None:
        with self.assertRaisesRegex(ValueError, "release build"):
            validate_bless_fingerprint(
                GateFingerprint(system={}, build={"cargo_profile": "debug"}, runtime={"data_dir_mode": "fresh"}, audit={})
            )
        with self.assertRaisesRegex(ValueError, "fresh data"):
            validate_bless_fingerprint(
                GateFingerprint(system={}, build={"cargo_profile": "release"}, runtime={"data_dir_mode": "unknown"}, audit={})
            )
        validate_bless_fingerprint(
            GateFingerprint(system={}, build={"cargo_profile": "release"}, runtime={"data_dir_mode": "unknown"}, audit={}),
            require_fresh_runtime=False,
        )
        validate_bless_fingerprint(
            GateFingerprint(system={}, build={"cargo_profile": "release"}, runtime={"data_dir_mode": "fresh"}, audit={})
        )

    def test_suite_staging_metadata_is_policy_bounded(self) -> None:
        policy = test_policy(sources=(replace(test_policy().sources[0], suite="s"),))
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "suites").mkdir()
            (root / "suites" / "s.toml").write_text(
                """
name = "s"

[[include]]
workload = "w"
queries = ["new"]
staging_until = "2026-05-25"
""",
                encoding="utf-8",
            )
            staging = load_staging_queries(root_dir=root, policy=policy, today=date(2026, 5, 20))

        self.assertEqual(staging[("w", "new")].staging_until, date(2026, 5, 25))

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "suites").mkdir()
            (root / "suites" / "s.toml").write_text(
                """
name = "s"

[[include]]
workload = "w"
queries = ["new"]
staging_until = "2026-06-30"
""",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "exceeds policy"):
                load_staging_queries(root_dir=root, policy=policy, today=date(2026, 5, 20))

    def test_source_registry_is_explicit(self) -> None:
        self.assertEqual(default_registry().get("sql_suite").source_type, "sql_suite")
        self.assertEqual(default_registry().get("divan_bench").source_type, "divan_bench")
        self.assertEqual(default_registry().get("mixed_sql_suite").source_type, "mixed_sql_suite")
        with self.assertRaisesRegex(ValueError, "unknown measurement source type"):
            SourceRegistry().get("missing")

    def test_divan_structured_payload_normalizes_to_gate_shape(self) -> None:
        source = load_policy(DIVAN_POLICY_PATH).sources[0]
        raw = {
            "schema_version": 1,
            "kind": "divan_bench_result",
            "crate": "paro-execution",
            "bench": "operator_runtime_dispatch",
            "benches": [
                {
                    "id": "a",
                    "items": 100,
                    "samples_ms": [3.0, 1.0, 2.0],
                    "audit": {
                        "chunk_count": 1,
                        "allocator_tracking_event_counts": [2, 4, 6],
                        "allocator_tracking_events_per_chunk": [2.0, 4.0, 6.0],
                    },
                },
                {"id": "b", "items": 200, "samples_ms": [10.0, 12.0]},
            ],
        }

        normalized = normalize_divan_payload(source, raw)
        indexed = index_queries(normalized)

        self.assertIn(QueryKey("operator_runtime_dispatch", "a", "{}"), indexed)
        query_a = indexed[QueryKey("operator_runtime_dispatch", "a", "{}")].query
        self.assertEqual(query_a["samples_count"], 3)
        self.assertEqual(query_a["stats"]["p50"], 2.0)
        self.assertEqual(query_a["stats"]["p99"], 3.0)
        self.assertEqual(query_a["audit"]["allocator_tracking_event_counts_median"], 4.0)
        self.assertEqual(query_a["audit"]["allocator_tracking_event_counts_p99"], 6.0)
        self.assertEqual(metric_value(query_a, "audit_allocator_tracking_event_counts_p99"), 6.0)

        retried = normalize_divan_payload(
            source,
            raw,
            retry_query_keys=frozenset({QueryKey("operator_runtime_dispatch", "b", "{}")}),
        )
        self.assertEqual([item["id"] for item in retried["workloads"][0]["queries"]], ["b"])

        with self.assertRaisesRegex(ValueError, "expected at least 4"):
            normalize_divan_payload(source, raw, minimum_sample_count=4)

    def test_divan_source_uses_locked_cargo_timeout_and_minimum_samples(self) -> None:
        source = load_policy(DIVAN_POLICY_PATH).sources[0]
        samples = [1.0] * 50

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            def fake_run(command, cwd, env, text, capture_output, check, timeout):
                self.assertIn("--locked", command)
                self.assertEqual(timeout, 7)
                self.assertEqual(env["PARO_DIVAN_SAMPLE_COUNT"], "50")
                Path(env["PARO_DIVAN_JSON_OUT"]).parent.mkdir(parents=True, exist_ok=True)
                Path(env["PARO_DIVAN_JSON_OUT"]).write_text(
                    json.dumps({
                        "schema_version": 1,
                        "kind": "divan_bench_result",
                        "crate": "paro-execution",
                        "bench": "operator_runtime_dispatch",
                        "benches": [{"id": "q", "items": 1, "samples_ms": samples}],
                    }),
                    encoding="utf-8",
                )
                return SimpleNamespace(returncode=0, stdout="", stderr="")

            with mock.patch.dict(os.environ, {"PARO_DIVAN_BENCH_TIMEOUT_S": "7"}, clear=False):
                with mock.patch("harness.sources.divan_bench.subprocess.run", fake_run):
                    measurement = DivanBenchSource().execute(
                        source,
                        SourceContext(
                            root_dir=root,
                            pid=0,
                            runner_module=SimpleNamespace(),
                            minimum_sample_count=50,
                        ),
                    )

        self.assertFalse(measurement.failed)
        self.assertEqual(measurement.payload["workloads"][0]["queries"][0]["samples_count"], 50)

    def test_auto_baseline_uses_only_gate_platform_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            baseline = root / "baselines" / "operator-runtime" / "macos-arm64.json"
            baseline.parent.mkdir(parents=True)
            baseline.write_text("{}", encoding="utf-8")
            (root / "baselines" / "operator-runtime-gate-macos-arm64.json").write_text("{}", encoding="utf-8")

            with mock.patch("harness.cli.common.platform_key", return_value="macos-arm64"):
                self.assertEqual(
                    gate_common.resolve_baseline(root, "operator-runtime", "auto"),
                    baseline,
                )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            legacy = root / "baselines" / "operator-runtime-gate-macos-arm64.json"
            legacy.parent.mkdir(parents=True)
            legacy.write_text("{}", encoding="utf-8")

            with mock.patch("harness.cli.common.platform_key", return_value="macos-arm64"):
                with self.assertRaisesRegex(gate_common.GateCommandError, "baseline not found"):
                    gate_common.resolve_baseline(root, "operator-runtime", "auto")


if __name__ == "__main__":
    unittest.main()
