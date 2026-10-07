# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
import unittest

from harness.loader import load_workload


class TpchPlannerPolicyTests(unittest.TestCase):
    def test_profiles_follow_the_invocation_not_a_forced_query_override(self):
        manifest = Path(__file__).resolve().parents[1] / "workloads/tpch/workload.toml"
        for enabled in (False, True):
            workload = load_workload(manifest, default_collect_explain_profile=enabled)
            self.assertEqual(len(workload.queries), 22)
            self.assertTrue(all(q.collect_explain_profile == enabled for q in workload.queries))

    def test_fixture_and_queries_use_the_single_production_planner(self):
        manifest = Path(__file__).resolve().parents[1] / "workloads/tpch/workload.toml"
        workload = load_workload(manifest, param_overrides={
            "optimizer_verify": True,
        })
        self.assertEqual(len(workload.queries), 22)
        self.assertNotIn("optimizer_search_policy", workload.params)
        self.assertNotIn("optimizer_search_policy", workload.setup_sql)
        self.assertNotIn("optimizer_aggregate_strategy", workload.setup_sql)
        self.assertIn("SET optimizer_verify = true", workload.setup_sql)

    def test_planning_workload_has_no_retired_ablation_arms(self):
        manifest = Path(__file__).resolve().parents[1] / "workloads/optimizer_planning/workload.toml"
        workload = load_workload(manifest)
        self.assertEqual([q.id for q in workload.queries], [
            "join_aggregate_topn", "union_aggregate", "aggregate_dense_union",
        ])
        scripts = [workload.setup_sql, workload.teardown_sql]
        for query in workload.queries:
            scripts.extend([query.sql, query.setup_sql or "", query.teardown_sql or ""])
            self.assertIsNone(query.max_median_ratio_to)
        for script in scripts:
            for setting in ("optimizer_search_policy", "optimizer_aggregate_strategy", "disabled_optimizer_rules"):
                self.assertNotIn(setting, script)
