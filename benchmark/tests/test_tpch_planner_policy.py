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

    def test_read_policy_applies_after_fixture_writes(self):
        manifest = Path(__file__).resolve().parents[1] / "workloads/tpch/workload.toml"
        workload = load_workload(manifest, param_overrides={
            "optimizer_search_policy": "pipeline", "optimizer_verify": True,
        })
        self.assertEqual(len(workload.queries), 22)
        self.assertEqual(workload.params["optimizer_search_policy"], "pipeline")
        self.assertLess(workload.setup_sql.index("SET optimizer_search_policy = 'quality'"),
                        workload.setup_sql.index("COPY lineitem"))
        self.assertGreater(workload.setup_sql.index("SET optimizer_search_policy = 'pipeline'"),
                           workload.setup_sql.index("COPY lineitem"))
        self.assertIn("SET optimizer_verify = true", workload.setup_sql)
