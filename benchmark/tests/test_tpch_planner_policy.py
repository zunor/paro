# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
import unittest

from harness.loader import load_workload


class TpchPlannerPolicyTests(unittest.TestCase):
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
