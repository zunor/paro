import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


class RegressEvidenceAuditTests(unittest.TestCase):
    def run_audit(self, extra=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "regress/cases").mkdir(parents=True)
            (root / "regress/harness").symlink_to(ROOT / "regress/harness", target_is_directory=True)
            expected = "EXPLAIN SELECT 1;\nQUERY PLAN\nold\n\nSELECT 1;\nvalue\n1\n"
            (root / "regress/cases/example.result").write_text(expected)
            for arm in ["control", "probe"]:
                (root / arm / "actuals").mkdir(parents=True)
                (root / arm / "error.txt").write_text("[SCRIPT   FILE]: cases/example.sql\n")
                (root / arm / "actuals/example.sql.actual").write_text(expected.replace("old", "new").replace("value\n1", "value\n2"))
            if extra:
                (root / "probe/actuals/orphan.actual").write_text(expected)
            output = subprocess.check_output([
                sys.executable, str(ROOT / "benchmark/tools/audit_regress_reports.py"),
                "--repository", str(root), "--control", str(root / "control"), "--probe", str(root / "probe"),
            ], text=True)
            return json.loads(output)

    def test_audits_result_mismatch_after_first_plan_difference(self):
        report = self.run_audit()
        self.assertTrue(report["all_actuals_byte_identical"])
        self.assertEqual(report["counts"]["probe:explain_snapshot"], 1)
        self.assertEqual(report["counts"]["probe:non_explain_result"], 1)
        self.assertEqual(len(report["cases"][0]["arms"]["probe"]["differences"]), 2)
        self.assertIn("raw snapshots remain failed", report["acceptance"])

    def test_unclaimed_actual_is_not_silently_ignored(self):
        report = self.run_audit(extra=True)
        self.assertFalse(report["all_actuals_byte_identical"])
        self.assertEqual(report["unexpected_actuals"]["probe"], ["orphan.actual"])
