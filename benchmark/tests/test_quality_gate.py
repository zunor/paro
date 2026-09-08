import math
import json
import unittest

from harness.quality_gate import evaluate, q_error, summarize


class QualityGateTests(unittest.TestCase):
    def test_symmetric_q_error(self):
        self.assertEqual(q_error(10, 10), 1.0)
        self.assertEqual(q_error(100, 10), q_error(10, 100))
        self.assertTrue(math.isinf(q_error(0, 1)))

    def test_gate_rejects_tail_regression(self):
        baseline = evaluate(
            {"observations": [{"estimated_rows": 10, "actual_rows": 10}]}
        )
        result = evaluate(
            {"observations": [{"estimated_rows": 100, "actual_rows": 10}]},
            baseline,
        )
        self.assertFalse(result["passed"])

    def test_summary_is_deterministic(self):
        self.assertEqual(summarize([1.0, 2.0, 4.0])["q95"], 4.0)

    def test_infinite_tail_is_strict_json(self):
        payload = json.dumps(summarize([1.0, math.inf]), allow_nan=False)
        self.assertIn('"max": "inf"', payload)


if __name__ == "__main__":
    unittest.main()
