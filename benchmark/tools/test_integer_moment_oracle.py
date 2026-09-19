import copy
import math
import unittest

from analyze_integer_moment_oracle import analyze


def cell(value):
    return {"text": str(value), "float_hex": value.hex() if isinstance(value, float) else None}


class IndependentMomentOracleTests(unittest.TestCase):
    def fixture(self):
        # Input bag [1, 1, 4], not another estimator or engine's stddev.
        # mean=2, sample variance=3, cov=sqrt(3)/2.
        sufficient = [[cell(v) for v in (7, 3, 6, 18)]]
        moments = {"actual": sufficient, "expected": copy.deepcopy(sufficient)}
        result = [[cell(v) for v in (7, 2.0, math.sqrt(3) / 2)]]
        results = {"actual": result, "expected": copy.deepcopy(result),
                   "oracle_status": "exact_match"}
        return moments, results

    def test_duplicate_input_bag_uses_sample_not_population_variance(self):
        moments, results = self.fixture()
        report = analyze(moments, results, 1, [([0], 1, 2)])
        self.assertEqual(report["engines"]["actual"]["ulp_histogram"], {0: 2})

    def test_one_ulp_is_reported_not_silently_certified(self):
        moments, results = self.fixture()
        results["actual"][0][2] = cell(math.nextafter(math.sqrt(3) / 2, math.inf))
        results["oracle_status"] = "mismatch"
        report = analyze(moments, results, 1, [([0], 1, 2)])
        self.assertEqual(report["engines"]["actual"]["ulp_histogram"], {0: 1, 1: 1})
        self.assertEqual(report["original_oracle_status"], "mismatch")

    def test_integer_value_loss_and_key_mismatch_are_errors(self):
        moments, results = self.fixture()
        moments["actual"][0][-1] = cell(19)
        with self.assertRaisesRegex(ValueError, "sufficient statistics"):
            analyze(moments, results, 1, [([0], 1, 2)])
        moments, results = self.fixture()
        results["actual"] *= 2
        with self.assertRaisesRegex(ValueError, "key order or multiset"):
            analyze(moments, results, 1, [([0], 1, 2)])

    def test_null_and_zero_mean_require_explicit_contract(self):
        moments, results = self.fixture()
        for side in ("actual", "expected"):
            moments[side][0][-2] = cell(0)
        with self.assertRaisesRegex(ValueError, "zero/NULL contract"):
            analyze(moments, results, 1, [([0], 1, 2)])


if __name__ == "__main__":
    unittest.main()
