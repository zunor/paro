from decimal import Decimal, localcontext
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "corpora"))
from numeric_result_contract import (
    Interval, Uncovered, assert_numeric_bijection, sample_moments, schedule_envelope,
)


class NumericalContractTests(unittest.TestCase):
    def test_independent_moments_and_null_semantics(self):
        self.assertEqual(sample_moments([]), (None, None, None))
        self.assertEqual(schedule_envelope([None]), (None, None, None))
        self.assertEqual(sample_moments([7]), (Decimal(7), None, None))
        self.assertEqual(sample_moments([-1, 1]), (Decimal(0), Decimal(2), None))
        mean, variance, cv = sample_moments([1, 1, 4, None])
        self.assertEqual((mean, variance), (Decimal(2), Decimal(3)))
        self.assertAlmostEqual(float(cv), 3**0.5/2)

    def test_duplicate_zero_and_large_offset_inputs(self):
        for values in [[4]*4, [-4]*4, [0]*4, [10**12+i for i in [0, 1, 1, 2]]]:
            reference = sample_moments(values)
            envelope = schedule_envelope(values)
            for index in [0, 1, 2]:
                if reference[index] is None:
                    self.assertIsNone(envelope[index])
                else:
                    # Exact real result is independently retained; an IEEE
                    # schedule need not round the final expression once.
                    self.assertTrue(envelope[index].lower <= envelope[index].upper)
            with localcontext() as context:
                context.prec = 100
                self.assertEqual(reference[1], 0 if len(set(values)) == 1 else Decimal(2)/3)

    def test_schedule_bound_is_not_a_fixed_ulp_tolerance(self):
        for values in [[1, 1, 4], [14, 132, 325], [-7, -3, -1, 9]]:
            forward = schedule_envelope(values)
            self.assertEqual(forward, schedule_envelope(list(reversed(values))))
        self.assertEqual(schedule_envelope([14, 132, 325])[2], Interval(1.0, 1.0))
        with self.assertRaises(Uncovered):
            schedule_envelope([1]*7)
        with self.assertRaises(Uncovered):
            schedule_envelope([2**53, 1])

    def test_relational_boundaries_and_duplicates_are_not_tolerated(self):
        self.assertFalse(Interval(1.0, 1.0).greater_than(1))
        with self.assertRaises(Uncovered):
            Interval(0.999999999, 1.000000001).greater_than(1)
        reference = [(1, Interval(0., 2.)), (1, Interval(0., 0.))]
        assert_numeric_bijection([(1, 0.), (1, 2.)], reference)
        with self.assertRaises(AssertionError):
            assert_numeric_bijection([(1, 2.), (1, 2.)], reference)
        with self.assertRaises(AssertionError):
            assert_numeric_bijection([(1, 0.)], reference)
        with self.assertRaises(AssertionError):
            assert_numeric_bijection([(2, 0.), (1, 2.)], reference)
        with self.assertRaises(AssertionError):
            assert_numeric_bijection([(1, float("nan"))], [(1, Interval(0., 2.))])
        with self.assertRaises(AssertionError):
            assert_numeric_bijection([(1, Decimal("1.001"))], [(1, Decimal("1"))])
