import unittest
import warnings

from paro_udf import batch_udf, register_compiled_kernel, register_native_jit_kernel
from paro_udf.column import ResultContractError, SlowPathWarning


class DecoratorTests(unittest.TestCase):
    def test_batch_udf_attaches_metadata_and_compiled_candidates(self) -> None:
        @register_compiled_kernel(kind="numba", signature="i64 -> i64")
        @register_native_jit_kernel(tier="experimental")
        @batch_udf(return_type="int64")
        def sample(_ctx, column):
            return column

        metadata = sample.__paro_batch_udf__
        self.assertEqual(metadata.name, "sample")
        self.assertEqual(metadata.family, "scalar_batch")
        self.assertEqual(metadata.return_types[0].canonical_name(), "int64")
        self.assertEqual(len(metadata.compiled_kernels), 2)
        self.assertEqual([item.kind for item in metadata.compiled_kernels], ["jit", "numba"])
        self.assertTrue(metadata.compiled_kernels[0].zero_dependency)

    def test_batch_udf_result_validation_broadcasts_scalars(self) -> None:
        @batch_udf(return_type="int64")
        def sample(_ctx, column):
            return 7

        columns = sample.__paro_validate_result__(sample(None, None), 3)
        self.assertEqual(len(columns), 1)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", SlowPathWarning)
            self.assertEqual(columns[0].materialize_py(), [7, 7, 7])

    def test_batch_udf_result_validation_rejects_length_mismatch(self) -> None:
        @batch_udf(return_type="int64")
        def sample(_ctx, column):
            return [1, 2]

        with self.assertRaises(ResultContractError):
            sample.__paro_validate_result__(sample(None, None), 3)


if __name__ == "__main__":
    unittest.main()
