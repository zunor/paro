# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

import unittest

import pyarrow as pa
import pyarrow.compute as pc

from paro_udf.testing import vector_call


class ArrowTests(unittest.TestCase):
    def test_compute_and_native_capsules(self):
        values = pa.array([1, None, 3], type=pa.int32())
        result = vector_call(lambda ctx, a: pc.multiply(a, 2).cast(a.type),
                             values, return_type=pa.int32())
        schema, array = result.__arrow_c_array__()
        imported = pa.Array._import_from_c_capsule(schema, array)
        self.assertEqual(imported.to_pylist(), [2, None, 6])

    def test_zero_argument_shape(self):
        result = vector_call(lambda ctx: pa.array([7] * ctx.num_rows, type=pa.int32()),
                             num_rows=3, return_type=pa.int32())
        self.assertEqual(result.to_pylist(), [7, 7, 7])

    def test_reject_shortened_result(self):
        with self.assertRaisesRegex(ValueError, "row count"):
            vector_call(lambda ctx, a: a.slice(0, 1), pa.array([1, 2]))

    def test_reject_wrong_type(self):
        with self.assertRaisesRegex(TypeError, "type mismatch"):
            vector_call(lambda ctx, a: a, pa.array([1], type=pa.int64()), return_type=pa.int32())
