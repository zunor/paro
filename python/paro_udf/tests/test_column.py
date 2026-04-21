import struct
import sys
import unittest
import warnings
from unittest import mock

from paro_udf.column import ArrowArrayExport, Column, SlowPathWarning


class FakeNdArray:
    def __init__(self, values, *, source=None, dtype=None):
        self._values = list(values)
        self.source = source
        self.dtype = dtype

    def tolist(self):
        return list(self._values)

    def reshape(self, *_shape):
        return self


class FakeNumpy:
    def frombuffer(self, buffer, *, dtype, count):
        fmt = {
            "int64": "<q",
            "int32": "<i",
            "float64": "<d",
            "bool": "<B",
        }[dtype]
        size = struct.calcsize(fmt)
        values = [
            struct.unpack_from(fmt, buffer, index * size)[0]
            for index in range(count)
        ]
        if dtype == "bool":
            values = [bool(value) for value in values]
        return FakeNdArray(values, source=buffer, dtype=dtype)

    def full(self, length, value, dtype=None):
        return FakeNdArray([value] * length, dtype=dtype)

    def arange(self, start, stop, step, dtype=None):
        values = []
        current = start
        while current < stop:
            values.append(current)
            current += step
        return FakeNdArray(values, dtype=dtype)

    def array(self, values, dtype=None):
        return FakeNdArray(values, dtype=dtype)

    asarray = array


class FakePyArrow:
    def consume_paro_export(self, export):
        return {
            "logical_type": export.logical_type,
            "encoding": export.encoding,
            "data": export.materialize_py(),
            "source": export.source,
        }

    def array(self, values):
        return {"data": list(values)}


class ColumnTests(unittest.TestCase):
    def test_fixed_width_to_numpy_uses_buffer_fast_path(self) -> None:
        column = Column([1, 2, 3], logical_type="int64")
        fake_numpy = FakeNumpy()
        with mock.patch.dict(sys.modules, {"numpy": fake_numpy}):
            array = column.to_numpy()
        self.assertEqual(array.tolist(), [1, 2, 3])
        self.assertIs(array.source, column.borrow_buffer())

    def test_arrow_export_and_to_arrow_share_column_source(self) -> None:
        column = Column(["a", "bb"], logical_type="varchar")
        export = column.__arrow_c_array__()
        self.assertIsInstance(export, ArrowArrayExport)

        fake_pyarrow = FakePyArrow()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", SlowPathWarning)
            with mock.patch.dict(sys.modules, {"pyarrow": fake_pyarrow}):
                arrow = column.to_arrow()

        self.assertEqual(arrow["logical_type"], "varchar")
        self.assertEqual(arrow["data"], ["a", "bb"])
        self.assertIs(arrow["source"], column)

    def test_from_result_can_infer_row_count_for_relation_expanding_outputs(self) -> None:
        column = Column.from_result([1, 4, 9], row_count=None, logical_type="int64")
        self.assertEqual(column.materialize_py(), [1, 4, 9])

    def test_from_result_rejects_constant_without_expected_row_count(self) -> None:
        with self.assertRaisesRegex(Exception, "explicit expected row count"):
            Column.from_result(7, row_count=None, logical_type="int64")

    def test_advanced_encodings_materialize_with_explicit_slow_path(self) -> None:
        constant = Column.from_constant(7, logical_type="int64", length=3)
        sequence = Column.from_sequence(10, 2, "int64", length=3)
        dictionary = Column.from_dictionary([0, 1, 0], ["x", "y"], logical_type="varchar")
        nested = Column([[1, 2], [3]], logical_type="list<int64>", encoding="list")

        self.assertEqual(constant.as_constant_view().value, 7)
        self.assertEqual(sequence.as_sequence_view().step, 2)
        self.assertEqual(dictionary.as_dictionary_view().indices, (0, 1, 0))

        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter("always")
            self.assertEqual(constant.materialize_py(), [7, 7, 7])
            self.assertEqual(sequence.materialize_py(), [10, 12, 14])
            self.assertEqual(dictionary.materialize_py(), ["x", "y", "x"])
            self.assertEqual(nested.materialize_py(), [[1, 2], [3]])

        self.assertTrue(any(item.category is SlowPathWarning for item in recorded))


if __name__ == "__main__":
    unittest.main()
