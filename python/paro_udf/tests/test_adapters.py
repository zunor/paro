import sys
import unittest
from unittest import mock

from paro_udf.adapters import ensure_arrow_export, export_arrow_capsule, to_numpy_buffer
from paro_udf.column import Column


class FakeNdArray:
    def __init__(self, values):
        self._values = list(values)

    def tolist(self):
        return list(self._values)


class FakeNumpy:
    def frombuffer(self, _buffer, *, dtype, count):
        del dtype
        return FakeNdArray(range(1, count + 1))

    def array(self, values, dtype=None):
        del dtype
        return FakeNdArray(values)

    asarray = array


class AdapterTests(unittest.TestCase):
    def test_numpy_and_arrow_adapters_consume_column_protocols(self) -> None:
        column = Column([1], logical_type="int64")

        with mock.patch.dict(sys.modules, {"numpy": FakeNumpy()}):
            array = to_numpy_buffer(column)
        export = export_arrow_capsule(column)

        self.assertEqual(array.tolist(), [1])
        self.assertEqual(ensure_arrow_export(column).logical_type, "int64")
        self.assertEqual(export.materialize_py(), [1])


if __name__ == "__main__":
    unittest.main()
