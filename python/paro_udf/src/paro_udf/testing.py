# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Local handler checks using the same public Arrow objects as execution."""

import pyarrow as pa

from .context import Context


def vector_call(handler, *arrays, num_rows=None, return_type=None):
    """Invoke a vector handler and check its declared result shape and type."""
    if any(not isinstance(array, pa.Array) for array in arrays):
        raise TypeError("vector arguments must be PyArrow arrays")
    rows = len(arrays[0]) if arrays else num_rows
    if rows is None or rows < 0 or any(len(array) != rows for array in arrays):
        raise ValueError("arguments must have the same row count")
    result = handler(Context(rows, "local", 0, 0), *arrays)
    if isinstance(result, pa.ChunkedArray):
        result = result.combine_chunks()
    if not isinstance(result, pa.Array):
        raise TypeError("vector result must be a PyArrow array")
    if len(result) != rows:
        raise ValueError("vector result row count mismatch")
    if return_type is not None and result.type != return_type:
        raise TypeError("vector result type mismatch")
    result.validate(full=True)
    return result
