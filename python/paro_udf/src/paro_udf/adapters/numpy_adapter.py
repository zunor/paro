"""NumPy fast-path adapter helpers."""

from __future__ import annotations

from paro_udf.column import Column


def to_numpy_buffer(value):
    if isinstance(value, Column):
        return value.to_numpy()
    if hasattr(value, "to_numpy"):
        return value.to_numpy()
    return value
