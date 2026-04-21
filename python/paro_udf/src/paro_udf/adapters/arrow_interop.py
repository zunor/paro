"""Arrow / PyCapsule interop helpers."""

from __future__ import annotations

from paro_udf.column import ArrowArrayExport


def export_arrow_capsule(value):
    if hasattr(value, "__arrow_c_array__"):
        return value.__arrow_c_array__()
    if hasattr(value, "__arrow_c_stream__"):
        return value.__arrow_c_stream__()
    if hasattr(value, "to_arrow"):
        return value.to_arrow()
    return value


def ensure_arrow_export(value) -> ArrowArrayExport:
    export = export_arrow_capsule(value)
    if not isinstance(export, ArrowArrayExport):
        raise TypeError("value does not expose a Paro Arrow export")
    return export
