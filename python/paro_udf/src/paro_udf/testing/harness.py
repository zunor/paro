"""Testing harness for local Paro Python UDF development."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from paro_udf.column import Column, ResultContractError
from paro_udf.context import BatchContext
from paro_udf.types import parse_logical_type


def make_column(
    values: Any,
    *,
    logical_type: object | None = None,
    encoding: str = "flat",
    null_mask: Sequence[bool] | None = None,
    length: int | None = None,
    name: str | None = None,
    offset_width: str = "U32",
) -> Column:
    if encoding == "constant":
        if length is None:
            raise ResultContractError("constant test columns require an explicit length")
        return Column.from_constant(
            values,
            logical_type=logical_type,
            length=length,
            null_mask=null_mask,
            name=name,
        )
    if encoding == "sequence":
        if not isinstance(values, Sequence) or len(values) != 2:
            raise ResultContractError("sequence test columns require a `(start, step)` pair")
        if length is None:
            raise ResultContractError("sequence test columns require an explicit length")
        return Column.from_sequence(
            values[0],
            values[1],
            logical_type or "int64",
            length=length,
            null_mask=null_mask,
            name=name,
        )
    if encoding == "dictionary":
        if not isinstance(values, dict) or "indices" not in values or "dictionary" not in values:
            raise ResultContractError(
                "dictionary test columns require `{'indices': ..., 'dictionary': ...}`"
            )
        return Column.from_dictionary(
            values["indices"],
            values["dictionary"],
            logical_type=logical_type,
            null_mask=null_mask,
            name=name,
        )
    return Column(
        values,
        logical_type=logical_type,
        encoding=encoding,
        null_mask=null_mask,
        name=name,
        length=length,
        offset_width=offset_width,
    )


def make_context(
    batch_id: int = 0,
    *,
    query_id: int | None = None,
    routine_identity: str | None = None,
    capability_profile: str | None = None,
    execution_backend: str | None = None,
    output_row_hint: int | None = None,
    **metadata: Any,
) -> BatchContext:
    return BatchContext(
        batch_id=batch_id,
        query_id=query_id,
        routine_identity=routine_identity,
        capability_profile=capability_profile,
        execution_backend=execution_backend,
        output_row_hint=output_row_hint,
        metadata=dict(metadata),
    )


def assert_column_equal(left: Column, right: Column, *, check_type: bool = True) -> None:
    if check_type:
        if parse_logical_type(left.logical_type) != parse_logical_type(right.logical_type):
            raise AssertionError(
                f"logical type mismatch: {left.logical_type!r} != {right.logical_type!r}"
            )
        if left.encoding != right.encoding:
            raise AssertionError(f"encoding mismatch: {left.encoding!r} != {right.encoding!r}")
    if left.null_mask != right.null_mask:
        raise AssertionError(f"null mask mismatch: {left.null_mask!r} != {right.null_mask!r}")
    left_values = left._materialize_values()
    right_values = right._materialize_values()
    if left_values != right_values:
        raise AssertionError(
            f"column values mismatch: {left_values!r} != {right_values!r}"
        )
