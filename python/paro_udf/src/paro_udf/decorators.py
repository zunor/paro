"""Decorator helpers for Paro batch UDFs."""

from __future__ import annotations

from dataclasses import dataclass
from functools import wraps
from typing import Any, Callable

from paro_udf.column import Column, ResultContractError, coerce_result_columns
from paro_udf.compiled import CompiledKernelSpec
from paro_udf.types import LogicalTypeSpec, parse_logical_type


@dataclass(frozen=True, slots=True)
class BatchUdfMetadata:
    name: str
    family: str
    return_types: tuple[LogicalTypeSpec, ...]
    returns_nullable: tuple[bool, ...]
    compiled_kernels: tuple[CompiledKernelSpec, ...]


def validate_batch_result(
    result: Any,
    *,
    row_count: int | None,
    return_types: object | tuple[object, ...] | None = None,
    returns_nullable: bool | tuple[bool, ...] = True,
    output_names: tuple[str, ...] | None = None,
) -> list[Column]:
    return coerce_result_columns(
        result,
        row_count=row_count,
        logical_types=return_types,
        returns_nullable=returns_nullable,
        output_names=output_names,
    )


def batch_udf(
    fn: Callable[..., Any] | None = None,
    *,
    name: str | None = None,
    family: str = "scalar_batch",
    return_type: object | None = None,
    return_types: tuple[object, ...] | None = None,
    returns_nullable: bool | tuple[bool, ...] = True,
):
    """Declare a Paro batch-oriented Python handler."""

    def decorator(inner: Callable[..., Any]) -> Callable[..., Any]:
        normalized_return_types = tuple(return_types or ((return_type,) if return_type is not None else ()))
        metadata = BatchUdfMetadata(
            name=name or inner.__name__,
            family=family,
            return_types=tuple(parse_logical_type(item) for item in normalized_return_types),
            returns_nullable=(
                tuple(bool(item) for item in returns_nullable)
                if isinstance(returns_nullable, tuple)
                else tuple(bool(returns_nullable) for _ in range(max(len(normalized_return_types), 1)))
            ),
            compiled_kernels=tuple(getattr(inner, "__paro_compiled_kernels__", ())),
        )

        @wraps(inner)
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            return inner(*args, **kwargs)

        def _validate(
            result: Any,
            row_count: int | None,
            output_names: tuple[str, ...] | None = None,
        ) -> list[Column]:
            return validate_batch_result(
                result,
                row_count=row_count,
                return_types=normalized_return_types or None,
                returns_nullable=metadata.returns_nullable,
                output_names=output_names,
            )

        wrapped.__paro_batch_udf__ = metadata
        wrapped.__paro_validate_result__ = _validate
        wrapped.__paro_compiled_kernels__ = metadata.compiled_kernels
        return wrapped

    if fn is not None:
        return decorator(fn)
    return decorator


__all__ = ["BatchUdfMetadata", "ResultContractError", "batch_udf", "validate_batch_result"]
