"""Capability-gated registration hooks for trusted compiled kernels."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any, Callable


@dataclass(frozen=True, slots=True)
class CompiledKernelSpec:
    kind: str
    entrypoint: str
    capability: str = "compiled_kernel"
    trusted_only: bool = True
    zero_dependency: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


def _attach_compiled_kernel(
    fn: Callable[..., Any],
    *,
    kind: str,
    entrypoint: str | None = None,
    capability: str = "compiled_kernel",
    trusted_only: bool = True,
    zero_dependency: bool = False,
    **metadata: Any,
) -> Callable[..., Any]:
    """Attach one compiled-kernel candidate to a batch UDF definition."""

    spec = CompiledKernelSpec(
        kind=kind,
        entrypoint=entrypoint or fn.__name__,
        capability=capability,
        trusted_only=trusted_only,
        zero_dependency=zero_dependency,
        metadata=dict(metadata),
    )
    compiled = list(getattr(fn, "__paro_compiled_kernels__", ()))
    compiled.append(spec)
    fn.__paro_compiled_kernels__ = tuple(compiled)
    if hasattr(fn, "__paro_batch_udf__"):
        fn.__paro_batch_udf__ = replace(
            fn.__paro_batch_udf__,
            compiled_kernels=tuple(compiled),
        )
    return fn


def register_compiled_kernel(
    fn: Callable[..., Any] | None = None,
    *,
    kind: str,
    entrypoint: str | None = None,
    capability: str = "compiled_kernel",
    trusted_only: bool = True,
    zero_dependency: bool = False,
    **metadata: Any,
):
    """Register a compiled-kernel candidate as a decorator or a helper call."""

    def decorator(inner: Callable[..., Any]) -> Callable[..., Any]:
        return _attach_compiled_kernel(
            inner,
            kind=kind,
            entrypoint=entrypoint,
            capability=capability,
            trusted_only=trusted_only,
            zero_dependency=zero_dependency,
            **metadata,
        )

    if fn is not None:
        return decorator(fn)
    return decorator


def register_native_jit_kernel(
    fn: Callable[..., Any] | None = None,
    *,
    entrypoint: str | None = None,
    trusted_only: bool = True,
    **metadata: Any,
):
    """Register a CPython-native JIT candidate for the compiled-kernel lane."""

    return register_compiled_kernel(
        fn,
        kind="jit",
        entrypoint=entrypoint,
        trusted_only=trusted_only,
        zero_dependency=True,
        **metadata,
    )
