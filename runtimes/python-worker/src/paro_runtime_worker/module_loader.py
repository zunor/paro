"""Module loading, handler discovery, and cache management for Python workers."""

from __future__ import annotations

from dataclasses import dataclass, field
import importlib
import importlib.util
from pathlib import Path
import sys
from types import ModuleType
from typing import Callable


@dataclass(frozen=True, slots=True)
class ModuleRequest:
    module_name: str | None = None
    module_path: str | None = None
    handler: str = "main"
    cache_key: str | None = None

    def resolved_cache_key(self) -> str:
        if self.cache_key:
            return self.cache_key
        if self.module_path:
            path = Path(self.module_path).resolve()
            stamp = path.stat().st_mtime_ns if path.exists() else 0
            return f"path::{path}::{stamp}"
        if self.module_name:
            return f"module::{self.module_name}"
        raise ValueError("module request requires either `module_name` or `module_path`")

    def import_name(self) -> str:
        if self.module_name:
            return self.module_name
        assert self.module_path is not None
        return f"paro_worker_{Path(self.module_path).stem}_{abs(hash(Path(self.module_path).resolve()))}"


@dataclass(frozen=True, slots=True)
class ResolvedHandler:
    target: Callable
    validator: Callable | None
    backend: str
    compiled_kind: str | None = None


@dataclass(slots=True)
class ModuleLoader:
    search_paths: tuple[str, ...] = ()
    _module_cache: dict[str, ModuleType] = field(default_factory=dict)
    _handler_cache: dict[tuple[str, str], Callable] = field(default_factory=dict)
    _execution_cache: dict[tuple[str, str, str, str], ResolvedHandler] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.ensure_search_paths(self.search_paths)

    def ensure_search_paths(self, paths: tuple[str, ...] | list[str]) -> None:
        for search_path in reversed(self.search_paths):
            if search_path and search_path not in sys.path:
                sys.path.insert(0, search_path)
        if tuple(paths) != self.search_paths:
            self.search_paths = tuple(paths)
        for search_path in reversed(tuple(paths)):
            if search_path and search_path not in sys.path:
                sys.path.insert(0, search_path)

    def load_module(self, request: ModuleRequest) -> ModuleType:
        cache_key = request.resolved_cache_key()
        cached = self._module_cache.get(cache_key)
        if cached is not None:
            return cached

        if request.module_path:
            path = Path(request.module_path).resolve()
            parent = str(path.parent)
            if parent and parent not in sys.path:
                sys.path.insert(0, parent)
            spec = importlib.util.spec_from_file_location(request.import_name(), path)
            if spec is None or spec.loader is None:
                raise ImportError(f"cannot import module from `{path}`")
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        elif request.module_name:
            module = importlib.import_module(request.module_name)
        else:
            raise ImportError("module request requires `module_name` or `module_path`")

        self._module_cache[cache_key] = module
        return module

    def load_handler(self, request: ModuleRequest) -> Callable:
        cache_key = (request.resolved_cache_key(), request.handler)
        cached = self._handler_cache.get(cache_key)
        if cached is not None:
            return cached

        module = self.load_module(request)
        target = self._resolve_attr(module, request.handler, "handler")
        if not callable(target):
            raise TypeError(f"resolved handler `{request.handler}` is not callable")
        self._handler_cache[cache_key] = target
        return target

    def load_execution_target(
        self,
        request: ModuleRequest,
        *,
        execution_backend: str = "process",
        compiled_kernel_kind: str | None = None,
    ) -> ResolvedHandler:
        cache_key = (
            request.resolved_cache_key(),
            request.handler,
            execution_backend,
            compiled_kernel_kind or "",
        )
        cached = self._execution_cache.get(cache_key)
        if cached is not None:
            return cached

        module = self.load_module(request)
        base_handler = self.load_handler(request)
        validator = getattr(base_handler, "__paro_validate_result__", None)
        if execution_backend != "compiled_kernel":
            resolved = ResolvedHandler(
                target=base_handler,
                validator=validator if callable(validator) else None,
                backend=execution_backend,
            )
            self._execution_cache[cache_key] = resolved
            return resolved

        metadata = getattr(base_handler, "__paro_batch_udf__", None)
        compiled_specs = tuple(
            getattr(base_handler, "__paro_compiled_kernels__", ())
            or getattr(metadata, "compiled_kernels", ())
        )
        if not compiled_specs:
            raise LookupError(
                f"handler `{request.handler}` does not declare any compiled kernel candidates"
            )
        selected_spec = None
        if compiled_kernel_kind is not None:
            for candidate in compiled_specs:
                if getattr(candidate, "kind", None) == compiled_kernel_kind:
                    selected_spec = candidate
                    break
        if selected_spec is None:
            selected_spec = compiled_specs[0]

        entrypoint = getattr(selected_spec, "entrypoint", request.handler)
        target = self._resolve_attr(module, entrypoint, "compiled kernel")
        if not callable(target):
            raise TypeError(f"compiled kernel `{entrypoint}` is not callable")
        compiled_validator = getattr(target, "__paro_validate_result__", None)
        resolved = ResolvedHandler(
            target=target,
            validator=compiled_validator if callable(compiled_validator) else (validator if callable(validator) else None),
            backend="compiled_kernel",
            compiled_kind=getattr(selected_spec, "kind", None),
        )
        self._execution_cache[cache_key] = resolved
        return resolved

    def load_execution_chain(
        self,
        request: ModuleRequest,
        *,
        execution_backend: str = "process",
        compiled_kernel_kind: str | None = None,
        additional_handlers: tuple[str, ...] = (),
    ) -> tuple[ResolvedHandler, ...]:
        chain = [
            self.load_execution_target(
                request,
                execution_backend=execution_backend,
                compiled_kernel_kind=compiled_kernel_kind,
            )
        ]
        for handler in additional_handlers:
            chain.append(
                self.load_execution_target(
                    ModuleRequest(
                        module_name=request.module_name,
                        module_path=request.module_path,
                        handler=handler,
                        cache_key=request.cache_key,
                    ),
                    execution_backend=execution_backend,
                    compiled_kernel_kind=compiled_kernel_kind,
                )
            )
        return tuple(chain)

    def _resolve_attr(self, module: ModuleType, dotted_path: str, label: str) -> object:
        target: object = module
        for part in dotted_path.split("."):
            target = getattr(target, part)
        return target

    def clear(self) -> None:
        self._module_cache.clear()
        self._handler_cache.clear()
        self._execution_cache.clear()
