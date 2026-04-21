"""Trusted fast-lane helpers for compiled and sub-interpreter execution."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from typing import Any

from paro_runtime_worker.protocol.control_header import ControlMessageKind

try:
    from concurrent import interpreters
except ImportError:  # pragma: no cover - Python without sub-interpreter support.
    interpreters = None


_SUBINTERPRETER_RUNNER = """
import builtins
import importlib.machinery
import json
import sys

if "_paro_decoder" not in globals():
    from paro_runtime_worker.column_decoder import ColumnDecoder
    from paro_runtime_worker.column_encoder import ColumnEncoder
    from paro_runtime_worker.module_loader import ModuleLoader, ModuleRequest
    from paro_runtime_worker.protocol.sideband import PythonTracebackPayload
    from paro_udf.context import BatchContext

    def _paro_json_default(value):
        if isinstance(value, memoryview):
            return bytes(value).hex()
        if isinstance(value, bytes):
            return value.hex()
        if hasattr(value, "to_dict"):
            return value.to_dict()
        raise TypeError(f"cannot JSON-encode payload value of type {type(value).__name__}")

    def _paro_normalize_buffers(buffers):
        normalized = []
        for buffer in buffers:
            if isinstance(buffer, str):
                normalized.append(bytes.fromhex(buffer))
            else:
                normalized.append(buffer)
        return tuple(normalized)

    def _paro_stdlib_paths():
        prefixes = {sys.base_prefix, sys.prefix, sys.exec_prefix}
        prefixes = {prefix for prefix in prefixes if prefix}
        paths = []
        for entry in sys.path:
            if entry and any(entry.startswith(prefix) for prefix in prefixes):
                paths.append(entry)
        return paths

    def _paro_install_import_guard(policy):
        allowed = {
            str(module).split(".", 1)[0]
            for module in policy.get("allowed_modules", ())
            if str(module)
        }
        if not allowed and policy.get("import_policy") != "allow_list":
            return
        allowed.update({"paro_udf", "paro_runtime_worker"})
        extension_policy = policy.get("extension_modules", "allow_validated_only")
        extension_suffixes = tuple(importlib.machinery.EXTENSION_SUFFIXES)
        original_import = _paro_original_import

        def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
            root = name.split(".", 1)[0]
            if (
                allowed
                and root not in allowed
                and root not in sys.modules
                and root not in sys.builtin_module_names
            ):
                raise ImportError(
                    f"module `{root}` is not allowed by sub-interpreter policy"
                )
            module = original_import(name, globals, locals, fromlist, level)
            if extension_policy == "deny_all":
                for candidate in (module, sys.modules.get(root)):
                    file_name = getattr(candidate, "__file__", "") or ""
                    if file_name.endswith(extension_suffixes):
                        raise ImportError(
                            f"native extension module `{root}` is denied by sub-interpreter policy"
                        )
            return module

        builtins.__import__ = guarded_import

    def _paro_apply_subinterpreter_policy(search_paths, policy):
        if not isinstance(policy, dict):
            return
        import_policy = policy.get("import_policy", "inherit_worker_paths")
        if import_policy in {"artifact_and_stdlib_only", "allow_list"}:
            normalized = []
            for entry in list(search_paths) + _paro_stdlib_paths():
                if entry and entry not in normalized:
                    normalized.append(entry)
            sys.path[:] = normalized
        _paro_install_import_guard(policy)

    def _paro_execute_chain(resolved_chain, context, columns, final_row_count, output_names):
        current_columns = tuple(columns)
        current_row_count = len(current_columns[0]) if current_columns else final_row_count
        for index, resolved in enumerate(resolved_chain):
            result = resolved.target(context, *current_columns)
            validator = resolved.validator
            if validator is None:
                raise TypeError(
                    "kernel fusion requires handlers declared with @batch_udf"
                )
            is_last = index == len(resolved_chain) - 1
            current_columns = tuple(
                validator(
                    result,
                    final_row_count if is_last else current_row_count,
                    output_names=output_names if is_last else None,
                )
            )
            current_row_count = len(current_columns[0]) if current_columns else 0
        return current_columns

    _paro_decoder = ColumnDecoder()
    _paro_encoder = ColumnEncoder()
    _paro_loader = ModuleLoader()
    _paro_original_import = builtins.__import__

task = json.loads(_paro_request_queue.get())
payload = task["payload"]
batch_id = int(task["batch_id"])
context_payload = dict(payload.get("context", {}))
module_request = ModuleRequest(
    module_name=payload.get("module_name"),
    module_path=payload.get("module_path"),
    handler=payload.get("handler", "main"),
    cache_key=payload.get("cache_key"),
)
context = BatchContext(
    batch_id=batch_id,
    query_id=context_payload.get("query_id"),
    routine_identity=context_payload.get("routine_identity"),
    capability_profile=context_payload.get("capability_profile"),
    execution_backend=context_payload.get("execution_backend"),
    output_row_hint=context_payload.get("output_row_hint"),
    metadata=dict(context_payload.get("metadata", {})),
)

try:
    ownership = payload.get("ownership") or {}
    decoded = _paro_decoder.decode_batch(
        dict(payload["lease"]),
        _paro_normalize_buffers(payload.get("buffers", ())),
        expected_host_epoch=ownership.get("owner_host_epoch"),
        expected_query_epoch=ownership.get("owner_query_epoch"),
    )
    _paro_loader.ensure_search_paths(tuple(payload.get("search_paths", ())))
    _paro_apply_subinterpreter_policy(
        tuple(payload.get("search_paths", ())),
        context.metadata.get("subinterpreter_policy", {}),
    )
    kernel_fusion = context.metadata.get("kernel_fusion")
    if isinstance(kernel_fusion, dict):
        additional_handlers = tuple(kernel_fusion.get("handlers", ()))
    elif isinstance(kernel_fusion, (list, tuple)):
        additional_handlers = tuple(kernel_fusion)
    else:
        additional_handlers = ()
    resolved_chain = _paro_loader.load_execution_chain(
        module_request,
        execution_backend=context.execution_backend or "subinterpreter",
        compiled_kernel_kind=context.metadata.get("compiled_kernel_kind"),
        additional_handlers=tuple(str(handler) for handler in additional_handlers if str(handler)),
    )
    if len(resolved_chain) > 1:
        validated_columns = _paro_execute_chain(
            resolved_chain,
            context,
            decoded.columns,
            payload.get("expected_row_count"),
            tuple(payload["output_names"]) if payload.get("output_names") is not None else None,
        )
        encoded = _paro_encoder.encode_columns(validated_columns)
    else:
        resolved = resolved_chain[0]
        result = resolved.target(context, *decoded.columns)
        if resolved.validator is not None:
            validated_columns = resolved.validator(
                result,
                payload.get("expected_row_count"),
                output_names=tuple(payload["output_names"]) if payload.get("output_names") is not None else None,
            )
            encoded = _paro_encoder.encode_columns(validated_columns)
        else:
            encoded = _paro_encoder.encode_result(
                result,
                row_count=payload.get("expected_row_count"),
                logical_types=tuple(payload.get("return_types", ())) or None,
                output_names=tuple(payload["output_names"]) if payload.get("output_names") is not None else None,
                returns_nullable=payload.get("returns_nullable", True),
            )
    response = {
        "kind": "complete",
        "payload": {
            "state": "Finished",
            "lease": encoded.to_payload(
                lease_id=int(payload["lease"]["lease_id"]),
                ownership=ownership,
                completion_fence=int(payload.get("completion_fence", 0)),
            ),
            "buffers": list(encoded.buffers),
        },
    }
    _paro_response_queue.put(json.dumps(response, default=_paro_json_default))
except Exception as exc:
    traceback_payload = PythonTracebackPayload.from_exception(
        exc,
        module=module_request.module_path or module_request.module_name or "<unknown>",
        handler=module_request.handler,
        batch_id=batch_id,
    )
    _paro_response_queue.put(
        json.dumps(
            {
                "kind": "error",
                "payload": traceback_payload.to_dict(),
            },
            default=_paro_json_default,
        )
    )
"""


def _json_default(value: Any) -> Any:
    if isinstance(value, memoryview):
        return bytes(value).hex()
    if isinstance(value, bytes):
        return value.hex()
    if hasattr(value, "to_dict"):
        return value.to_dict()
    raise TypeError(f"cannot JSON-encode payload value of type {type(value).__name__}")


@dataclass(frozen=True, slots=True)
class SubInterpreterPolicy:
    import_policy: str = "inherit_worker_paths"
    allowed_modules: tuple[str, ...] = ()
    extension_modules: str = "allow_validated_only"
    gil: str = "shared"

    @classmethod
    def from_context_metadata(cls, metadata: dict[str, Any]) -> "SubInterpreterPolicy":
        raw = metadata.get("subinterpreter_policy") or {}
        if not isinstance(raw, dict):
            return cls()
        return cls(
            import_policy=str(raw.get("import_policy", "inherit_worker_paths")),
            allowed_modules=tuple(str(module) for module in raw.get("allowed_modules", ()) if str(module)),
            extension_modules=str(raw.get("extension_modules", "allow_validated_only")),
            gil=str(raw.get("gil", "shared")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "import_policy": self.import_policy,
            "allowed_modules": list(self.allowed_modules),
            "extension_modules": self.extension_modules,
            "gil": self.gil,
        }

    def cache_key(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True)


@dataclass(slots=True)
class _SubInterpreterRuntime:
    interpreter: Any
    request_queue: Any
    response_queue: Any


@dataclass(slots=True)
class SubInterpreterExecutor:
    """Execute requests inside cached CPython sub-interpreters keyed by policy."""

    _runtimes: dict[str, _SubInterpreterRuntime] = field(default_factory=dict)

    @property
    def available(self) -> bool:
        return interpreters is not None

    def execute(self, payload: dict[str, Any], batch_id: int) -> tuple[ControlMessageKind, dict[str, Any]]:
        if interpreters is None:
            raise RuntimeError("sub-interpreter execution is unavailable in this Python runtime")
        context = dict(payload.get("context", {}))
        metadata = dict(context.get("metadata", {}))
        policy = SubInterpreterPolicy.from_context_metadata(metadata)
        metadata["subinterpreter_policy"] = policy.to_dict()
        context["metadata"] = metadata
        normalized_payload = dict(payload)
        normalized_payload["context"] = context

        runtime = self._ensure_ready(policy)
        runtime.request_queue.put(
            json.dumps({"payload": normalized_payload, "batch_id": batch_id}, default=_json_default)
        )
        runtime.interpreter.exec(_SUBINTERPRETER_RUNNER)
        response = json.loads(runtime.response_queue.get())
        payload_map = dict(response.get("payload", {}))
        if isinstance(payload_map.get("buffers"), list):
            payload_map["buffers"] = [
                bytes.fromhex(buffer) if isinstance(buffer, str) else buffer
                for buffer in payload_map["buffers"]
            ]
        kind = response.get("kind", "complete")
        if kind == "error":
            return ControlMessageKind.ERROR, payload_map
        return ControlMessageKind.COMPLETE, payload_map

    def _ensure_ready(self, policy: SubInterpreterPolicy) -> _SubInterpreterRuntime:
        key = policy.cache_key()
        cached = self._runtimes.get(key)
        if cached is not None:
            return cached
        assert interpreters is not None
        request_queue = interpreters.create_queue()
        response_queue = interpreters.create_queue()
        interpreter = interpreters.create()
        interpreter.prepare_main(
            _paro_request_queue=request_queue,
            _paro_response_queue=response_queue,
        )
        runtime = _SubInterpreterRuntime(
            interpreter=interpreter,
            request_queue=request_queue,
            response_queue=response_queue,
        )
        self._runtimes[key] = runtime
        return runtime
