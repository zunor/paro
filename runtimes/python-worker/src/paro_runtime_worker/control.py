"""Control-plane loop used by the Python worker runtime."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import json
from typing import Any, Iterable

from paro_runtime_worker.column_decoder import ColumnDecoder
from paro_runtime_worker.column_encoder import ColumnEncoder
from paro_runtime_worker.module_loader import ModuleLoader, ModuleRequest
from paro_runtime_worker.protocol.control_header import ControlHeader, ControlMessageKind
from paro_runtime_worker.protocol.fusion import KernelFusionPlan
from paro_runtime_worker.protocol.sideband import PythonTracebackPayload
from paro_runtime_worker.trusted_lane import SubInterpreterExecutor
from paro_udf.context import BatchContext


class SubmissionState(str, Enum):
    SUBMITTED = "Submitted"
    STARTED = "Started"
    FINISHED = "Finished"
    FAILED = "Failed"
    CANCELLED = "Cancelled"


@dataclass(slots=True)
class SubmissionLifecycle:
    state: SubmissionState = SubmissionState.SUBMITTED
    retry_count: int = 0

    def transition_to(self, next_state: SubmissionState) -> None:
        allowed = {
            SubmissionState.SUBMITTED: {SubmissionState.STARTED, SubmissionState.CANCELLED, SubmissionState.FAILED},
            SubmissionState.STARTED: {SubmissionState.FINISHED, SubmissionState.FAILED, SubmissionState.CANCELLED},
            SubmissionState.FINISHED: set(),
            SubmissionState.FAILED: set(),
            SubmissionState.CANCELLED: set(),
        }
        if next_state not in allowed[self.state]:
            raise RuntimeError(f"invalid submission transition {self.state.value} -> {next_state.value}")
        self.state = next_state


@dataclass(frozen=True, slots=True)
class SubmissionRequest:
    module_request: ModuleRequest
    lease: dict[str, Any]
    buffers: tuple[bytes | bytearray | memoryview, ...]
    search_paths: tuple[str, ...]
    context: BatchContext
    expected_row_count: int | None
    return_types: tuple[object, ...] | None
    output_names: tuple[str, ...] | None
    returns_nullable: bool | tuple[bool, ...]
    ownership: dict[str, int] | None
    completion_fence: int
    kernel_fusion: KernelFusionPlan | None

    @classmethod
    def from_payload(cls, payload: dict[str, Any], batch_id: int) -> "SubmissionRequest":
        module_request = ModuleRequest(
            module_name=payload.get("module_name"),
            module_path=payload.get("module_path"),
            handler=payload.get("handler", "main"),
            cache_key=payload.get("cache_key"),
        )
        context_payload = dict(payload.get("context", {}))
        context = BatchContext(
            batch_id=batch_id,
            query_id=context_payload.get("query_id"),
            routine_identity=context_payload.get("routine_identity"),
            capability_profile=context_payload.get("capability_profile"),
            execution_backend=context_payload.get("execution_backend"),
            output_row_hint=context_payload.get("output_row_hint"),
            metadata=dict(context_payload.get("metadata", {})),
        )
        kernel_fusion = KernelFusionPlan.from_metadata(context.metadata)
        raw_return_types = payload.get("return_types")
        if raw_return_types is None and payload.get("return_type") is not None:
            raw_return_types = (payload["return_type"],)
        elif raw_return_types is not None:
            raw_return_types = tuple(raw_return_types)
        return cls(
            module_request=module_request,
            lease=dict(payload["lease"]),
            buffers=tuple(_normalize_buffers(payload.get("buffers", ()))),
            search_paths=tuple(payload.get("search_paths", ())),
            context=context,
            expected_row_count=payload.get("expected_row_count"),
            return_types=raw_return_types,
            output_names=tuple(payload["output_names"]) if payload.get("output_names") is not None else None,
            returns_nullable=payload.get("returns_nullable", True),
            ownership=payload.get("ownership"),
            completion_fence=int(payload.get("completion_fence", 0)),
            kernel_fusion=kernel_fusion,
        )


@dataclass(frozen=True, slots=True)
class WorkerResponse:
    header: ControlHeader
    payload: dict[str, Any]
    lifecycle: SubmissionLifecycle

    @property
    def kind(self) -> ControlMessageKind:
        return self.header.kind


def _payload_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, default=_json_default).encode("utf-8")


def _json_default(value: Any) -> Any:
    if isinstance(value, memoryview):
        return bytes(value).hex()
    if isinstance(value, bytes):
        return value.hex()
    if hasattr(value, "to_dict"):
        return value.to_dict()
    raise TypeError(f"cannot JSON-encode payload value of type {type(value).__name__}")


def _normalize_buffers(buffers) -> list[bytes | bytearray | memoryview]:
    normalized = []
    for buffer in buffers:
        if isinstance(buffer, str):
            normalized.append(bytes.fromhex(buffer))
        else:
            normalized.append(buffer)
    return normalized


def _decode_payload(payload: bytes | bytearray | memoryview | dict[str, Any] | None, expected_len: int) -> dict[str, Any]:
    if payload is None:
        return {}
    if isinstance(payload, dict):
        return payload
    raw = bytes(payload)
    if len(raw) != expected_len:
        raise ValueError(f"payload length {len(raw)} does not match control header length {expected_len}")
    if not raw:
        return {}
    return json.loads(raw.decode("utf-8"))


@dataclass(slots=True)
class ControlLoop:
    """Single-active-batch worker control loop with structured completion/error payloads."""

    decoder: ColumnDecoder = field(default_factory=ColumnDecoder)
    encoder: ColumnEncoder = field(default_factory=ColumnEncoder)
    module_loader: ModuleLoader = field(default_factory=ModuleLoader)
    subinterpreter: SubInterpreterExecutor = field(default_factory=SubInterpreterExecutor)
    processed: list[ControlMessageKind] = field(default_factory=list)
    lifecycles: dict[int, SubmissionLifecycle] = field(default_factory=dict)
    cancelled_batches: set[int] = field(default_factory=set)

    def handle(
        self,
        header_payload: bytes,
        payload: bytes | bytearray | memoryview | dict[str, Any] | None = None,
    ) -> WorkerResponse:
        header = ControlHeader.decode(header_payload)
        payload_map = _decode_payload(payload, header.payload_len)
        self.processed.append(header.kind)
        if header.kind == ControlMessageKind.SUBMIT:
            return self._handle_submit(header, payload_map)
        if header.kind == ControlMessageKind.CANCEL:
            return self._handle_cancel(header)
        return self._ack(
            kind=ControlMessageKind.COMPLETE,
            batch_id=header.batch_id,
            lease_id=header.lease_id,
            payload={"state": header.kind.name.title()},
            lifecycle=self.lifecycles.get(header.batch_id, SubmissionLifecycle()),
        )

    def run(
        self,
        frames: Iterable[
            tuple[bytes, bytes | bytearray | memoryview | dict[str, Any] | None]
        ] | None = None,
    ) -> list[WorkerResponse]:
        if frames is None:
            return []
        return [self.handle(header, payload) for header, payload in frames]

    def _handle_cancel(self, header: ControlHeader) -> WorkerResponse:
        lifecycle = self.lifecycles.get(header.batch_id)
        if lifecycle is None:
            lifecycle = SubmissionLifecycle(state=SubmissionState.CANCELLED)
            self.lifecycles[header.batch_id] = lifecycle
            self.cancelled_batches.add(header.batch_id)
        elif lifecycle.state == SubmissionState.SUBMITTED:
            lifecycle.transition_to(SubmissionState.CANCELLED)
        elif lifecycle.state == SubmissionState.STARTED:
            lifecycle.transition_to(SubmissionState.CANCELLED)
        else:
            self.cancelled_batches.add(header.batch_id)
            lifecycle = SubmissionLifecycle(state=SubmissionState.CANCELLED)
            self.lifecycles[header.batch_id] = lifecycle
        return self._ack(
            kind=ControlMessageKind.COMPLETE,
            batch_id=header.batch_id,
            lease_id=header.lease_id,
            payload={"state": SubmissionState.CANCELLED.value, "batch_id": header.batch_id},
            lifecycle=lifecycle,
        )

    def _handle_submit(self, header: ControlHeader, payload: dict[str, Any]) -> WorkerResponse:
        lifecycle = self.lifecycles.setdefault(header.batch_id, SubmissionLifecycle())
        if header.batch_id in self.cancelled_batches or lifecycle.state == SubmissionState.CANCELLED:
            if lifecycle.state == SubmissionState.SUBMITTED:
                lifecycle.transition_to(SubmissionState.CANCELLED)
            return self._ack(
                kind=ControlMessageKind.COMPLETE,
                batch_id=header.batch_id,
                lease_id=header.lease_id,
                payload={"state": SubmissionState.CANCELLED.value, "batch_id": header.batch_id},
                lifecycle=lifecycle,
            )

        request = SubmissionRequest.from_payload(payload, header.batch_id)
        lifecycle.transition_to(SubmissionState.STARTED)

        try:
            if request.context.execution_backend == "subinterpreter":
                return self._handle_submit_via_subinterpreter(header, payload, lifecycle)
            decoded = self.decoder.decode_batch(
                request.lease,
                request.buffers,
                expected_host_epoch=request.ownership.get("owner_host_epoch") if request.ownership else None,
                expected_query_epoch=request.ownership.get("owner_query_epoch") if request.ownership else None,
            )
            self.module_loader.ensure_search_paths(request.search_paths)
            additional_handlers = (
                request.kernel_fusion.additional_handlers if request.kernel_fusion is not None else ()
            )
            resolved_chain = self.module_loader.load_execution_chain(
                request.module_request,
                execution_backend=request.context.execution_backend or "process",
                compiled_kernel_kind=request.context.metadata.get("compiled_kernel_kind"),
                additional_handlers=additional_handlers,
            )
            if len(resolved_chain) > 1:
                validated_columns = self._execute_fused_chain(
                    request,
                    decoded.columns,
                    resolved_chain,
                )
                encoded = self.encoder.encode_columns(validated_columns)
            else:
                resolved = resolved_chain[0]
                result = resolved.target(request.context, *decoded.columns)

                if resolved.validator is not None:
                    validated_columns = resolved.validator(
                        result,
                        request.expected_row_count,
                        output_names=request.output_names,
                    )
                    encoded = self.encoder.encode_columns(validated_columns)
                else:
                    encoded = self.encoder.encode_result(
                        result,
                        row_count=request.expected_row_count,
                        logical_types=request.return_types,
                        output_names=request.output_names,
                        returns_nullable=request.returns_nullable,
                    )

            lifecycle.transition_to(SubmissionState.FINISHED)
            payload = {
                "state": SubmissionState.FINISHED.value,
                "lease": encoded.to_payload(
                    lease_id=header.lease_id,
                    ownership=request.ownership,
                    completion_fence=request.completion_fence,
                ),
                "buffers": list(encoded.buffers),
            }
            return self._ack(
                kind=ControlMessageKind.COMPLETE,
                batch_id=header.batch_id,
                lease_id=header.lease_id,
                payload=payload,
                lifecycle=lifecycle,
            )
        except Exception as exc:
            lifecycle.transition_to(SubmissionState.FAILED)
            traceback_payload = PythonTracebackPayload.from_exception(
                exc,
                module=request.module_request.module_name
                or request.module_request.module_path
                or "<inline>",
                handler=request.module_request.handler,
                batch_id=header.batch_id,
            )
            return self._ack(
                kind=ControlMessageKind.ERROR,
                batch_id=header.batch_id,
                lease_id=header.lease_id,
                payload=traceback_payload.to_dict(),
                lifecycle=lifecycle,
            )

    def _execute_fused_chain(
        self,
        request: SubmissionRequest,
        columns,
        resolved_chain,
    ):
        current_columns = tuple(columns)
        current_row_count = len(current_columns[0]) if current_columns else request.expected_row_count
        for index, resolved in enumerate(resolved_chain):
            result = resolved.target(request.context, *current_columns)
            if resolved.validator is None:
                raise TypeError("kernel fusion requires handlers declared with @batch_udf")
            is_last = index == len(resolved_chain) - 1
            current_columns = tuple(
                resolved.validator(
                    result,
                    request.expected_row_count if is_last else current_row_count,
                    output_names=request.output_names if is_last else None,
                )
            )
            current_row_count = len(current_columns[0]) if current_columns else 0
        return list(current_columns)

    def _handle_submit_via_subinterpreter(
        self,
        header: ControlHeader,
        payload: dict[str, Any],
        lifecycle: SubmissionLifecycle,
    ) -> WorkerResponse:
        kind, response_payload = self.subinterpreter.execute(payload, header.batch_id)
        if kind == ControlMessageKind.ERROR:
            lifecycle.transition_to(SubmissionState.FAILED)
            return self._ack(
                kind=ControlMessageKind.ERROR,
                batch_id=header.batch_id,
                lease_id=header.lease_id,
                payload=response_payload,
                lifecycle=lifecycle,
            )
        lifecycle.transition_to(SubmissionState.FINISHED)
        return self._ack(
            kind=ControlMessageKind.COMPLETE,
            batch_id=header.batch_id,
            lease_id=header.lease_id,
            payload=response_payload,
            lifecycle=lifecycle,
        )

    def _ack(
        self,
        *,
        kind: ControlMessageKind,
        batch_id: int,
        lease_id: int,
        payload: dict[str, Any],
        lifecycle: SubmissionLifecycle,
    ) -> WorkerResponse:
        payload_bytes = _payload_bytes(payload)
        header = ControlHeader.new(
            kind,
            batch_id=batch_id,
            lease_id=lease_id,
            payload_len=len(payload_bytes),
        )
        return WorkerResponse(header=header, payload=payload, lifecycle=lifecycle)
