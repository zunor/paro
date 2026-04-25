"""Worker entrypoint for the Python external runtime."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import struct
import sys
from typing import Any, Iterable

from paro_runtime_worker.control import ControlLoop, WorkerResponse
from paro_runtime_worker.protocol.control_header import ControlHeader

FRAME_MAGIC = b"PAROFRM1"
FRAME_TRAILER = struct.Struct("<II")
BUFFER_LEN = struct.Struct("<Q")

@dataclass(slots=True)
class WorkerRuntime:
    """Small embeddable worker runtime used by tests and future process wiring."""

    input_arena: dict[int, memoryview] = field(default_factory=dict)
    output_arena: dict[int, memoryview] = field(default_factory=dict)
    control_loop: ControlLoop = field(default_factory=ControlLoop)

    def map_input_buffer(self, index: int, buffer: bytes | bytearray | memoryview) -> None:
        self.input_arena[index] = memoryview(buffer)

    def map_output_buffer(self, index: int, buffer: bytes | bytearray | memoryview) -> None:
        self.output_arena[index] = memoryview(buffer)

    def process_frame(
        self,
        header: bytes,
        payload: bytes | bytearray | memoryview | dict[str, Any] | None = None,
    ) -> WorkerResponse:
        return self.control_loop.handle(header, payload)

    def run(
        self,
        frames: Iterable[
            tuple[bytes, bytes | bytearray | memoryview | dict[str, Any] | None]
        ] | None = None,
    ) -> list[WorkerResponse]:
        return self.control_loop.run(frames)


def main() -> int:
    runtime = WorkerRuntime()
    input_stream = sys.stdin.buffer
    output_stream = sys.stdout.buffer
    while True:
        frame = _read_frame(input_stream)
        if frame is None:
            break
        header, payload = frame
        response = runtime.process_frame(header, payload)
        _write_frame(output_stream, response)
    return 0


def _read_frame(stream) -> tuple[bytes, dict[str, Any]] | None:
    magic = _read_exact(stream, len(FRAME_MAGIC))
    if magic is None:
        return None
    if magic != FRAME_MAGIC:
        raise ValueError("invalid worker frame magic")

    header = _read_exact(stream, 32)
    payload_len, buffer_count = FRAME_TRAILER.unpack(
        _read_exact_required(stream, FRAME_TRAILER.size)
    )
    lengths = [
        BUFFER_LEN.unpack(_read_exact_required(stream, BUFFER_LEN.size))[0]
        for _ in range(buffer_count)
    ]
    payload_bytes = _read_exact_required(stream, payload_len)
    payload = json.loads(payload_bytes.decode("utf-8")) if payload_bytes else {}
    payload["buffers"] = [_read_exact_required(stream, length) for length in lengths]
    return header, payload


def _write_frame(stream, response: WorkerResponse) -> None:
    payload = dict(response.payload)
    buffers = [bytes(buffer) for buffer in payload.pop("buffers", ())]
    payload_bytes = json.dumps(
        payload,
        sort_keys=True,
        default=_json_default,
        separators=(",", ":"),
    ).encode("utf-8")
    header = ControlHeader.new(
        response.header.kind,
        batch_id=response.header.batch_id,
        lease_id=response.header.lease_id,
        payload_len=len(payload_bytes),
    ).encode()
    stream.write(FRAME_MAGIC)
    stream.write(header)
    stream.write(FRAME_TRAILER.pack(len(payload_bytes), len(buffers)))
    for buffer in buffers:
        stream.write(BUFFER_LEN.pack(len(buffer)))
    stream.write(payload_bytes)
    for buffer in buffers:
        stream.write(buffer)
    stream.flush()


def _read_exact(stream, length: int) -> bytes | None:
    payload = stream.read(length)
    if payload == b"" and length > 0:
        return None
    if len(payload) != length:
        raise EOFError("worker frame ended before expected bytes")
    return payload


def _read_exact_required(stream, length: int) -> bytes:
    payload = _read_exact(stream, length)
    if payload is None:
        raise EOFError("worker frame ended before expected bytes")
    return payload


def _json_default(value: Any) -> Any:
    if isinstance(value, memoryview):
        return bytes(value).hex()
    if isinstance(value, bytes):
        return value.hex()
    if hasattr(value, "to_dict"):
        return value.to_dict()
    raise TypeError(f"cannot JSON-encode payload value of type {type(value).__name__}")
