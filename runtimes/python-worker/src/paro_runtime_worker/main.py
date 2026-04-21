"""Worker entrypoint for the Python external runtime."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import sys
from typing import Any, Iterable

from paro_runtime_worker.control import ControlLoop, WorkerResponse


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
    for line in sys.stdin:
        frame = line.strip()
        if not frame:
            continue
        request = json.loads(frame)
        response = runtime.process_frame(
            bytes.fromhex(request["header"]),
            request.get("payload"),
        )
        sys.stdout.write(
            json.dumps(
                {
                    "header": response.header.encode().hex(),
                    "payload": response.payload,
                },
                sort_keys=True,
                default=_json_default,
            )
            + "\n"
        )
        sys.stdout.flush()
    return 0


def _json_default(value: Any) -> Any:
    if isinstance(value, memoryview):
        return bytes(value).hex()
    if isinstance(value, bytes):
        return value.hex()
    if hasattr(value, "to_dict"):
        return value.to_dict()
    raise TypeError(f"cannot JSON-encode payload value of type {type(value).__name__}")
