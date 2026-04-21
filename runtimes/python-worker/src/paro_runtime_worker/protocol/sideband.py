"""Helpers for sideband schemas and structured error payloads."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
import traceback


_ROOT = Path(__file__).resolve().parents[5] / "runtimes" / "protocol" / "sideband"
_MAX_MESSAGE_CHARS = 2_048
_MAX_TRACEBACK_CHARS = 16_384
_TRUNCATION_MARKER = "\n... [truncated] ...\n"


class SidebandSchemaKind(str, Enum):
    ARTIFACT = "artifact.fbs"
    DATA_PLANE = "data_plane.fbs"
    ERROR = "error.fbs"


def schema_dir() -> Path:
    return _ROOT


def schema_path(name: str | SidebandSchemaKind) -> Path:
    return _ROOT / (name.value if isinstance(name, SidebandSchemaKind) else name)


def load_schema(name: str | SidebandSchemaKind) -> str:
    return schema_path(name).read_text(encoding="utf-8")


@dataclass(frozen=True, slots=True)
class PythonTracebackPayload:
    exception_type: str
    message: str
    formatted_traceback: str
    module: str
    handler: str
    batch_id: int
    truncated: bool = False

    @classmethod
    def from_exception(
        cls,
        exc: BaseException,
        *,
        module: str,
        handler: str,
        batch_id: int,
    ) -> "PythonTracebackPayload":
        message, message_truncated = _truncate_middle(str(exc), _MAX_MESSAGE_CHARS)
        formatted = "".join(
            traceback.format_exception(type(exc), exc, exc.__traceback__)
        ).rstrip()
        formatted, traceback_truncated = _truncate_middle(formatted, _MAX_TRACEBACK_CHARS)
        return cls(
            exception_type=type(exc).__name__,
            message=message,
            formatted_traceback=formatted,
            module=module,
            handler=handler,
            batch_id=batch_id,
            truncated=message_truncated or traceback_truncated,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "exception_type": self.exception_type,
            "message": self.message,
            "formatted_traceback": self.formatted_traceback,
            "module": self.module,
            "handler": self.handler,
            "batch_id": self.batch_id,
            "truncated": self.truncated,
        }


def _truncate_middle(text: str, max_chars: int) -> tuple[str, bool]:
    if max_chars <= 0 or len(text) <= max_chars:
        return text, False
    available = max_chars - len(_TRUNCATION_MARKER)
    if available <= 0:
        return _TRUNCATION_MARKER[:max_chars], True
    head = available // 2
    tail = available - head
    return (
        f"{text[:head]}{_TRUNCATION_MARKER}{text[-tail:]}",
        True,
    )
