"""Kernel-fusion payload helpers shared by worker components."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class KernelFusionMode(str, Enum):
    ROW_PRESERVING_CHAIN = "row_preserving_chain"


@dataclass(frozen=True, slots=True)
class KernelFusionPlan:
    mode: KernelFusionMode
    additional_handlers: tuple[str, ...]

    @classmethod
    def from_metadata(cls, metadata: dict[str, Any]) -> "KernelFusionPlan | None":
        raw = metadata.get("kernel_fusion")
        if raw is None:
            return None
        if isinstance(raw, (list, tuple)):
            return cls(
                mode=KernelFusionMode.ROW_PRESERVING_CHAIN,
                additional_handlers=tuple(str(item) for item in raw if str(item)),
            )
        if not isinstance(raw, dict):
            raise TypeError("kernel_fusion metadata must be a mapping or a sequence of handlers")
        mode = KernelFusionMode(raw.get("mode", KernelFusionMode.ROW_PRESERVING_CHAIN))
        handlers = tuple(str(item) for item in raw.get("handlers", ()) if str(item))
        return cls(mode=mode, additional_handlers=handlers)

    def to_metadata(self) -> dict[str, Any]:
        return {
            "mode": self.mode.value,
            "handlers": list(self.additional_handlers),
        }

    def full_chain(self, primary_handler: str) -> tuple[str, ...]:
        return (primary_handler, *self.additional_handlers)
