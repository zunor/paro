"""Batch execution context visible to Python UDF handlers."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any


@dataclass(frozen=True, slots=True)
class BatchContext:
    batch_id: int = 0
    query_id: int | None = None
    routine_identity: str | None = None
    capability_profile: str | None = None
    execution_backend: str | None = None
    output_row_hint: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def with_metadata(self, **items: Any) -> "BatchContext":
        merged = dict(self.metadata)
        merged.update(items)
        return replace(self, metadata=merged)

    def require_capability(self, capability: str) -> None:
        profile = self.capability_profile or ""
        if capability not in profile:
            raise PermissionError(
                f"batch context requires capability `{capability}`, current profile `{profile or 'unset'}`"
            )
