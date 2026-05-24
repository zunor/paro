# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Performance gate outcome model."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from .entry import GateEntry, GateStatus


class GateEnforcement(str, Enum):
    SHADOW = "shadow"
    SOFT = "soft"
    HARD = "hard"


@dataclass(frozen=True)
class GateOutcome:
    gate: str
    enforcement: GateEnforcement
    entries: tuple[GateEntry, ...]
    archive_health: Any | None = None

    @property
    def failed(self) -> bool:
        return any(entry.status == GateStatus.REGRESS for entry in self.entries)

    @property
    def blocking_failed(self) -> bool:
        return self.enforcement == GateEnforcement.HARD and self.failed

    @property
    def has_entries(self) -> bool:
        return bool(self.entries)
