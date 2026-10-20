# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Context:
    """The immutable identity and selected row count of this handler call."""

    num_rows: int
    invocation_id: str
    routine_id: int
    generation: int
