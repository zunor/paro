"""Advanced encoding-preserving column views for performance-sensitive kernels."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence


@dataclass(frozen=True, slots=True)
class ConstantView:
    value: Any
    length: int

    def materialize(self, null_mask: Sequence[bool] | None = None) -> list[Any]:
        return [
            None if null_mask is not None and index < len(null_mask) and null_mask[index] else self.value
            for index in range(self.length)
        ]


@dataclass(frozen=True, slots=True)
class SequenceView:
    start: int | float
    step: int | float
    length: int

    def materialize(self, null_mask: Sequence[bool] | None = None) -> list[Any]:
        values: list[Any] = []
        current = self.start
        for index in range(self.length):
            values.append(
                None if null_mask is not None and index < len(null_mask) and null_mask[index] else current
            )
            current += self.step
        return values


@dataclass(frozen=True, slots=True)
class DictionaryView:
    indices: tuple[int, ...]
    dictionary: tuple[Any, ...]

    def materialize(self, null_mask: Sequence[bool] | None = None) -> list[Any]:
        values: list[Any] = []
        for index, dictionary_index in enumerate(self.indices):
            if null_mask is not None and index < len(null_mask) and null_mask[index]:
                values.append(None)
            else:
                values.append(self.dictionary[dictionary_index])
        return values


def as_constant_view(value: Any) -> ConstantView:
    if isinstance(value, ConstantView):
        return value
    if hasattr(value, "as_constant_view"):
        return value.as_constant_view()
    raise TypeError("value does not expose a constant column view")


def as_sequence_view(value: Any) -> SequenceView:
    if isinstance(value, SequenceView):
        return value
    if hasattr(value, "as_sequence_view"):
        return value.as_sequence_view()
    raise TypeError("value does not expose a sequence column view")


def as_dictionary_view(value: Any) -> DictionaryView:
    if isinstance(value, DictionaryView):
        return value
    if hasattr(value, "as_dictionary_view"):
        return value.as_dictionary_view()
    raise TypeError("value does not expose a dictionary column view")


def borrow_buffer(value: Any) -> memoryview:
    if hasattr(value, "borrow_buffer"):
        return value.borrow_buffer()
    return memoryview(value)

