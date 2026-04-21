"""Encode Python UDF return values into Paro ABI descriptors and buffers."""

from __future__ import annotations

from dataclasses import dataclass
import struct
from typing import Any

from paro_udf.column import (
    Column,
    ConstantView,
    DictionaryView,
    FlatBufferStorage,
    ListStorage,
    SequenceView,
    StructStorage,
    VarLenStorage,
    coerce_result_columns,
    pack_validity_bitmap,
)
from paro_udf.types import encode_scalar_value


@dataclass(frozen=True, slots=True)
class EncodedBatch:
    row_count: int
    columns: tuple[dict[str, object], ...]
    buffers: tuple[memoryview, ...]
    payload_checksum: int | None

    def to_payload(
        self,
        *,
        lease_id: int,
        ownership: dict[str, int] | None = None,
        completion_fence: int = 0,
    ) -> dict[str, object]:
        return {
            "version": 1,
            "lease_id": lease_id,
            "row_count": self.row_count,
            "state": "Committed",
            "ownership": ownership
            or {
                "owner_worker_epoch": 0,
                "owner_host_epoch": 0,
                "owner_query_epoch": 0,
            },
            "completion_fence": completion_fence,
            "payload_checksum": self.payload_checksum,
            "columns": list(self.columns),
        }


class ColumnEncoder:
    def encode_result(
        self,
        result: Any,
        *,
        row_count: int | None,
        logical_types: object | tuple[object, ...] | None = None,
        output_names: tuple[str, ...] | None = None,
        returns_nullable: bool | tuple[bool, ...] = True,
    ) -> EncodedBatch:
        columns = coerce_result_columns(
            result,
            row_count=row_count,
            logical_types=logical_types,
            output_names=output_names,
            returns_nullable=returns_nullable,
        )
        return self.encode_columns(columns)

    def encode_columns(self, columns: list[Column] | tuple[Column, ...]) -> EncodedBatch:
        if columns:
            row_count = len(columns[0])
            for column in columns[1:]:
                if len(column) != row_count:
                    raise ValueError("all encoded output columns must share the same row count")
        else:
            row_count = 0
        buffer_accumulator: list[memoryview] = []
        descriptors = tuple(
            self._encode_descriptor(column, buffer_accumulator, column_name=column._name or f"c{index}")
            for index, column in enumerate(columns)
        )
        checksum = (
            sum(sum(view.cast("B")) for view in buffer_accumulator) % (1 << 32)
            if buffer_accumulator
            else None
        )
        return EncodedBatch(
            row_count=row_count,
            columns=descriptors,
            buffers=tuple(buffer_accumulator),
            payload_checksum=checksum,
        )

    def _append_buffer(
        self,
        buffers: list[memoryview],
        buffer: bytes | bytearray | memoryview,
        *,
        alignment: int,
    ) -> dict[str, object]:
        view = memoryview(buffer)
        buffer_index = len(buffers)
        buffers.append(view)
        return {
            "buffer_index": buffer_index,
            "offset": 0,
            "len": len(view),
            "alignment": alignment,
            "generation": 0,
            "device": "Host",
        }

    def _encode_descriptor(
        self,
        column: Column,
        buffers: list[memoryview],
        *,
        column_name: str,
    ) -> dict[str, object]:
        descriptor: dict[str, object] = {
            "name": column_name,
            "logical_type": column.logical_type_spec().to_abi_json(),
            "encoding": column.encoding.capitalize() if column.encoding != "flat" else "Flat",
            "population_mode": "Eager",
            "nullable": column.nullable,
            "validity": None,
            "layout": {},
            "children": [],
        }

        if any(column.null_mask):
            descriptor["validity"] = self._append_buffer(
                buffers,
                pack_validity_bitmap(column.null_mask),
                alignment=1,
            )

        storage = column._storage
        if isinstance(storage, FlatBufferStorage):
            descriptor["layout"] = {
                "FixedWidth": {
                    "values": self._append_buffer(
                        buffers,
                        storage.buffer,
                        alignment=max(storage.stride, 1),
                    ),
                    "stride": storage.stride,
                }
            }
            return descriptor

        if isinstance(storage, VarLenStorage):
            descriptor["layout"] = {
                "VarLen": {
                    "offsets": self._append_buffer(
                        buffers,
                        storage.offsets,
                        alignment=4 if storage.offset_width == "U32" else 8,
                    ),
                    "data": self._append_buffer(buffers, storage.data, alignment=1),
                    "offset_width": storage.offset_width,
                }
            }
            return descriptor

        if isinstance(storage, ConstantView):
            descriptor["layout"] = {
                "Constant": {
                    "value": encode_scalar_value(storage.value, column.logical_type_spec())
                }
            }
            return descriptor

        if isinstance(storage, SequenceView):
            descriptor["layout"] = {
                "Sequence": {
                    "start": storage.start,
                    "step": storage.step,
                }
            }
            return descriptor

        if isinstance(storage, DictionaryView):
            indices = bytearray(len(storage.indices) * 4)
            for index, dictionary_index in enumerate(storage.indices):
                struct.pack_into("<I", indices, index * 4, dictionary_index)
            descriptor["layout"] = {
                "Dictionary": {
                    "indices": self._append_buffer(buffers, indices, alignment=4),
                    "dictionary": self._encode_descriptor(
                        Column(
                            list(storage.dictionary),
                            logical_type=column.logical_type_spec(),
                            name=f"{column_name}_dictionary",
                        ),
                        buffers,
                        column_name=f"{column_name}_dictionary",
                    ),
                }
            }
            return descriptor

        if isinstance(storage, ListStorage):
            descriptor["layout"] = {
                "List": {
                    "offsets": self._append_buffer(
                        buffers,
                        storage.offsets,
                        alignment=4 if storage.offset_width == "U32" else 8,
                    ),
                    "offset_width": storage.offset_width,
                }
            }
            descriptor["children"] = [
                self._encode_descriptor(
                    storage.child,
                    buffers,
                    column_name=f"{column_name}_element",
                )
            ]
            return descriptor

        if isinstance(storage, StructStorage):
            descriptor["layout"] = {"Struct": {}}
            descriptor["children"] = [
                self._encode_descriptor(child, buffers, column_name=field.name)
                for child, field in zip(storage.children, column.logical_type_spec().fields, strict=True)
            ]
            return descriptor

        raise TypeError(f"unsupported column storage `{type(storage).__name__}`")
