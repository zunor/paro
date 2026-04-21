"""Decode Paro shared-memory ABI descriptors into SDK column views."""

from __future__ import annotations

from dataclasses import dataclass
import struct
from typing import Any

from paro_udf.column import Column, unpack_validity_bitmap
from paro_udf.types import LogicalTypeError, decode_scalar_value, parse_logical_type


@dataclass(frozen=True, slots=True)
class DecodedBatch:
    lease_id: int
    row_count: int
    columns: tuple[Column, ...]


def _slice_buffer(buffers, lease: dict[str, Any]) -> memoryview:
    buffer_index = int(lease["buffer_index"])
    offset = int(lease["offset"])
    length = int(lease["len"])
    try:
        source = buffers[buffer_index]
    except IndexError as exc:
        raise LogicalTypeError(f"buffer index {buffer_index} is out of range") from exc
    view = memoryview(source)
    if offset + length > len(view):
        raise LogicalTypeError(
            f"lease slice [{offset}, {offset + length}) exceeds buffer {buffer_index} length {len(view)}"
        )
    return view[offset : offset + length]


def _decode_offsets(buffer: memoryview, offset_width: str, row_count: int) -> tuple[int, ...]:
    normalized = offset_width.upper()
    packer = struct.Struct("<I" if normalized == "U32" else "<Q")
    expected = (row_count + 1) * packer.size
    if len(buffer) != expected:
        raise LogicalTypeError(
            f"offset buffer has {len(buffer)} bytes, expected {expected} for {row_count} rows"
        )
    return tuple(
        packer.unpack_from(buffer, index * packer.size)[0] for index in range(row_count + 1)
    )


def _lease_state(lease: dict[str, Any]) -> str:
    return str(lease.get("state", "Committed"))


def _lease_ownership(lease: dict[str, Any]) -> dict[str, int]:
    ownership = lease.get("ownership")
    if isinstance(ownership, dict):
        return {
            "owner_worker_epoch": int(ownership.get("owner_worker_epoch", 0)),
            "owner_host_epoch": int(ownership.get("owner_host_epoch", 0)),
            "owner_query_epoch": int(ownership.get("owner_query_epoch", 0)),
        }
    return {
        "owner_worker_epoch": int(lease.get("owner_worker_epoch", 0)),
        "owner_host_epoch": int(lease.get("owner_host_epoch", 0)),
        "owner_query_epoch": int(lease.get("owner_query_epoch", 0)),
    }


class ColumnDecoder:
    def decode_batch(
        self,
        lease: dict[str, Any],
        buffers,
        *,
        expected_host_epoch: int | None = None,
        expected_query_epoch: int | None = None,
    ) -> DecodedBatch:
        state = _lease_state(lease)
        if state != "Committed":
            raise LogicalTypeError(f"lease {lease.get('lease_id')} is not committed: {state}")

        ownership = _lease_ownership(lease)
        if expected_host_epoch is not None and ownership["owner_host_epoch"] != expected_host_epoch:
            raise LogicalTypeError(
                f"lease host epoch mismatch: expected {expected_host_epoch}, got {ownership['owner_host_epoch']}"
            )
        if expected_query_epoch is not None and ownership["owner_query_epoch"] != expected_query_epoch:
            raise LogicalTypeError(
                f"lease query epoch mismatch: expected {expected_query_epoch}, got {ownership['owner_query_epoch']}"
            )

        row_count = int(lease["row_count"])
        columns = tuple(
            self.decode(descriptor, buffers, row_count=row_count)
            for descriptor in lease.get("columns", [])
        )
        return DecodedBatch(lease_id=int(lease["lease_id"]), row_count=row_count, columns=columns)

    def decode(self, descriptor: dict[str, Any], buffers, *, row_count: int) -> Column:
        logical_type = parse_logical_type(descriptor["logical_type"])
        layout = descriptor.get("layout", {})
        if not isinstance(layout, dict) or len(layout) != 1:
            raise LogicalTypeError(f"invalid column layout `{layout!r}`")
        layout_kind, payload = next(iter(layout.items()))
        null_mask = None
        if descriptor.get("nullable") and descriptor.get("validity") is not None:
            null_mask = unpack_validity_bitmap(
                _slice_buffer(buffers, descriptor["validity"]),
                row_count,
            )

        if layout_kind == "FixedWidth":
            return Column.from_buffer(
                _slice_buffer(buffers, payload["values"]),
                logical_type,
                length=row_count,
                name=descriptor.get("name"),
                null_mask=null_mask,
                nullable=bool(descriptor.get("nullable", False)),
            )

        if layout_kind == "VarLen":
            return Column.from_varlen_buffers(
                _slice_buffer(buffers, payload["offsets"]),
                _slice_buffer(buffers, payload["data"]),
                logical_type,
                length=row_count,
                offset_width=str(payload.get("offset_width", "U32")),
                name=descriptor.get("name"),
                null_mask=null_mask,
                nullable=bool(descriptor.get("nullable", False)),
            )

        if layout_kind == "Constant":
            return Column.from_constant(
                decode_scalar_value(payload["value"]),
                logical_type,
                length=row_count,
                name=descriptor.get("name"),
                null_mask=null_mask,
                nullable=bool(descriptor.get("nullable", False)),
            )

        if layout_kind == "Sequence":
            return Column.from_sequence(
                payload["start"],
                payload["step"],
                logical_type,
                length=row_count,
                name=descriptor.get("name"),
                null_mask=null_mask,
                nullable=bool(descriptor.get("nullable", False)),
            )

        if layout_kind == "Dictionary":
            indices_view = _slice_buffer(buffers, payload["indices"])
            if len(indices_view) % 4 != 0:
                raise LogicalTypeError("dictionary indices buffer must use 4-byte little-endian integers")
            indices = tuple(
                struct.unpack_from("<I", indices_view, index * 4)[0]
                for index in range(len(indices_view) // 4)
            )
            dictionary_descriptor = payload["dictionary"]
            dictionary_rows = self._derive_row_count(dictionary_descriptor, buffers)
            dictionary_column = self.decode(dictionary_descriptor, buffers, row_count=dictionary_rows)
            return Column.from_dictionary(
                indices,
                dictionary_column._materialize_values(),
                logical_type=logical_type,
                name=descriptor.get("name"),
                null_mask=null_mask,
                nullable=bool(descriptor.get("nullable", False)),
            )

        if layout_kind == "List":
            if len(descriptor.get("children", [])) != 1:
                raise LogicalTypeError("list descriptors require exactly one child column")
            offsets_view = _slice_buffer(buffers, payload["offsets"])
            offsets = _decode_offsets(offsets_view, str(payload.get("offset_width", "U32")), row_count)
            child_row_count = offsets[-1] if offsets else 0
            child = self.decode(descriptor["children"][0], buffers, row_count=child_row_count)
            return Column.from_list_storage(
                offsets_view,
                child,
                logical_type=logical_type,
                length=row_count,
                offset_width=str(payload.get("offset_width", "U32")),
                name=descriptor.get("name"),
                null_mask=null_mask,
                nullable=bool(descriptor.get("nullable", False)),
            )

        if layout_kind == "Struct":
            children = tuple(
                self.decode(child, buffers, row_count=row_count)
                for child in descriptor.get("children", [])
            )
            return Column.from_struct_children(
                children,
                logical_type=logical_type,
                length=row_count,
                name=descriptor.get("name"),
                null_mask=null_mask,
                nullable=bool(descriptor.get("nullable", False)),
            )

        raise LogicalTypeError(f"unsupported column layout `{layout_kind}`")

    def _derive_row_count(self, descriptor: dict[str, Any], buffers) -> int:
        layout = descriptor.get("layout", {})
        if not isinstance(layout, dict) or len(layout) != 1:
            raise LogicalTypeError("dictionary child descriptor must contain exactly one layout")
        layout_kind, payload = next(iter(layout.items()))
        if layout_kind == "FixedWidth":
            stride = int(payload["stride"])
            values = _slice_buffer(buffers, payload["values"])
            return len(values) // stride
        if layout_kind in {"VarLen", "List"}:
            offsets = _slice_buffer(buffers, payload["offsets"])
            width = 4 if str(payload.get("offset_width", "U32")).upper() == "U32" else 8
            return len(offsets) // width - 1
        if layout_kind in {"Constant", "Sequence"}:
            raise LogicalTypeError(
                f"cannot infer dictionary child row count from layout `{layout_kind}` without an explicit row count"
            )
        if layout_kind == "Struct":
            children = descriptor.get("children", [])
            if not children:
                return 0
            return self._derive_row_count(children[0], buffers)
        if layout_kind == "Dictionary":
            indices = _slice_buffer(buffers, payload["indices"])
            return len(indices) // 4
        raise LogicalTypeError(f"unsupported layout `{layout_kind}` for row-count inference")
