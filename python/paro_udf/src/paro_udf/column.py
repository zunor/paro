"""Column abstraction for Paro Python UDFs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
import importlib
import struct
import warnings
from typing import Any

from paro_udf.advanced.views import ConstantView, DictionaryView, SequenceView
from paro_udf.types import (
    ColumnProtocol,
    LogicalTypeError,
    LogicalTypeSpec,
    decode_scalar_value,
    encode_scalar_value,
    ensure_logical_type,
    infer_logical_type,
    parse_logical_type,
)


class SlowPathWarning(UserWarning):
    """Signals an explicit fallback to Python object materialization."""


class ResultContractError(ValueError):
    """Raised when a Python handler returns a value that violates the Paro contract."""


@dataclass(frozen=True, slots=True)
class FlatBufferStorage:
    buffer: memoryview
    stride: int


@dataclass(frozen=True, slots=True)
class VarLenStorage:
    offsets: memoryview
    data: memoryview
    offset_width: str


@dataclass(frozen=True, slots=True)
class ListStorage:
    offsets: memoryview
    child: "Column"
    offset_width: str


@dataclass(frozen=True, slots=True)
class StructStorage:
    children: tuple["Column", ...]


@dataclass(frozen=True, slots=True)
class ArrowArrayExport:
    """Lightweight Arrow/PyCapsule-compatible export wrapper."""

    source: "Column"
    requested_schema: object | None = None

    @property
    def logical_type(self) -> str:
        return self.source.logical_type

    @property
    def encoding(self) -> str:
        return self.source.encoding

    @property
    def length(self) -> int:
        return len(self.source)

    @property
    def null_count(self) -> int:
        return sum(1 for is_null in self.source.null_mask if is_null)

    def materialize_py(self) -> list[Any]:
        return self.source._materialize_values()

    def buffer_views(self) -> dict[str, memoryview]:
        return self.source.arrow_buffer_views()


_NUMPY_DTYPES = {
    "boolean": "bool",
    "int8": "int8",
    "uint8": "uint8",
    "int16": "int16",
    "uint16": "uint16",
    "int32": "int32",
    "uint32": "uint32",
    "date": "int32",
    "float32": "float32",
    "int64": "int64",
    "uint64": "uint64",
    "time": "int64",
    "timestamp": "int64",
    "timestamptz": "int64",
    "float64": "float64",
    "decimal": "int64",
}


def _load_optional_module(name: str) -> Any:
    try:
        return importlib.import_module(name)
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            f"{name} is required for this Paro UDF fast path; install the optional dependency first"
        ) from exc


def pack_validity_bitmap(null_mask: Sequence[bool]) -> bytes:
    if not null_mask:
        return b""
    bitmap = bytearray((len(null_mask) + 7) // 8)
    for index, is_null in enumerate(null_mask):
        if not is_null:
            bitmap[index // 8] |= 1 << (index % 8)
    return bytes(bitmap)


def unpack_validity_bitmap(buffer: bytes | bytearray | memoryview, length: int) -> tuple[bool, ...]:
    if length == 0:
        return ()
    raw = bytes(buffer)
    return tuple(
        not bool(raw[index // 8] & (1 << (index % 8))) for index in range(length)
    )


def _normalize_null_mask(null_mask: Sequence[bool] | None, length: int) -> tuple[bool, ...]:
    if null_mask is None:
        return tuple(False for _ in range(length))
    if len(null_mask) != length:
        raise ResultContractError(
            f"null mask length {len(null_mask)} does not match column length {length}"
        )
    return tuple(bool(item) for item in null_mask)


def _offset_width_bytes(offset_width: str) -> int:
    normalized = offset_width.upper()
    if normalized == "U32":
        return 4
    if normalized == "U64":
        return 8
    raise LogicalTypeError(f"unsupported offset width `{offset_width}`")


def _offset_struct(offset_width: str) -> struct.Struct:
    width = _offset_width_bytes(offset_width)
    return struct.Struct("<I" if width == 4 else "<Q")


def _pack_offsets(offsets: Sequence[int], offset_width: str) -> memoryview:
    packer = _offset_struct(offset_width)
    buffer = bytearray(len(offsets) * packer.size)
    for index, value in enumerate(offsets):
        packer.pack_into(buffer, index * packer.size, int(value))
    return memoryview(buffer)


def _read_offsets(offsets: memoryview, offset_width: str, length: int) -> tuple[int, ...]:
    packer = _offset_struct(offset_width)
    expected = (length + 1) * packer.size
    if len(offsets) != expected:
        raise LogicalTypeError(
            f"offset buffer has {len(offsets)} bytes, expected {expected} for {length} rows"
        )
    return tuple(
        packer.unpack_from(offsets, index * packer.size)[0]
        for index in range(length + 1)
    )


def _fixed_struct(spec: LogicalTypeSpec) -> struct.Struct:
    fmt = spec.struct_format()
    if fmt is None:
        raise LogicalTypeError(
            f"logical type `{spec.canonical_name()}` does not have a fixed-width scalar format"
        )
    return struct.Struct("<" + fmt)


def _coerce_scalar(value: Any, spec: LogicalTypeSpec) -> Any:
    if value is None:
        return 0
    if spec.kind == "boolean":
        return 1 if value else 0
    if spec.kind == "decimal":
        decimal_value = Decimal(value)
        return int(decimal_value.scaleb(spec.scale or 0))
    return value


def _pack_fixed_width(values: Sequence[Any], spec: LogicalTypeSpec) -> FlatBufferStorage:
    stride = spec.fixed_width_bytes()
    if stride is None:
        raise LogicalTypeError(
            f"logical type `{spec.canonical_name()}` is not fixed-width and cannot use to_numpy()"
        )
    packer = _fixed_struct(spec)
    buffer = bytearray(len(values) * stride)
    for index, value in enumerate(values):
        packer.pack_into(buffer, index * stride, _coerce_scalar(value, spec))
    return FlatBufferStorage(memoryview(buffer), stride)


def _materialize_fixed_width(storage: FlatBufferStorage, spec: LogicalTypeSpec, length: int) -> list[Any]:
    packer = _fixed_struct(spec)
    values: list[Any] = []
    for index in range(length):
        raw = packer.unpack_from(storage.buffer, index * storage.stride)[0]
        if spec.kind == "boolean":
            values.append(bool(raw))
        elif spec.kind == "decimal":
            scale = spec.scale or 0
            values.append(Decimal(raw).scaleb(-scale))
        else:
            values.append(raw)
    return values


def _build_varlen_storage(values: Sequence[Any], spec: LogicalTypeSpec, offset_width: str) -> VarLenStorage:
    offsets = [0]
    payload = bytearray()
    for value in values:
        if value is None:
            raw = b""
        elif spec.kind in {"varchar", "json"}:
            raw = str(value).encode("utf-8")
        else:
            raw = bytes(value)
        payload.extend(raw)
        offsets.append(len(payload))
    return VarLenStorage(
        offsets=_pack_offsets(offsets, offset_width),
        data=memoryview(payload),
        offset_width=offset_width,
    )


def _materialize_varlen(storage: VarLenStorage, spec: LogicalTypeSpec, length: int) -> list[Any]:
    offsets = _read_offsets(storage.offsets, storage.offset_width, length)
    values: list[Any] = []
    for index in range(length):
        chunk = bytes(storage.data[offsets[index] : offsets[index + 1]])
        if spec.kind in {"varchar", "json"}:
            values.append(chunk.decode("utf-8"))
        else:
            values.append(chunk)
    return values


def _flatten_list_values(rows: Sequence[Any], child_type: LogicalTypeSpec, offset_width: str) -> ListStorage:
    flattened: list[Any] = []
    offsets = [0]
    for row in rows:
        nested = [] if row is None else list(row)
        flattened.extend(nested)
        offsets.append(len(flattened))
    child = Column(flattened, logical_type=child_type)
    return ListStorage(_pack_offsets(offsets, offset_width), child, offset_width)


def _materialize_list(storage: ListStorage, length: int) -> list[Any]:
    offsets = _read_offsets(storage.offsets, storage.offset_width, length)
    child_values = storage.child._materialize_values()
    return [child_values[offsets[index] : offsets[index + 1]] for index in range(length)]


def _build_struct_storage(rows: Sequence[Mapping[str, Any]], spec: LogicalTypeSpec) -> StructStorage:
    children = []
    for field in spec.fields:
        child_values = [row.get(field.name) if row is not None else None for row in rows]
        children.append(
            Column(
                child_values,
                logical_type=field.data_type,
                name=field.name,
                nullable=field.nullable,
            )
        )
    return StructStorage(tuple(children))


def _materialize_struct(storage: StructStorage, spec: LogicalTypeSpec, length: int) -> list[dict[str, Any]]:
    child_values = [child._materialize_values() for child in storage.children]
    rows: list[dict[str, Any]] = []
    for index in range(length):
        row = {}
        for field, values in zip(spec.fields, child_values, strict=True):
            row[field.name] = values[index]
        rows.append(row)
    return rows


def _first_non_null(values: Sequence[Any]) -> Any:
    return next((value for value in values if value is not None), None)


class Column(ColumnProtocol):
    """Batch-oriented Paro column view with explicit fast and slow paths."""

    def __init__(
        self,
        data: Any,
        logical_type: object | None = None,
        encoding: str = "flat",
        null_mask: Sequence[bool] | None = None,
        *,
        name: str | None = None,
        length: int | None = None,
        nullable: bool | None = None,
        offset_width: str = "U32",
    ) -> None:
        spec = ensure_logical_type(logical_type, data)
        normalized_encoding = encoding.lower()
        storage, inferred_length = self._coerce_storage(
            data,
            spec,
            normalized_encoding,
            length=length,
            offset_width=offset_width,
        )
        self._logical_type = spec
        self._encoding = normalized_encoding
        self._storage = storage
        self._length = inferred_length if length is None else length
        self._name = name or ""
        self._null_mask = _normalize_null_mask(null_mask, self._length)
        self._nullable = (
            bool(nullable)
            if nullable is not None
            else any(self._null_mask)
        )

    @classmethod
    def from_buffer(
        cls,
        buffer: bytes | bytearray | memoryview,
        logical_type: object,
        *,
        length: int,
        name: str | None = None,
        null_mask: Sequence[bool] | None = None,
        nullable: bool | None = None,
    ) -> "Column":
        spec = parse_logical_type(logical_type)
        stride = spec.fixed_width_bytes()
        if stride is None:
            raise LogicalTypeError(
                f"logical type `{spec.canonical_name()}` does not support fixed-width buffers"
            )
        return cls._from_storage(
            FlatBufferStorage(memoryview(buffer), stride),
            spec,
            "flat",
            length,
            name=name,
            null_mask=null_mask,
            nullable=nullable,
        )

    @classmethod
    def from_varlen_buffers(
        cls,
        offsets: bytes | bytearray | memoryview,
        data: bytes | bytearray | memoryview,
        logical_type: object,
        *,
        length: int,
        offset_width: str = "U32",
        name: str | None = None,
        null_mask: Sequence[bool] | None = None,
        nullable: bool | None = None,
    ) -> "Column":
        return cls._from_storage(
            VarLenStorage(memoryview(offsets), memoryview(data), offset_width.upper()),
            parse_logical_type(logical_type),
            "flat",
            length,
            name=name,
            null_mask=null_mask,
            nullable=nullable,
        )

    @classmethod
    def from_constant(
        cls,
        value: Any,
        logical_type: object | None = None,
        *,
        length: int,
        name: str | None = None,
        null_mask: Sequence[bool] | None = None,
        nullable: bool | None = None,
    ) -> "Column":
        spec = ensure_logical_type(logical_type, value)
        return cls._from_storage(
            ConstantView(value=value, length=length),
            spec,
            "constant",
            length,
            name=name,
            null_mask=null_mask,
            nullable=nullable,
        )

    @classmethod
    def from_sequence(
        cls,
        start: int | float,
        step: int | float,
        logical_type: object,
        *,
        length: int,
        name: str | None = None,
        null_mask: Sequence[bool] | None = None,
        nullable: bool | None = None,
    ) -> "Column":
        return cls._from_storage(
            SequenceView(start=start, step=step, length=length),
            parse_logical_type(logical_type),
            "sequence",
            length,
            name=name,
            null_mask=null_mask,
            nullable=nullable,
        )

    @classmethod
    def from_dictionary(
        cls,
        indices: Sequence[int],
        dictionary: Sequence[Any],
        logical_type: object | None = None,
        *,
        name: str | None = None,
        null_mask: Sequence[bool] | None = None,
        nullable: bool | None = None,
    ) -> "Column":
        dictionary_values = tuple(dictionary)
        if not dictionary_values:
            raise ResultContractError("dictionary-encoded columns require at least one dictionary value")
        spec = ensure_logical_type(logical_type, dictionary_values[0])
        return cls._from_storage(
            DictionaryView(indices=tuple(int(index) for index in indices), dictionary=dictionary_values),
            spec,
            "dictionary",
            len(indices),
            name=name,
            null_mask=null_mask,
            nullable=nullable,
        )

    @classmethod
    def from_list_storage(
        cls,
        offsets: bytes | bytearray | memoryview,
        child: "Column",
        *,
        logical_type: object | None = None,
        length: int,
        offset_width: str = "U32",
        name: str | None = None,
        null_mask: Sequence[bool] | None = None,
        nullable: bool | None = None,
    ) -> "Column":
        spec = ensure_logical_type(logical_type, [child.logical_type])
        return cls._from_storage(
            ListStorage(memoryview(offsets), child, offset_width.upper()),
            spec,
            "list",
            length,
            name=name,
            null_mask=null_mask,
            nullable=nullable,
        )

    @classmethod
    def from_struct_children(
        cls,
        children: Sequence["Column"],
        *,
        logical_type: object,
        length: int,
        name: str | None = None,
        null_mask: Sequence[bool] | None = None,
        nullable: bool | None = None,
    ) -> "Column":
        return cls._from_storage(
            StructStorage(tuple(children)),
            parse_logical_type(logical_type),
            "struct",
            length,
            name=name,
            null_mask=null_mask,
            nullable=nullable,
        )

    @classmethod
    def from_arrow_export(
        cls,
        export: ArrowArrayExport,
        *,
        row_count: int | None = None,
        logical_type: object | None = None,
        name: str | None = None,
    ) -> "Column":
        if not isinstance(export, ArrowArrayExport):
            raise ResultContractError(
                "Paro can only zero-copy import its own Arrow export wrapper without pyarrow"
            )
        source = export.source
        if row_count is not None and len(source) != row_count:
            raise ResultContractError(
                f"arrow export row count {len(source)} does not match expected {row_count}"
            )
        if logical_type is not None and source.logical_type != parse_logical_type(logical_type).canonical_name():
            raise ResultContractError(
                f"arrow export logical type `{source.logical_type}` does not match expected `{parse_logical_type(logical_type).canonical_name()}`"
            )
        if name is None:
            return source
        spec = (
            parse_logical_type(logical_type)
            if logical_type is not None
            else source.logical_type_spec()
        )
        return cls._from_storage(
            source._storage,
            spec,
            source.encoding,
            len(source),
            null_mask=source.null_mask,
            nullable=source.nullable,
            name=name,
        )

    @classmethod
    def from_result(
        cls,
        result: Any,
        *,
        row_count: int | None,
        logical_type: object | None = None,
        name: str | None = None,
        returns_nullable: bool = True,
    ) -> "Column":
        if isinstance(result, Column):
            column = result
        elif isinstance(result, ArrowArrayExport):
            column = cls.from_arrow_export(result, row_count=row_count, logical_type=logical_type, name=name)
        elif hasattr(result, "__arrow_c_array__"):
            column = cls.from_arrow_export(result.__arrow_c_array__(), row_count=row_count, logical_type=logical_type, name=name)
        elif isinstance(result, (str, bytes, bytearray, memoryview, bool, int, float, Decimal)) or result is None:
            if row_count is None:
                raise ResultContractError(
                    "scalar or constant results require an explicit expected row count"
                )
            column = cls.from_constant(result, logical_type=logical_type, length=row_count, name=name)
        else:
            values = list(result)
            column = cls(values, logical_type=logical_type, name=name)

        if row_count is not None and len(column) != row_count:
            if column.encoding == "constant" and len(column) == row_count:
                pass
            else:
                raise ResultContractError(
                    f"handler returned {len(column)} rows, expected {row_count}"
                )
        if not returns_nullable and any(column.null_mask):
            raise ResultContractError("handler returned NULLs for a NOT NULL result contract")
        if logical_type is not None:
            expected = parse_logical_type(logical_type).canonical_name()
            if column.logical_type != expected:
                raise ResultContractError(
                    f"handler returned logical type `{column.logical_type}`, expected `{expected}`"
                )
        return column

    @classmethod
    def _from_storage(
        cls,
        storage: Any,
        spec: LogicalTypeSpec,
        encoding: str,
        length: int,
        *,
        name: str | None = None,
        null_mask: Sequence[bool] | None = None,
        nullable: bool | None = None,
    ) -> "Column":
        column = cls.__new__(cls)
        column._storage = storage
        column._logical_type = spec
        column._encoding = encoding
        column._length = length
        column._name = name or ""
        column._null_mask = _normalize_null_mask(null_mask, length)
        column._nullable = (
            bool(nullable)
            if nullable is not None
            else any(column._null_mask)
        )
        return column

    def _coerce_storage(
        self,
        data: Any,
        spec: LogicalTypeSpec,
        encoding: str,
        *,
        length: int | None,
        offset_width: str,
    ) -> tuple[Any, int]:
        if isinstance(data, (FlatBufferStorage, VarLenStorage, ListStorage, StructStorage, ConstantView, SequenceView, DictionaryView)):
            inferred_length = data.length if isinstance(data, (ConstantView, SequenceView)) else length
            if inferred_length is None:
                raise ResultContractError("buffer-backed columns require an explicit length")
            return data, int(inferred_length)

        if isinstance(data, Column):
            return data._storage, len(data)

        if encoding == "constant":
            if length is None:
                raise ResultContractError("constant columns require an explicit length")
            return ConstantView(value=data, length=length), length

        if encoding == "sequence":
            if isinstance(data, Sequence) and len(data) == 2:
                if length is None:
                    raise ResultContractError("sequence columns require an explicit length")
                return SequenceView(start=data[0], step=data[1], length=length), length
            raise ResultContractError("sequence columns require a `(start, step)` pair")

        if encoding == "dictionary":
            if not isinstance(data, Mapping):
                raise ResultContractError(
                    "dictionary columns require a mapping with `indices` and `dictionary`"
                )
            indices = tuple(int(item) for item in data["indices"])
            dictionary = tuple(data["dictionary"])
            return DictionaryView(indices=indices, dictionary=dictionary), len(indices)

        if encoding == "list":
            rows = list(data)
            child_type = spec.element or infer_logical_type(_first_non_null(rows) or [])
            return _flatten_list_values(rows, child_type, offset_width.upper()), len(rows)

        if encoding == "struct":
            rows = list(data)
            if spec.kind != "struct":
                raise ResultContractError("struct encoding requires a struct logical type")
            return _build_struct_storage(rows, spec), len(rows)

        if isinstance(data, (bytes, bytearray, memoryview)):
            stride = spec.fixed_width_bytes()
            if stride is None:
                raise ResultContractError(
                    f"logical type `{spec.canonical_name()}` requires structured buffers, not a flat byte view"
                )
            inferred_length = length if length is not None else len(data) // stride
            return FlatBufferStorage(memoryview(data), stride), inferred_length

        values = list(data)
        if spec.kind in {"varchar", "blob", "json", "jsonb"}:
            return _build_varlen_storage(values, spec, offset_width.upper()), len(values)
        if spec.kind == "list":
            child_type = spec.element or infer_logical_type(_first_non_null(values) or [])
            return _flatten_list_values(values, child_type, offset_width.upper()), len(values)
        if spec.kind == "struct":
            return _build_struct_storage(values, spec), len(values)
        return _pack_fixed_width(values, spec), len(values)

    def __len__(self) -> int:
        return self._length

    def __iter__(self):
        return iter(self.materialize_py())

    def __repr__(self) -> str:
        return (
            f"Column(name={self._name!r}, logical_type={self.logical_type!r}, "
            f"encoding={self.encoding!r}, length={len(self)})"
        )

    def logical_type_spec(self) -> LogicalTypeSpec:
        return self._logical_type

    @property
    def logical_type(self) -> str:
        return self._logical_type.canonical_name()

    @property
    def encoding(self) -> str:
        return self._encoding

    @property
    def null_mask(self) -> tuple[bool, ...]:
        return self._null_mask

    @property
    def nullable(self) -> bool:
        return self._nullable

    def arrow_buffer_views(self) -> dict[str, memoryview]:
        buffers: dict[str, memoryview] = {}
        validity = self._validity_buffer()
        if validity is not None:
            buffers["validity"] = validity
        if isinstance(self._storage, FlatBufferStorage):
            buffers["values"] = self._storage.buffer
        elif isinstance(self._storage, VarLenStorage):
            buffers["offsets"] = self._storage.offsets
            buffers["data"] = self._storage.data
        elif isinstance(self._storage, ListStorage):
            buffers["offsets"] = self._storage.offsets
        return buffers

    def _validity_buffer(self) -> memoryview | None:
        if any(self._null_mask):
            return memoryview(pack_validity_bitmap(self._null_mask))
        return None

    def to_numpy(self):
        numpy = _load_optional_module("numpy")
        dtype = _NUMPY_DTYPES.get(self._logical_type.kind)

        if self._encoding == "flat" and isinstance(self._storage, FlatBufferStorage) and dtype is not None:
            if self._logical_type.kind == "array" and self._logical_type.element is not None and self._logical_type.array_length is not None:
                child_dtype = _NUMPY_DTYPES.get(self._logical_type.element.kind)
                if child_dtype is None:
                    raise ResultContractError(
                        f"logical type `{self.logical_type}` cannot expose an ndarray fast path"
                    )
                array = numpy.frombuffer(
                    self._storage.buffer,
                    dtype=child_dtype,
                    count=len(self) * self._logical_type.array_length,
                )
                if hasattr(array, "reshape"):
                    return array.reshape(len(self), self._logical_type.array_length)
                return array
            return numpy.frombuffer(self._storage.buffer, dtype=dtype, count=len(self))

        if self._encoding == "constant":
            return numpy.full(len(self), self._storage.value, dtype=dtype)  # type: ignore[arg-type]
        if self._encoding == "sequence":
            stop = self._storage.start + self._storage.step * len(self)  # type: ignore[union-attr]
            return numpy.arange(self._storage.start, stop, self._storage.step, dtype=dtype)  # type: ignore[union-attr]

        warnings.warn(
            f"Column.to_numpy() fell back to Python object materialization for `{self.logical_type}`",
            SlowPathWarning,
            stacklevel=2,
        )
        if hasattr(numpy, "array"):
            return numpy.array(self.materialize_py(), dtype=object)
        if hasattr(numpy, "asarray"):
            return numpy.asarray(self.materialize_py(), dtype=object)
        raise RuntimeError("numpy module does not provide `array` or `asarray`")

    def to_arrow(self):
        pyarrow = _load_optional_module("pyarrow")
        export = self.__arrow_c_array__()
        if hasattr(pyarrow, "consume_paro_export"):
            return pyarrow.consume_paro_export(export)
        if hasattr(pyarrow, "array"):
            return pyarrow.array(export.materialize_py())
        raise RuntimeError("pyarrow module does not provide a supported Paro adapter entrypoint")

    def __arrow_c_array__(self, requested_schema: object | None = None) -> ArrowArrayExport:
        return ArrowArrayExport(source=self, requested_schema=requested_schema)

    def materialize_py(self) -> list[Any]:
        warnings.warn(
            "Column.materialize_py() is an explicit slow path; prefer to_numpy() or to_arrow() in hot kernels",
            SlowPathWarning,
            stacklevel=2,
        )
        return self._materialize_values()

    def _materialize_values(self) -> list[Any]:
        if isinstance(self._storage, ConstantView):
            values = self._storage.materialize(self._null_mask)
        elif isinstance(self._storage, SequenceView):
            values = self._storage.materialize(self._null_mask)
        elif isinstance(self._storage, DictionaryView):
            values = self._storage.materialize(self._null_mask)
        elif isinstance(self._storage, FlatBufferStorage):
            values = _materialize_fixed_width(self._storage, self._logical_type, len(self))
        elif isinstance(self._storage, VarLenStorage):
            values = _materialize_varlen(self._storage, self._logical_type, len(self))
        elif isinstance(self._storage, ListStorage):
            values = _materialize_list(self._storage, len(self))
        elif isinstance(self._storage, StructStorage):
            values = _materialize_struct(self._storage, self._logical_type, len(self))
        else:
            raise ResultContractError(f"unsupported column storage `{type(self._storage).__name__}`")
        return [
            None if self._null_mask[index] else values[index]
            for index in range(len(self))
        ]

    def as_constant_view(self) -> ConstantView:
        if isinstance(self._storage, ConstantView):
            return self._storage
        raise TypeError("column is not constant-encoded")

    def as_sequence_view(self) -> SequenceView:
        if isinstance(self._storage, SequenceView):
            return self._storage
        raise TypeError("column is not sequence-encoded")

    def as_dictionary_view(self) -> DictionaryView:
        if isinstance(self._storage, DictionaryView):
            return self._storage
        raise TypeError("column is not dictionary-encoded")

    def borrow_buffer(self) -> memoryview:
        if isinstance(self._storage, FlatBufferStorage):
            return self._storage.buffer
        if isinstance(self._storage, VarLenStorage):
            return self._storage.data
        raise TypeError("column does not expose a direct underlying buffer")

    def to_abi_descriptor(self, *, name: str | None = None) -> dict[str, object]:
        descriptor_name = name or self._name or "column"
        descriptor: dict[str, object] = {
            "name": descriptor_name,
            "logical_type": self._logical_type.to_abi_json(),
            "encoding": self._encoding.capitalize() if self._encoding != "flat" else "Flat",
            "population_mode": "Eager",
            "nullable": self._nullable,
            "validity": None,
            "layout": {},
            "children": [],
        }
        if any(self._null_mask):
            validity = self._validity_buffer()
            assert validity is not None
            descriptor["validity"] = {
                "buffer_index": 0,
                "offset": 0,
                "len": len(validity),
                "alignment": 1,
                "generation": 0,
                "device": "Host",
            }

        if isinstance(self._storage, FlatBufferStorage):
            descriptor["layout"] = {
                "FixedWidth": {
                    "values": {
                        "buffer_index": 0,
                        "offset": 0,
                        "len": len(self._storage.buffer),
                        "alignment": max(self._storage.stride, 1),
                        "generation": 0,
                        "device": "Host",
                    },
                    "stride": self._storage.stride,
                }
            }
        elif isinstance(self._storage, VarLenStorage):
            descriptor["layout"] = {
                "VarLen": {
                    "offsets": {
                        "buffer_index": 0,
                        "offset": 0,
                        "len": len(self._storage.offsets),
                        "alignment": _offset_width_bytes(self._storage.offset_width),
                        "generation": 0,
                        "device": "Host",
                    },
                    "data": {
                        "buffer_index": 1,
                        "offset": 0,
                        "len": len(self._storage.data),
                        "alignment": 1,
                        "generation": 0,
                        "device": "Host",
                    },
                    "offset_width": self._storage.offset_width,
                }
            }
        elif isinstance(self._storage, ConstantView):
            descriptor["layout"] = {
                "Constant": {
                    "value": encode_scalar_value(self._storage.value, self._logical_type)
                }
            }
        elif isinstance(self._storage, SequenceView):
            descriptor["layout"] = {
                "Sequence": {
                    "start": self._storage.start,
                    "step": self._storage.step,
                }
            }
        elif isinstance(self._storage, DictionaryView):
            descriptor["layout"] = {
                "Dictionary": {
                    "indices": {
                        "buffer_index": 0,
                        "offset": 0,
                        "len": len(self._storage.indices) * 4,
                        "alignment": 4,
                        "generation": 0,
                        "device": "Host",
                    },
                    "dictionary": Column(
                        self._storage.dictionary,
                        logical_type=self._logical_type,
                    ).to_abi_descriptor(name=f"{descriptor_name}_dictionary"),
                }
            }
        elif isinstance(self._storage, ListStorage):
            descriptor["layout"] = {
                "List": {
                    "offsets": {
                        "buffer_index": 0,
                        "offset": 0,
                        "len": len(self._storage.offsets),
                        "alignment": _offset_width_bytes(self._storage.offset_width),
                        "generation": 0,
                        "device": "Host",
                    },
                    "offset_width": self._storage.offset_width,
                }
            }
            descriptor["children"] = [
                self._storage.child.to_abi_descriptor(name=f"{descriptor_name}_element")
            ]
        elif isinstance(self._storage, StructStorage):
            descriptor["layout"] = {"Struct": {}}
            descriptor["children"] = [
                child.to_abi_descriptor(name=field.name)
                for child, field in zip(self._storage.children, self._logical_type.fields, strict=True)
            ]
        else:
            raise ResultContractError(f"cannot serialize column storage `{type(self._storage).__name__}`")
        return descriptor


def coerce_result_columns(
    result: Any,
    *,
    row_count: int | None,
    logical_types: object | Sequence[object] | None = None,
    output_names: Sequence[str] | None = None,
    returns_nullable: bool | Sequence[bool] = True,
) -> list[Column]:
    if isinstance(logical_types, Sequence) and not isinstance(logical_types, (str, bytes, bytearray)):
        normalized_types = list(logical_types)
    elif logical_types is None:
        normalized_types = []
    else:
        normalized_types = [logical_types]

    if isinstance(returns_nullable, Sequence) and not isinstance(returns_nullable, (str, bytes, bytearray)):
        nullable_flags = list(returns_nullable)
    else:
        nullable_flags = [bool(returns_nullable)] * max(len(normalized_types), 1)

    if isinstance(result, tuple):
        raw_columns = list(result)
    else:
        raw_columns = [result]

    if normalized_types and len(normalized_types) != len(raw_columns):
        raise ResultContractError(
            f"handler returned {len(raw_columns)} columns, expected {len(normalized_types)}"
        )

    if output_names is not None and len(output_names) != len(raw_columns):
        raise ResultContractError(
            f"handler returned {len(raw_columns)} columns, expected {len(output_names)} output names"
        )

    columns: list[Column] = []
    for index, raw in enumerate(raw_columns):
        logical_type = normalized_types[index] if index < len(normalized_types) else None
        nullable = nullable_flags[index] if index < len(nullable_flags) else True
        name = output_names[index] if output_names is not None else None
        columns.append(
            Column.from_result(
                raw,
                row_count=row_count,
                logical_type=logical_type,
                name=name,
                returns_nullable=nullable,
            )
        )
    return columns
