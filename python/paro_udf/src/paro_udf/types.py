"""Logical-type helpers shared by the SDK and Python worker."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any


class LogicalTypeError(ValueError):
    """Raised when a Paro ABI logical type cannot be parsed or inferred."""


_ABI_TO_KIND = {
    "Boolean": "boolean",
    "Int8": "int8",
    "Int16": "int16",
    "Int32": "int32",
    "Int64": "int64",
    "HugeInt": "hugeint",
    "UInt8": "uint8",
    "UInt16": "uint16",
    "UInt32": "uint32",
    "UInt64": "uint64",
    "UHugeInt": "uhugeint",
    "Float32": "float32",
    "Float64": "float64",
    "Decimal": "decimal",
    "Varchar": "varchar",
    "Blob": "blob",
    "Date": "date",
    "Time": "time",
    "Timestamp": "timestamp",
    "TimestampTz": "timestamptz",
    "Interval": "interval",
    "Uuid": "uuid",
    "Json": "json",
    "Jsonb": "jsonb",
    "Array": "array",
    "List": "list",
    "Struct": "struct",
}

_KIND_TO_ABI = {value: key for key, value in _ABI_TO_KIND.items()}

_FIXED_WIDTH_BYTES = {
    "boolean": 1,
    "int8": 1,
    "uint8": 1,
    "int16": 2,
    "uint16": 2,
    "int32": 4,
    "uint32": 4,
    "date": 4,
    "float32": 4,
    "int64": 8,
    "uint64": 8,
    "time": 8,
    "timestamp": 8,
    "timestamptz": 8,
    "float64": 8,
    "decimal64": 8,
    "hugeint": 16,
    "uhugeint": 16,
    "interval": 16,
    "uuid": 16,
}

_STRUCT_FORMATS = {
    "boolean": "B",
    "int8": "b",
    "uint8": "B",
    "int16": "h",
    "uint16": "H",
    "int32": "i",
    "uint32": "I",
    "date": "i",
    "float32": "f",
    "int64": "q",
    "uint64": "Q",
    "time": "q",
    "timestamp": "q",
    "timestamptz": "q",
    "float64": "d",
}

_SCALAR_LOGICAL_TYPES = {
    "boolean",
    "int8",
    "int16",
    "int32",
    "int64",
    "hugeint",
    "uint8",
    "uint16",
    "uint32",
    "uint64",
    "uhugeint",
    "float32",
    "float64",
    "decimal",
    "varchar",
    "blob",
    "date",
    "time",
    "timestamp",
    "timestamptz",
    "interval",
    "uuid",
    "json",
    "jsonb",
}


@dataclass(frozen=True, slots=True)
class StructFieldSpec:
    """Logical-type metadata for one field inside a Paro struct column."""

    name: str
    data_type: "LogicalTypeSpec"
    nullable: bool = True

    def to_abi_json(self) -> dict[str, object]:
        return {
            "name": self.name,
            "data_type": self.data_type.to_abi_json(),
            "nullable": self.nullable,
        }


@dataclass(frozen=True, slots=True)
class LogicalTypeSpec:
    """Canonicalized logical-type descriptor for SDK-side type checks."""

    kind: str
    element: "LogicalTypeSpec | None" = None
    fields: tuple[StructFieldSpec, ...] = ()
    array_length: int | None = None
    precision: int | None = None
    scale: int | None = None

    def canonical_name(self) -> str:
        if self.kind == "decimal":
            return f"decimal({self.precision},{self.scale})"
        if self.kind == "list":
            assert self.element is not None
            return f"list<{self.element.canonical_name()}>"
        if self.kind == "array":
            assert self.element is not None and self.array_length is not None
            return f"array<{self.element.canonical_name()},{self.array_length}>"
        if self.kind == "struct":
            fields = ", ".join(
                f"{field.name}:{field.data_type.canonical_name()}"
                for field in self.fields
            )
            return f"struct<{fields}>"
        return self.kind

    def fixed_width_bytes(self) -> int | None:
        if self.kind == "decimal":
            if self.precision is None:
                return None
            return 8 if self.precision <= 18 else 16
        if self.kind == "array" and self.element is not None and self.array_length is not None:
            child_width = self.element.fixed_width_bytes()
            return None if child_width is None else child_width * self.array_length
        return _FIXED_WIDTH_BYTES.get(self.kind)

    def struct_format(self) -> str | None:
        if self.kind == "decimal" and self.precision is not None and self.precision <= 18:
            return "q"
        return _STRUCT_FORMATS.get(self.kind)

    def is_scalar(self) -> bool:
        return self.kind in _SCALAR_LOGICAL_TYPES

    def is_varlen(self) -> bool:
        return self.kind in {"varchar", "blob", "json", "jsonb", "list"}

    def is_nested(self) -> bool:
        return self.kind in {"array", "list", "struct"}

    def to_abi_json(self) -> object:
        abi_kind = _KIND_TO_ABI.get(self.kind)
        if abi_kind is None:
            raise LogicalTypeError(f"unknown logical type kind `{self.kind}`")

        if self.kind == "decimal":
            return {"Decimal": {"precision": self.precision, "scale": self.scale}}
        if self.kind == "list":
            assert self.element is not None
            return {"List": self.element.to_abi_json()}
        if self.kind == "array":
            assert self.element is not None and self.array_length is not None
            return {
                "Array": {
                    "element": self.element.to_abi_json(),
                    "length": self.array_length,
                }
            }
        if self.kind == "struct":
            return {"Struct": [field.to_abi_json() for field in self.fields]}
        return abi_kind


def parse_logical_type(value: object) -> LogicalTypeSpec:
    """Parse a Rust ABI JSON logical type or a user-facing shorthand string."""

    if isinstance(value, LogicalTypeSpec):
        return value

    if isinstance(value, str):
        stripped = value.strip()
        lowered = stripped.lower()
        if lowered.startswith("decimal(") and lowered.endswith(")"):
            precision, scale = lowered[len("decimal(") : -1].split(",", maxsplit=1)
            return LogicalTypeSpec(
                kind="decimal",
                precision=int(precision),
                scale=int(scale),
            )
        if lowered.startswith("list<") and lowered.endswith(">"):
            return LogicalTypeSpec(
                kind="list",
                element=parse_logical_type(lowered[len("list<") : -1]),
            )
        if lowered.startswith("array<") and lowered.endswith(">"):
            element, length = lowered[len("array<") : -1].rsplit(",", maxsplit=1)
            return LogicalTypeSpec(
                kind="array",
                element=parse_logical_type(element),
                array_length=int(length),
            )
        if lowered.startswith("struct<") and lowered.endswith(">"):
            field_specs = []
            inner = lowered[len("struct<") : -1].strip()
            if inner:
                for raw_field in inner.split(","):
                    field_name, raw_type = raw_field.split(":", maxsplit=1)
                    field_specs.append(
                        StructFieldSpec(
                            name=field_name.strip(),
                            data_type=parse_logical_type(raw_type.strip()),
                            nullable=True,
                        )
                    )
            return LogicalTypeSpec(kind="struct", fields=tuple(field_specs))
        if stripped in _ABI_TO_KIND:
            return LogicalTypeSpec(kind=_ABI_TO_KIND[stripped])
        if lowered in _KIND_TO_ABI:
            return LogicalTypeSpec(kind=lowered)
        raise LogicalTypeError(f"cannot parse logical type `{value}`")

    if isinstance(value, Mapping):
        if len(value) != 1:
            raise LogicalTypeError(f"invalid logical type mapping `{value}`")
        abi_kind, payload = next(iter(value.items()))
        kind = _ABI_TO_KIND.get(str(abi_kind))
        if kind is None:
            raise LogicalTypeError(f"unknown ABI logical type `{abi_kind}`")
        if kind == "decimal":
            if not isinstance(payload, Mapping):
                raise LogicalTypeError("decimal payload must be a mapping")
            return LogicalTypeSpec(
                kind="decimal",
                precision=int(payload["precision"]),
                scale=int(payload["scale"]),
            )
        if kind == "list":
            return LogicalTypeSpec(kind="list", element=parse_logical_type(payload))
        if kind == "array":
            if not isinstance(payload, Mapping):
                raise LogicalTypeError("array payload must be a mapping")
            return LogicalTypeSpec(
                kind="array",
                element=parse_logical_type(payload["element"]),
                array_length=int(payload["length"]),
            )
        if kind == "struct":
            fields = []
            for field in payload if isinstance(payload, Sequence) else ():
                if not isinstance(field, Mapping):
                    raise LogicalTypeError("struct field payload must be a mapping")
                fields.append(
                    StructFieldSpec(
                        name=str(field["name"]),
                        data_type=parse_logical_type(field["data_type"]),
                        nullable=bool(field.get("nullable", True)),
                    )
                )
            return LogicalTypeSpec(kind="struct", fields=tuple(fields))
        return LogicalTypeSpec(kind=kind)

    raise LogicalTypeError(f"unsupported logical type payload `{value!r}`")


def infer_logical_type(value: object) -> LogicalTypeSpec:
    """Infer a Paro logical type from one Python value or a sequence of values."""

    if isinstance(value, ColumnProtocol):
        return value.logical_type_spec()

    if value is None:
        return LogicalTypeSpec("varchar")
    if isinstance(value, bool):
        return LogicalTypeSpec("boolean")
    if isinstance(value, int):
        return LogicalTypeSpec("int64")
    if isinstance(value, float):
        return LogicalTypeSpec("float64")
    if isinstance(value, Decimal):
        sign, digits, exponent = value.as_tuple()
        precision = max(len(digits), 1)
        scale = max(-exponent, 0)
        return LogicalTypeSpec("decimal", precision=precision, scale=scale)
    if isinstance(value, str):
        return LogicalTypeSpec("varchar")
    if isinstance(value, (bytes, bytearray, memoryview)):
        return LogicalTypeSpec("blob")
    if isinstance(value, Mapping):
        fields = tuple(
            StructFieldSpec(
                name=str(key),
                data_type=infer_logical_type(item),
                nullable=item is None,
            )
            for key, item in value.items()
        )
        return LogicalTypeSpec("struct", fields=fields)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray, memoryview)):
        if not value:
            return LogicalTypeSpec("varchar")
        first_non_null = next((item for item in value if item is not None), None)
        if first_non_null is None:
            return LogicalTypeSpec("varchar")
        if any(isinstance(item, (list, tuple, Mapping)) for item in value if item is not None):
            return LogicalTypeSpec("list", element=infer_logical_type(first_non_null))
        return infer_logical_type(first_non_null)

    raise LogicalTypeError(f"cannot infer logical type from `{value!r}`")


def ensure_logical_type(value: object | None, fallback: object) -> LogicalTypeSpec:
    if value is None:
        return infer_logical_type(fallback)
    return parse_logical_type(value)


def abi_enum_name(kind: str) -> str:
    abi_kind = _KIND_TO_ABI.get(kind)
    if abi_kind is None:
        raise LogicalTypeError(f"unknown logical type kind `{kind}`")
    return abi_kind


def encode_scalar_value(value: object, logical_type: object | None = None) -> object:
    """Encode one scalar into the serde-friendly ABI JSON form."""

    if value is None:
        return "Null"

    spec = ensure_logical_type(logical_type, value)
    abi_kind = abi_enum_name(spec.kind)

    if spec.kind == "decimal":
        return {
            "Decimal": {
                "value": int(Decimal(value).scaleb(spec.scale or 0)),
                "precision": spec.precision,
                "scale": spec.scale,
            }
        }
    if spec.kind in {"varchar", "json"}:
        return {"Utf8": str(value)}
    if spec.kind in {"blob", "jsonb"}:
        return {"Binary": list(bytes(value))}
    if spec.kind == "uuid":
        raw = bytes(value)
        if len(raw) != 16:
            raise LogicalTypeError("uuid values must be 16 bytes")
        return {"Uuid": list(raw)}
    return {abi_kind: value}


def decode_scalar_value(value: object) -> object:
    """Decode the serde-friendly scalar layout used by the Rust ABI."""

    if value == "Null":
        return None
    if not isinstance(value, Mapping) or len(value) != 1:
        raise LogicalTypeError(f"invalid scalar ABI payload `{value!r}`")
    kind, payload = next(iter(value.items()))
    if kind in {"Utf8", "Json"}:
        return str(payload)
    if kind in {"Binary", "Jsonb"}:
        return bytes(payload)
    if kind == "Uuid":
        return bytes(payload)
    if kind == "Decimal":
        if not isinstance(payload, Mapping):
            raise LogicalTypeError("decimal scalar payload must be a mapping")
        scale = int(payload["scale"])
        return Decimal(int(payload["value"])).scaleb(-scale)
    return payload


class ColumnProtocol:
    """Small protocol shim to avoid a hard import cycle with Column."""

    def logical_type_spec(self) -> LogicalTypeSpec:  # pragma: no cover - protocol shim
        raise NotImplementedError
