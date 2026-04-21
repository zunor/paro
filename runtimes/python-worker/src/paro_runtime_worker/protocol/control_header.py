"""Fixed-layout control header bindings shared with the Rust host runtime."""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
import struct


CONTROL_HEADER_VERSION = 1
CONTROL_HEADER_STRUCT = struct.Struct("<HHIQQII")
CONTROL_HEADER_SIZE = CONTROL_HEADER_STRUCT.size


class ControlMessageKind(IntEnum):
    SUBMIT = 1
    CANCEL = 2
    COMPLETE = 3
    CREDIT_RETURN = 4
    ERROR = 5


@dataclass(slots=True)
class ControlHeader:
    version: int
    kind: ControlMessageKind
    flags: int
    batch_id: int
    lease_id: int
    payload_len: int
    reserved: int = 0

    @classmethod
    def new(
        cls,
        kind: ControlMessageKind,
        batch_id: int,
        lease_id: int,
        payload_len: int,
    ) -> "ControlHeader":
        return cls(
            version=CONTROL_HEADER_VERSION,
            kind=kind,
            flags=0,
            batch_id=batch_id,
            lease_id=lease_id,
            payload_len=payload_len,
            reserved=0,
        )

    def encode(self) -> bytes:
        return CONTROL_HEADER_STRUCT.pack(
            self.version,
            int(self.kind),
            self.flags,
            self.batch_id,
            self.lease_id,
            self.payload_len,
            self.reserved,
        )

    @classmethod
    def decode(cls, payload: bytes) -> "ControlHeader":
        if len(payload) != CONTROL_HEADER_SIZE:
            raise ValueError(f"control header requires {CONTROL_HEADER_SIZE} bytes")
        version, kind, flags, batch_id, lease_id, payload_len, reserved = (
            CONTROL_HEADER_STRUCT.unpack(payload)
        )
        if version != CONTROL_HEADER_VERSION:
            raise ValueError(f"unsupported control header version {version}")
        return cls(
            version=version,
            kind=ControlMessageKind(kind),
            flags=flags,
            batch_id=batch_id,
            lease_id=lease_id,
            payload_len=payload_len,
            reserved=reserved,
        )
