# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Bounded, versioned control messages. SQL values travel only in Arrow."""

import base64
import json
import sys

VERSION = 1
MAX_CONTROL_BYTES = 1 << 20
MAX_BATCH_BYTES = 16 << 20
MAX_BATCH_ROWS = 65536
MAX_INVOCATION_ROWS = 1 << 20
MAX_RESULT_BYTES = 256 << 20
MAX_HANDLERS = 8
MAX_OWNER_HANDLERS = 4
MAX_TERMINALS = 8192
TIMEOUT = 30.0


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("PROTOCOL: duplicate control field")
        result[key] = value
    return result


def decode(data):
    if len(data) > MAX_CONTROL_BYTES:
        raise ValueError("RESOURCE_EXHAUSTED: control frame too large")
    data = bytes(data)
    # Bound nesting before calling the recursive JSON decoder.
    depth = 0
    quoted = escaped = False
    for byte in data:
        if quoted:
            if escaped:
                escaped = False
            elif byte == 92:
                escaped = True
            elif byte == 34:
                quoted = False
        elif byte == 34:
            quoted = True
        elif byte in (91, 123):
            depth += 1
            if depth > 32:
                raise ValueError("PROTOCOL: control nesting limit")
        elif byte in (93, 125):
            depth -= 1
    value = json.loads(data, object_pairs_hook=_pairs)
    if not isinstance(value, dict) or type(value.get("version")) is not int or value.get("version") != VERSION:
        raise ValueError("PROTOCOL: incompatible control version")
    return value


def invocation_fence(value):
    if (not isinstance(value, list) or len(value) != 3
            or any(type(part) is not int or not 0 <= part < 1 << 64 for part in value[:2])
            or not isinstance(value[2], str) or len(value[2]) != 64
            or any(char not in "0123456789abcdef" for char in value[2])):
        raise ValueError("PROTOCOL: invalid invocation fence")
    return tuple(value)


def encode(**values):
    data = json.dumps(
        {"version": VERSION, **values}, separators=(",", ":"), allow_nan=False
    ).encode()
    if len(data) > MAX_CONTROL_BYTES:
        raise ValueError("RESOURCE_EXHAUSTED: control frame too large")
    return data


def schema_from_wire(value):
    import pyarrow as pa

    raw = base64.b64decode(value, validate=True)
    if len(raw) > MAX_CONTROL_BYTES:
        raise ValueError("RESOURCE_EXHAUSTED: schema too large")
    return pa.ipc.read_schema(pa.BufferReader(raw))


def capabilities(epoch):
    import pyarrow as pa

    return {
        "version": VERSION,
        "epoch": epoch,
        "python": list(sys.version_info[:3]),
        "arrow": pa.__version__,
        "window": 1,
        "max_handlers": MAX_HANDLERS,
        "max_owner_handlers": MAX_OWNER_HANDLERS,
        "max_batch_bytes": MAX_BATCH_BYTES,
        "max_batch_rows": MAX_BATCH_ROWS,
        "modes": ["scalar", "vector", "table"],
    }
