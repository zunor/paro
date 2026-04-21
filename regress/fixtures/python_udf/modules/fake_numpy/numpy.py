# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

import struct


class FakeArray(list):
    def __add__(self, scalar):
        return FakeArray([item + scalar for item in self])


def frombuffer(buffer, *, dtype, count):
    if dtype == "int32":
        fmt = "<i"
    elif dtype == "int64":
        fmt = "<q"
    else:
        raise AssertionError(f"unsupported dtype {dtype}")

    size = struct.calcsize(fmt)
    raw = memoryview(buffer).tobytes()
    return FakeArray(
        struct.unpack_from(fmt, raw, index * size)[0]
        for index in range(count)
    )


def array(*_args, **_kwargs):
    raise AssertionError("fixed-width fast path should not fall back to numpy.array()")


def asarray(*_args, **_kwargs):
    raise AssertionError("fixed-width fast path should not fall back to numpy.asarray()")
