# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""User-code subprocess. No Flight imports, JSON SQL values, or simulated arrays."""

import contextlib
import os
import struct
import sys

import pyarrow as pa
import pyarrow.compute as pc
from paro_udf import Context

from .definition import validate
from .protocol import MAX_BATCH_BYTES, MAX_BATCH_ROWS, decode, encode

LENGTH = struct.Struct("<Q")


def read_frame(stream, limit):
    length = stream.read(LENGTH.size)
    if not length:
        return None
    if len(length) != LENGTH.size:
        raise EOFError("truncated frame length")
    size = LENGTH.unpack(length)[0]
    if size > limit:
        raise ValueError("RESOURCE_EXHAUSTED: handler frame too large")
    data = bytearray(size)
    view = memoryview(data)
    while view:
        count = stream.readinto(view)
        if not count:
            raise EOFError("truncated frame body")
        view = view[count:]
    return data


def write_frame(stream, data):
    stream.write(LENGTH.pack(len(data)))
    stream.write(data)
    stream.flush()


def serialize(batch):
    if batch.num_rows > MAX_BATCH_ROWS or batch.nbytes > MAX_BATCH_BYTES:
        raise ValueError("RESOURCE_EXHAUSTED: Arrow batch too large")
    # Size the uncompressed stream before allocating it, including its schema
    # and EOS marker. A fixed buffer cannot silently grow past the budget.
    size = batch.schema.serialize().size + pa.ipc.get_record_batch_size(batch) + 8
    if size > MAX_BATCH_BYTES:
        raise ValueError("RESOURCE_EXHAUSTED: Arrow batch too large")
    data = pa.allocate_buffer(size)
    sink = pa.FixedSizeBufferWriter(data)
    with pa.ipc.new_stream(sink, batch.schema, options=pa.ipc.IpcWriteOptions(compression=None)) as writer:
        writer.write_batch(batch)
    return data.slice(0, sink.tell())


def deserialize(data):
    with pa.ipc.open_stream(pa.BufferReader(data)) as reader:
        batches = iter(reader)
        batch = next(batches)
        if next(batches, None) is not None:
            raise ValueError("PROTOCOL: expected one Arrow batch")
        return batch


def invoke(handler, definition, batch, outputs, invocation):
    rows = batch.num_rows
    selected = list(range(rows))
    if definition["strict"]:
        selected = [
            index for index in selected
            if all(column[index].is_valid for column in batch.columns)
        ]
    active = batch.take(pa.array(selected, type=pa.int32())) if len(selected) != rows else batch
    ctx = Context(active.num_rows, invocation, definition["routine_id"], definition["generation"])
    mode = definition["mode"]
    if mode == "scalar":
        values = [
            handler(ctx, *(column[row].as_py() for column in active.columns))
            for row in range(active.num_rows)
        ]
        try:
            output = pa.array(values, type=outputs.field(0).type, safe=True)
        except (pa.ArrowException, TypeError, OverflowError) as error:
            raise ValueError(f"TYPE_CONTRACT: scalar result cannot be converted to {outputs.field(0).type}") from error
    elif not active.num_rows and mode != "table":
        output = pa.array([], type=outputs.field(0).type)
    else:
        output = handler(ctx, *active.columns)
    if mode == "table":
        if isinstance(output, pa.Table):
            if output.num_rows > MAX_BATCH_ROWS or output.nbytes > MAX_BATCH_BYTES:
                raise ValueError("RESOURCE_EXHAUSTED: table result exceeds batch limit")
            output = output.combine_chunks()
            batches = output.to_batches(max_chunksize=MAX_BATCH_ROWS)
            if len(batches) > 1:
                raise ValueError("RESOURCE_EXHAUSTED: table result exceeds batch row limit")
            output = batches[0] if batches else pa.RecordBatch.from_arrays(
                [pa.array([], type=field.type) for field in outputs], schema=outputs
            )
        if not isinstance(output, pa.RecordBatch):
            raise ValueError("TYPE_CONTRACT: table handler must return RecordBatch or Table")
        if not output.schema.equals(outputs, check_metadata=False):
            raise ValueError("TYPE_CONTRACT: table result schema mismatch")
        result = pa.RecordBatch.from_arrays(output.columns, schema=outputs)
    else:
        if isinstance(output, pa.ChunkedArray):
            if len(output) != active.num_rows or output.nbytes > MAX_BATCH_BYTES:
                raise ValueError("TYPE_CONTRACT: output shape or size mismatch")
            output = output.combine_chunks()
        if not isinstance(output, pa.Array) or output.type != outputs.field(0).type:
            raise ValueError("TYPE_CONTRACT: vector result must be an Arrow array of the declared type")
        if len(output) != active.num_rows:
            raise ValueError("TYPE_CONTRACT: output row count mismatch")
        if len(selected) != rows:
            positions = {row: index for index, row in enumerate(selected)}
            output = pc.take(output, pa.array([positions.get(row) for row in range(rows)], type=pa.int32()))
        result = pa.RecordBatch.from_arrays([output], schema=outputs)
    result.validate(full=True)
    if result.num_rows > MAX_BATCH_ROWS or result.nbytes > MAX_BATCH_BYTES:
        raise ValueError("RESOURCE_EXHAUSTED: handler result too large")
    return result


def main():
    # A dedicated response descriptor keeps even os.write(1, ...) out of IPC.
    response = os.fdopen(int(os.environ.pop("PARO_RESPONSE_FD")), "wb", buffering=0)
    request = decode(read_frame(sys.stdin.buffer, 1 << 20))
    definition = request["definition"]
    inputs, outputs = validate(definition)
    with open(os.devnull, "w") as discard, contextlib.redirect_stdout(discard), contextlib.redirect_stderr(discard):
        namespace = {"__name__": "__paro_udf__"}
        try:
            exec(compile(definition["source"], "<paro-udf>", "exec"), namespace)
            handler = namespace[definition["handler"]]
        except Exception as error:
            write_frame(response, encode(ok=False, error=f"{type(error).__name__}: {error}"[:8192]))
            return
        while True:
            data = read_frame(sys.stdin.buffer, MAX_BATCH_BYTES)
            if data is None:
                break
            try:
                batch = deserialize(data)
                if not batch.schema.equals(inputs, check_metadata=True):
                    raise ValueError("TYPE_CONTRACT: input schema mismatch")
                result = invoke(handler, definition, batch, outputs, request["id"])
                payload = serialize(result)
                write_frame(response, encode(ok=True))
                write_frame(response, payload)
            except Exception as error:
                write_frame(response, encode(ok=False, error=f"{type(error).__name__}: {error}"[:8192]))
                break


if __name__ == "__main__":
    main()
