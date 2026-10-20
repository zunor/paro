# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""One local Flight service, with bounded invocations and isolated user heaps."""

import hmac
import os
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field

import pyarrow.flight as flight

from .definition import validate
from .process import HandlerProcess
from .protocol import (
    MAX_BATCH_BYTES, MAX_BATCH_ROWS, MAX_HANDLERS, MAX_INVOCATION_ROWS,
    MAX_OWNER_HANDLERS, MAX_RESULT_BYTES, MAX_TERMINALS, TIMEOUT,
    capabilities, decode, encode, invocation_fence,
)

CLEANUP_TIMEOUT = 10.0


@dataclass
class Invocation:
    owner: str
    fence: tuple
    cancelled: threading.Event = field(default_factory=threading.Event)
    cleaned: threading.Event = field(default_factory=threading.Event)
    condition: threading.Condition = field(default_factory=threading.Condition)
    produced: int = -1
    acknowledged: int = -1
    finishing: bool = False
    finished: bool = False
    process: object = None
    deadline: float = field(default_factory=lambda: time.monotonic() + TIMEOUT)
    cancel_started: float = None


class Server(flight.FlightServerBase):
    def __init__(self, token, epoch):
        super().__init__("grpc://127.0.0.1:0")
        self.token = token
        self.epoch = epoch
        self.lock = threading.Lock()
        self.active = {}
        self.terminal = OrderedDict()
        self.owners = {}
        self.stopping = False
        self.highest_sequence = 0
        self.retired_through = 0
        self.validation = threading.BoundedSemaphore(MAX_HANDLERS)
        self.monitor_stopped = threading.Event()
        self.monitor = threading.Thread(target=self.watch_lifetimes, daemon=True)
        self.monitor.start()

    def abort(self):
        # A stuck Flight read/write cannot be interrupted through PyArrow's
        # server API. Retire this service rather than silently returning quota
        # while its callback or supervisor still owns work. Lease EOF kills
        # handlers; Rust observes the lost epoch and creates a fresh service.
        os._exit(70)

    def watch_lifetimes(self):
        while not self.monitor_stopped.wait(0.05):
            now = time.monotonic()
            with self.lock:
                for invocation in self.active.values():
                    if now >= invocation.deadline:
                        invocation.cancelled.set()
                    if invocation.cancelled.is_set():
                        if invocation.cancel_started is None:
                            invocation.cancel_started = now
                        if now >= invocation.cancel_started + CLEANUP_TIMEOUT:
                            self.abort()

    def control(self, data):
        value = decode(data)
        if not hmac.compare_digest(str(value.pop("token", "")), self.token):
            raise ValueError("PROTOCOL: invalid service token")
        if value.pop("epoch", None) != self.epoch:
            raise ValueError("PROTOCOL: stale worker epoch")
        return value

    def validate_definition(self, definition):
        if not self.validation.acquire(blocking=False):
            raise ValueError("RESOURCE_EXHAUSTED: definition validation capacity")
        try:
            return validate(definition)
        finally:
            self.validation.release()

    def do_action(self, context, action):
        value = self.control(action.body)
        if action.type == "capabilities":
            yield flight.Result(encode(**{k: v for k, v in capabilities(self.epoch).items() if k != "version"}))
            return
        if action.type == "validate":
            self.validate_definition(value["definition"])
            yield flight.Result(encode(ok=True))
            return
        key = value["id"]
        fence = invocation_fence(value["fence"])
        with self.lock:
            ordinal = self.ordinal(key)
            invocation = self.active.get(key)
            terminal = self.terminal.get(key)
            # Cancellation can win the race with DoExchange admission. Record
            # it under the same lock so a late open can never start user code.
            if invocation is None and terminal is None and action.type == "cancel":
                if ordinal > self.retired_through:
                    self.advance_window(ordinal)
                    self.terminal[key] = (fence, "failed")
                terminal = (fence, "failed")
        if invocation is None:
            if terminal == (fence, "success") and action.type == "finish":
                yield flight.Result(encode(ok=True))
                return
            if terminal is not None and terminal[0] == fence and action.type == "cancel":
                yield flight.Result(encode(ok=True))
                return
            raise ValueError("PROTOCOL: unknown or expired invocation")
        if invocation.fence != fence:
            raise ValueError("PROTOCOL: invocation generation mismatch")
        with invocation.condition:
            if action.type == "cancel":
                invocation.cancelled.set()
            elif action.type == "ack":
                sequence = value["sequence"]
                if type(sequence) is not int or sequence < 0 or sequence != invocation.produced or sequence < invocation.acknowledged:
                    raise ValueError("PROTOCOL: result ACK out of order")
                invocation.acknowledged = sequence
            elif action.type == "finish":
                if not invocation.finishing or invocation.acknowledged != invocation.produced:
                    raise ValueError("PROTOCOL: premature Finish")
                invocation.finished = True
            else:
                raise ValueError("PROTOCOL: unknown action")
            invocation.condition.notify_all()
        if action.type in ("cancel", "finish") and not invocation.cleaned.wait(CLEANUP_TIMEOUT):
            self.abort()
            raise RuntimeError("WORKER_FAILURE: handler cleanup did not complete")
        yield flight.Result(encode(ok=True))

    def ordinal(self, key):
        prefix = self.epoch + "-"
        if not isinstance(key, str) or len(key) > 128 or not key.startswith(prefix):
            raise ValueError("PROTOCOL: invalid invocation ordinal")
        suffix = key[len(prefix):]
        if not suffix.isascii() or not suffix.isdecimal():
            raise ValueError("PROTOCOL: invalid invocation ordinal")
        ordinal = int(suffix)
        if suffix != str(ordinal) or not 0 < ordinal < 1 << 64:
            raise ValueError("PROTOCOL: invalid invocation ordinal")
        return ordinal

    def advance_window(self, ordinal):
        self.highest_sequence = max(self.highest_sequence, ordinal)
        self.retired_through = max(0, self.highest_sequence - MAX_TERMINALS)
        for retired in list(self.terminal):
            if self.ordinal(retired) <= self.retired_through:
                del self.terminal[retired]

    def admit(self, key, fence, owner):
        with self.lock:
            # Epoch-scoped increasing ordinals give a bounded replay window.
            # Out-of-order concurrent opens are allowed within the window; retired
            # ordinals stay fenced even after their terminal record is evicted.
            ordinal = self.ordinal(key)
            if ordinal <= self.retired_through:
                raise ValueError("PROTOCOL: retired invocation ordinal")
            if self.stopping or len(self.active) >= MAX_HANDLERS:
                raise ValueError("RESOURCE_EXHAUSTED: worker invocation capacity")
            if key in self.active or key in self.terminal:
                raise ValueError("PROTOCOL: invocation replay")
            if self.owners.get(owner, 0) >= MAX_OWNER_HANDLERS:
                raise ValueError("RESOURCE_EXHAUSTED: owner handler capacity")
            self.advance_window(ordinal)
            invocation = Invocation(owner, fence)
            self.active[key] = invocation
            self.owners[owner] = self.owners.get(owner, 0) + 1
            return invocation

    def do_exchange(self, context, descriptor, reader, writer):
        opening = self.control(descriptor.command)
        definition = opening["definition"]
        inputs, outputs = self.validate_definition(definition)
        key, fence = opening["id"], invocation_fence(opening["fence"])
        if (not isinstance(key, str) or len(key) > 128
                or fence != (definition["routine_id"], definition["generation"], definition["digest"])):
            raise ValueError("PROTOCOL: invalid invocation identity")
        invocation = self.admit(key, fence, definition["owner"])
        deadline = invocation.deadline
        success = False
        rows = result_bytes = 0
        sequence = 0
        try:
            if not reader.schema.equals(inputs, check_metadata=True):
                raise ValueError("TYPE_CONTRACT: input schema mismatch")
            writer.begin(outputs)
            for chunk in reader:
                if context.is_cancelled() or invocation.cancelled.is_set():
                    raise RuntimeError("CANCELLED: invocation cancelled")
                batch = chunk.data
                message = decode(chunk.app_metadata)
                if batch is None or type(message.get("sequence")) is not int or message.get("sequence") != sequence:
                    raise ValueError("PROTOCOL: unexpected input sequence")
                if batch.num_rows > MAX_BATCH_ROWS or batch.nbytes > MAX_BATCH_BYTES:
                    raise ValueError("RESOURCE_EXHAUSTED: input batch limit")
                rows += batch.num_rows
                if rows > MAX_INVOCATION_ROWS:
                    raise ValueError("RESOURCE_EXHAUSTED: invocation row limit")
                if invocation.process is None:
                    invocation.process = HandlerProcess(definition, key)
                output = invocation.process.execute(
                    batch, lambda: invocation.cancelled.is_set() or context.is_cancelled(), deadline
                )
                if not output.schema.equals(outputs, check_metadata=True):
                    raise ValueError("TYPE_CONTRACT: result schema mismatch")
                if definition["mode"] != "table" and output.num_rows != batch.num_rows:
                    raise ValueError("TYPE_CONTRACT: result row count mismatch")
                result_bytes += output.nbytes
                if result_bytes > MAX_RESULT_BYTES:
                    raise ValueError("RESOURCE_EXHAUSTED: invocation result limit")
                writer.write_metadata(encode(kind="consumed", sequence=sequence))
                with invocation.condition:
                    invocation.produced = sequence
                writer.write_with_metadata(output, encode(kind="result", sequence=sequence))
                with invocation.condition:
                    while invocation.acknowledged != sequence:
                        self.wait(invocation, context, deadline)
                if invocation.process.exhausted():
                    invocation.process.close()
                    invocation.process = None
                sequence += 1
            # EndInput is the normal Flight send-half-close.
            with invocation.condition:
                invocation.finishing = True
            writer.write_metadata(encode(kind="finish", sequence=sequence))
            with invocation.condition:
                while not invocation.finished:
                    self.wait(invocation, context, deadline)
            success = True
        finally:
            # Quota stays charged until the owned process tree has been reaped.
            try:
                if invocation.process is not None:
                    invocation.process.close()
            except BaseException:
                self.abort()
                raise
            with self.lock:
                self.active.pop(key, None)
                self.owners[invocation.owner] -= 1
                if not self.owners[invocation.owner]:
                    del self.owners[invocation.owner]
                if int(key.removeprefix(self.epoch + "-")) > self.retired_through:
                    self.terminal[key] = (fence, "success" if success else "failed")
            invocation.cleaned.set()

    @staticmethod
    def wait(invocation, context, deadline):
        if invocation.cancelled.is_set() or context.is_cancelled():
            raise RuntimeError("CANCELLED: invocation cancelled")
        if time.monotonic() >= deadline:
            raise TimeoutError("TIMEOUT: invocation or result acknowledgement timed out")
        invocation.condition.wait(0.05)

    def stop(self):
        with self.lock:
            self.stopping = True
            for invocation in self.active.values():
                invocation.cancelled.set()
        self.shutdown()
        self.monitor_stopped.set()
        self.monitor.join()
