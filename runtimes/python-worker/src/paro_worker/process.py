# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Own handler processes, pipes, and cancellation through termination and reap."""

import os
import selectors
import subprocess
import sys
import time
from collections import deque

from .handler import LENGTH, deserialize, serialize
from .protocol import MAX_BATCH_BYTES, MAX_CONTROL_BYTES, decode, encode


class HandlerProcess:
    def __init__(self, definition, invocation):
        self.child = None
        self.response = self.liveness = None
        self.started = time.monotonic()
        self.batches = self.bytes = 0
        response_read = response_write = watch_read = None
        try:
            response_read, response_write = os.pipe()
            watch_read, self.liveness = os.pipe()
            env = {
                key: value for key, value in os.environ.items()
                if key in ("PATH", "SYSTEMROOT", "LANG", "LC_ALL", "TZ", "PYTHONPATH")
            }
            env["PARO_RESPONSE_FD"] = str(response_write)
            self.child = subprocess.Popen(
                [sys.executable, "-m", "paro_worker.watchdog", str(watch_read)],
                stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                env=env, start_new_session=True, pass_fds=(response_write, watch_read),
            )
            self.response = os.fdopen(response_read, "rb", buffering=0)
            response_read = None
            os.set_blocking(self.child.stdin.fileno(), False)
            os.set_blocking(self.response.fileno(), False)
            control = encode(definition=definition, id=invocation)
            self.initial = LENGTH.pack(len(control)) + control
        except BaseException:
            self.close()
            raise
        finally:
            for fd in (response_read, response_write, watch_read):
                if fd is not None:
                    os.close(fd)

    def execute(self, batch, cancelled, deadline):
        data = serialize(batch)
        outgoing = deque(memoryview(part) for part in (self.initial, LENGTH.pack(data.size), data) if len(part))
        self.initial = b""
        incoming = bytearray()
        metadata = None
        expected = None
        with selectors.DefaultSelector() as selector:
            selector.register(self.child.stdin, selectors.EVENT_WRITE, "write")
            selector.register(self.response, selectors.EVENT_READ, "read")
            while True:
                if cancelled():
                    raise RuntimeError("CANCELLED: Python invocation cancelled")
                if time.monotonic() >= deadline:
                    raise TimeoutError("TIMEOUT: Python invocation exceeded its deadline")
                for key, _ in selector.select(0.05):
                    if key.data == "write":
                        count = os.write(key.fd, outgoing[0][:65536])
                        outgoing[0] = outgoing[0][count:]
                        if not outgoing[0]:
                            outgoing.popleft()
                        if not outgoing:
                            selector.unregister(key.fileobj)
                    else:
                        piece = os.read(key.fd, 65536)
                        if not piece:
                            raise RuntimeError("WORKER_FAILURE: handler exited before completing its result")
                        incoming.extend(piece)
                        while True:
                            if expected is None:
                                if len(incoming) < LENGTH.size:
                                    break
                                expected = LENGTH.unpack(incoming[:LENGTH.size])[0]
                                del incoming[:LENGTH.size]
                                limit = MAX_CONTROL_BYTES if metadata is None else MAX_BATCH_BYTES
                                if expected > limit:
                                    raise ValueError("RESOURCE_EXHAUSTED: handler response exceeds frame limit")
                            if len(incoming) < expected:
                                break
                            frame = bytes(incoming[:expected])
                            del incoming[:expected]
                            expected = None
                            if metadata is None:
                                metadata = decode(frame)
                                if not metadata.get("ok"):
                                    raise RuntimeError("PYTHON_EXCEPTION: " + str(metadata.get("error", "")))
                            else:
                                self.batches += 1
                                self.bytes += data.size + len(frame)
                                return deserialize(frame)

    def exhausted(self):
        return self.batches >= 64 or self.bytes >= 64 << 20 or time.monotonic() - self.started >= 30

    def close(self):
        # Revoke ownership before any other cleanup operation can fail.
        if self.liveness is not None:
            os.close(self.liveness)
            self.liveness = None
        if self.child is not None:
            self.child.stdin.close()
        if self.child is not None:
            # The supervisor kills and reaps the user process before exiting.
            # Do not report capacity as free until that ownership is discharged.
            status = self.child.wait(timeout=10)
            if status != 0:
                raise RuntimeError("WORKER_FAILURE: handler supervisor failed to confirm cleanup")
            self.child = None
        if self.response is not None:
            self.response.close()
            self.response = None
