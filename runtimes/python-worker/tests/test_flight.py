# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Real Arrow consumers exercise the service boundary and process lifecycle."""

import base64
import contextlib
import hashlib
import json
import itertools
import os
import secrets
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pyarrow as pa
import pyarrow.flight as flight

from paro_worker.protocol import encode


def wait_reaped(pid):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.01)
    raise AssertionError(f"handler {pid} was not reaped")


def wait_handler(marker, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            return int(marker.read_text())
        except (FileNotFoundError, ValueError):
            # write_text creates the file before writing the PID.
            time.sleep(0.01)
    raise AssertionError("handler did not publish its PID")


class FlightTests(unittest.TestCase):
    sequence = itertools.count(1)
    @classmethod
    def setUpClass(cls):
        cls.token, cls.epoch = secrets.token_hex(32), secrets.token_hex(8)
        cls.process = subprocess.Popen(
            [sys.executable, "-u", "-m", "paro_worker"],
            env={**os.environ, "PARO_WORKER_TOKEN": cls.token, "PARO_WORKER_EPOCH": cls.epoch},
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        line = cls.process.stdout.readline()
        if not line:
            cls.process.kill()
            raise AssertionError(cls.process.stderr.read().decode())
        cls.port = json.loads(line)["port"]
        cls.client = flight.connect(f"grpc://127.0.0.1:{cls.port}")

    @classmethod
    def tearDownClass(cls):
        cls.client.close()
        cls.process.stdin.close()
        try:
            cls.process.wait(timeout=10)
        finally:
            if cls.process.poll() is None:
                cls.process.kill()
                cls.process.wait()
            cls.process.stdout.close()
            cls.process.stderr.close()

    def control(self, **values):
        return encode(token=self.token, epoch=self.epoch, **values)

    def definition(self, source, mode="vector", strict=False):
        input_schema = pa.schema([pa.field("arg0", pa.int32())])
        output_schema = pa.schema([pa.field("result", pa.int32())])
        return {
            "routine_id": 12, "generation": 3, "owner": "tester",
            "source": source, "digest": hashlib.sha256(source.encode()).hexdigest(),
            "handler": "main", "mode": mode, "strict": strict,
            "input_schema": base64.b64encode(input_schema.serialize()).decode(),
            "output_schema": base64.b64encode(output_schema.serialize()).decode(),
        }, input_schema, output_schema

    def action(self, name, **payload):
        return [json.loads(result.body.to_pybytes()) for result in
                self.client.do_action(flight.Action(name, self.control(**payload)))]

    @contextlib.contextmanager
    def isolated_service(self, setup):
        program = (
            "import json, os\n"
            "import paro_worker.server as worker\n"
            + setup + "\n"
            "server = worker.Server(os.environ['PARO_WORKER_TOKEN'], os.environ['PARO_WORKER_EPOCH'])\n"
            "print(json.dumps({'port': server.port}), flush=True)\n"
            "server.serve()\n"
        )
        with subprocess.Popen(
            [sys.executable, "-u", "-c", program],
            env={**os.environ, "PARO_WORKER_TOKEN": self.token, "PARO_WORKER_EPOCH": self.epoch},
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        ) as process:
            try:
                ready = json.loads(process.stdout.readline())
                with flight.connect(f"grpc://127.0.0.1:{ready['port']}") as client:
                    yield process, client
            finally:
                if process.poll() is None:
                    process.terminate()
                process.wait(timeout=5)

    def exchange(self, definition, batch):
        identifier = f"{self.epoch}-{next(self.sequence)}"
        fence = [definition["routine_id"], definition["generation"], definition["digest"]]
        descriptor = flight.FlightDescriptor.for_command(self.control(
            id=identifier, fence=fence, definition=definition,
        ))
        writer, reader = self.client.do_exchange(descriptor)
        writer.begin(batch.schema)
        writer.write_with_metadata(batch, encode(sequence=0))
        writer.done_writing()
        output = []
        try:
            for chunk in reader:
                metadata = json.loads(chunk.app_metadata.to_pybytes())
                if chunk.data is not None:
                    output.append(chunk.data)
                    self.action("ack", id=identifier, fence=fence, sequence=metadata["sequence"])
                elif metadata["kind"] == "finish":
                    self.action("finish", id=identifier, fence=fence)
        finally:
            writer.close()
        return output

    def test_vector_compute_is_real_arrow(self):
        definition, inputs, _ = self.definition(
            "import pyarrow.compute as pc\ndef main(ctx, values):\n    return pc.add(values, 1).cast(values.type)"
        )
        batch = pa.RecordBatch.from_arrays([pa.array([1, None, 8], type=pa.int32())], schema=inputs)
        self.assertEqual(self.exchange(definition, batch)[0].column(0).to_pylist(), [2, None, 9])

    def test_scalar_strict_skips_nulls(self):
        definition, inputs, _ = self.definition(
            "def main(ctx, value):\n    return value + 7", "scalar", True
        )
        batch = pa.RecordBatch.from_arrays([pa.array([1, None, 8], type=pa.int32())], schema=inputs)
        self.assertEqual(self.exchange(definition, batch)[0].column(0).to_pylist(), [8, None, 15])

    def test_print_cannot_corrupt_transport(self):
        definition, inputs, _ = self.definition(
            "import os\ndef main(ctx, values):\n    os.write(1, b'noise')\n    print('noise')\n    return values"
        )
        batch = pa.RecordBatch.from_arrays([pa.array([2], type=pa.int32())], schema=inputs)
        self.assertEqual(self.exchange(definition, batch)[0].column(0).to_pylist(), [2])

    def test_vector_shape_is_checked(self):
        definition, inputs, _ = self.definition(
            "def main(ctx, values):\n    return values.slice(0, 1)"
        )
        batch = pa.RecordBatch.from_arrays([pa.array([2, 3], type=pa.int32())], schema=inputs)
        with self.assertRaisesRegex(flight.FlightServerError, "output row count"):
            self.exchange(definition, batch)

    def test_validation_does_not_execute_module(self):
        definition, _, _ = self.definition(
            "raise RuntimeError('must not execute')\ndef main(ctx, values):\n    return values"
        )
        self.assertTrue(self.action("validate", definition=definition)[0]["ok"])

    def test_forged_source_digest_is_rejected(self):
        definition, _, _ = self.definition("def main(ctx, values):\n    return values")
        definition["source"] += "\n# changed"
        with self.assertRaisesRegex(pa.ArrowInvalid, "digest mismatch"):
            self.action("validate", definition=definition)

    def test_cancel_before_admission_fences_late_open(self):
        definition, inputs, _ = self.definition("def main(ctx, values):\n    return values")
        identifier = f"{self.epoch}-{next(self.sequence)}"
        fence = [definition["routine_id"], definition["generation"], definition["digest"]]
        self.action("cancel", id=identifier, fence=fence)
        writer, reader = self.client.do_exchange(flight.FlightDescriptor.for_command(self.control(
            id=identifier, fence=fence, definition=definition,
        )))
        try:
            with self.assertRaisesRegex(pa.ArrowInvalid, "replay"):
                writer.begin(inputs)
                writer.done_writing()
                list(reader)
        finally:
            with contextlib.suppress(pa.ArrowException):
                writer.close()
        self.action("cancel", id=identifier, fence=fence)

    def test_cancel_while_waiting_for_result_ack_reaps_handler(self):
        with tempfile.TemporaryDirectory(prefix="paro-ack-cancel-") as directory:
            marker = Path(directory) / "pid"
            definition, inputs, _ = self.definition(
                "import os\nfrom pathlib import Path\n"
                f"def main(ctx, values):\n    Path({str(marker)!r}).write_text(str(os.getpid()))\n"
                "    return values"
            )
            identifier = f"{self.epoch}-{next(self.sequence)}"
            fence = [definition["routine_id"], definition["generation"], definition["digest"]]
            writer, reader = self.client.do_exchange(flight.FlightDescriptor.for_command(self.control(
                id=identifier, fence=fence, definition=definition,
            )))
            try:
                writer.begin(inputs)
                writer.write_with_metadata(pa.RecordBatch.from_arrays(
                    [pa.array([1], type=pa.int32())], schema=inputs), encode(sequence=0))
                writer.done_writing()
                reader.read_chunk()  # consumed
                self.assertEqual(reader.read_chunk().data.num_rows, 1)
                self.action("cancel", id=identifier, fence=fence)
                with self.assertRaises(ProcessLookupError):
                    os.kill(int(marker.read_text()), 0)
                with self.assertRaisesRegex(flight.FlightServerError, "CANCELLED"):
                    list(reader)
            finally:
                with contextlib.suppress(pa.ArrowException):
                    writer.close()

    def test_stalled_input_retires_service_after_deadline(self):
        definition, inputs, _ = self.definition("def main(ctx, values):\n    return values")
        with self.isolated_service("worker.TIMEOUT = 0.2\nworker.CLEANUP_TIMEOUT = 0.2") as (process, client):
            writer, reader = client.do_exchange(flight.FlightDescriptor.for_command(self.control(
                id=f"{self.epoch}-1",
                fence=[definition["routine_id"], definition["generation"], definition["digest"]],
                definition=definition,
            )))
            try:
                writer.begin(inputs)
                self.assertEqual(reader.schema.names, ["result"])
                self.assertEqual(process.wait(timeout=5), 70)
                with self.assertRaises(pa.ArrowException):
                    list(reader)
            finally:
                with contextlib.suppress(pa.ArrowException):
                    writer.close()

    def test_cleanup_failure_retires_service_and_revokes_handler(self):
        setup = (
            "def broken_close(self):\n"
            "    raise RuntimeError('injected cleanup failure')\n"
            "worker.HandlerProcess.close = broken_close\n"
        )
        with tempfile.TemporaryDirectory(prefix="paro-cleanup-fault-") as directory:
            marker = Path(directory) / "pid"
            definition, inputs, _ = self.definition(
                "import os\nfrom pathlib import Path\n"
                f"def main(ctx, values):\n    Path({str(marker)!r}).write_text(str(os.getpid()))\n"
                "    return values"
            )
            identifier = f"{self.epoch}-1"
            fence = [definition["routine_id"], definition["generation"], definition["digest"]]
            with self.isolated_service(setup) as (process, client):
                writer, reader = client.do_exchange(flight.FlightDescriptor.for_command(self.control(
                    id=identifier, fence=fence, definition=definition,
                )))
                try:
                    writer.begin(inputs)
                    writer.write_with_metadata(pa.RecordBatch.from_arrays(
                        [pa.array([1], type=pa.int32())], schema=inputs), encode(sequence=0))
                    writer.done_writing()
                    reader.read_chunk()
                    reader.read_chunk()
                    list(client.do_action(flight.Action("ack", self.control(
                        id=identifier, fence=fence, sequence=0))))
                    self.assertEqual(json.loads(reader.read_chunk().app_metadata.to_pybytes())["kind"], "finish")
                    with self.assertRaises(pa.ArrowException):
                        list(client.do_action(flight.Action("finish", self.control(id=identifier, fence=fence))))
                    self.assertEqual(process.wait(timeout=5), 70)
                    wait_reaped(int(marker.read_text()))
                finally:
                    with contextlib.suppress(pa.ArrowException):
                        writer.close()
    def test_batches_acknowledgement_fence_and_finish(self):
        definition, inputs, _ = self.definition("def main(ctx, values):\n    return values")
        identifier = f"{self.epoch}-{next(self.sequence)}"
        fence = [definition["routine_id"], definition["generation"], definition["digest"]]
        descriptor = flight.FlightDescriptor.for_command(self.control(
            id=identifier, fence=fence, definition=definition,
        ))
        writer, reader = self.client.do_exchange(descriptor)
        writer.begin(inputs)
        try:
            for sequence in range(2):
                batch = pa.RecordBatch.from_arrays([pa.array([sequence], type=pa.int32())], schema=inputs)
                writer.write_with_metadata(batch, encode(sequence=sequence))
                consumed = reader.read_chunk()
                self.assertEqual(json.loads(consumed.app_metadata.to_pybytes())["kind"], "consumed")
                result = reader.read_chunk()
                self.assertEqual(result.data.column(0).to_pylist(), [sequence])
                with self.assertRaisesRegex(pa.ArrowInvalid, "generation mismatch"):
                    self.action("ack", id=identifier, fence=[12, 999, definition["digest"]], sequence=sequence)
                with self.assertRaisesRegex(pa.ArrowInvalid, "ACK out of order"):
                    self.action("ack", id=identifier, fence=fence, sequence=sequence + 1)
                self.action("ack", id=identifier, fence=fence, sequence=sequence)
            writer.done_writing()
            self.assertEqual(json.loads(reader.read_chunk().app_metadata.to_pybytes())["kind"], "finish")
            self.action("finish", id=identifier, fence=fence)
            self.assertEqual(list(reader), [])
            self.action("finish", id=identifier, fence=fence)
        finally:
            writer.close()

    def test_cancel_reaps_handler_before_returning_capacity(self):
        with tempfile.TemporaryDirectory(prefix="paro-cancel-") as directory:
            marker = Path(directory) / "pid"
            definition, inputs, _ = self.definition(
                f"import os, time\nfrom pathlib import Path\n"
                f"def main(ctx, values):\n    Path({str(marker)!r}).write_text(str(os.getpid()))\n"
                "    time.sleep(60)\n    return values"
            )
            identifier = f"{self.epoch}-{next(self.sequence)}"
            fence = [definition["routine_id"], definition["generation"], definition["digest"]]
            writer, reader = self.client.do_exchange(flight.FlightDescriptor.for_command(self.control(
                id=identifier, fence=fence, definition=definition,
            )))
            writer.begin(inputs)
            writer.write_with_metadata(pa.RecordBatch.from_arrays(
                [pa.array([1], type=pa.int32())], schema=inputs), encode(sequence=0))
            writer.done_writing()
            with ThreadPoolExecutor(max_workers=1) as executor:
                pending = executor.submit(list, reader)
                pid = wait_handler(marker)
                self.action("cancel", id=identifier, fence=fence)
                with self.assertRaises(ProcessLookupError):
                    os.kill(pid, 0)
                with self.assertRaisesRegex(flight.FlightServerError, "CANCELLED"):
                    pending.result(timeout=5)
                self.action("cancel", id=identifier, fence=fence)
            with self.assertRaises(flight.FlightServerError):
                writer.close()

    def test_owner_crash_revokes_the_handler_lifetime(self):
        with tempfile.TemporaryDirectory(prefix="paro-owner-crash-") as directory:
            marker = Path(directory) / "pid"
            definition, _, _ = self.definition(
                f"import os, time\nfrom pathlib import Path\n"
                f"def main(ctx, values):\n    Path({str(marker)!r}).write_text(str(os.getpid()))\n"
                "    time.sleep(60)\n    return values"
            )
            program = (
                "import time, pyarrow as pa\n"
                "from paro_worker.process import HandlerProcess\n"
                "from paro_worker.definition import validate\n"
                f"definition = {definition!r}\n"
                "inputs, _ = validate(definition)\n"
                "batch = pa.RecordBatch.from_arrays([pa.array([1], type=pa.int32())], schema=inputs)\n"
                "owner = HandlerProcess(definition, 'crash-test')\n"
                "owner.execute(batch, lambda: False, time.monotonic() + 60)\n"
            )
            owner = subprocess.Popen(
                [sys.executable, "-c", program],
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            try:
                pid = wait_handler(marker)
                owner.kill()
                owner.wait(timeout=5)
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    try:
                        os.kill(pid, 0)
                    except ProcessLookupError:
                        break
                    time.sleep(0.01)
                else:
                    self.fail("handler outlived its owner's liveness pipe")
            finally:
                if owner.poll() is None:
                    owner.kill()
                owner.wait(timeout=5)

    def test_supervisor_termination_reaps_its_handler(self):
        from paro_worker.process import HandlerProcess

        with tempfile.TemporaryDirectory(prefix="paro-supervisor-stop-") as directory:
            marker = Path(directory) / "pid"
            definition, inputs, _ = self.definition(
                "import os, time\nfrom pathlib import Path\n"
                f"def main(ctx, values):\n    Path({str(marker)!r}).write_text(str(os.getpid()))\n"
                "    time.sleep(60)\n    return values"
            )
            owner = HandlerProcess(definition, "supervisor-stop")
            try:
                batch = pa.RecordBatch.from_arrays([pa.array([1], type=pa.int32())], schema=inputs)
                with ThreadPoolExecutor(max_workers=1) as executor:
                    pending = executor.submit(owner.execute, batch, lambda: False, time.monotonic() + 5)
                    pid = wait_handler(marker, timeout=4)
                    os.kill(owner.child.pid, signal.SIGTERM)
                    with self.assertRaisesRegex(RuntimeError, "handler exited"):
                        pending.result(timeout=5)
                    owner.close()
                    with self.assertRaises(ProcessLookupError):
                        os.kill(pid, 0)
            finally:
                owner.close()


if __name__ == "__main__":
    unittest.main()
