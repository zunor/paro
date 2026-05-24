# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from harness.process_probe import ProcessProbeError, resolve_parod_process  # noqa: E402


class ProcessProbeTests(unittest.TestCase):
    def test_cli_pid_must_be_parod(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            with mock.patch("harness.process_probe.subprocess.check_output", return_value="/tmp/parod --listen"):
                probe = resolve_parod_process("42", root_dir=root, require=True)

            self.assertEqual(probe.pid, 42)
            self.assertEqual(probe.source, "cli --pid")

            with mock.patch("harness.process_probe.subprocess.check_output", return_value="/bin/sleep 1"):
                with self.assertRaisesRegex(ProcessProbeError, "not parod"):
                    resolve_parod_process("42", root_dir=root, require=True)

    def test_auto_pid_prefers_env_then_pid_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "benchmark"
            root.mkdir()
            pid_file = root.parent / ".ci" / "parod.pid"
            pid_file.parent.mkdir()
            pid_file.write_text("44", encoding="utf-8")

            def fake_check_output(command, **_kwargs):
                self.assertEqual(command[2], "43")
                return "/tmp/parod --listen"

            with mock.patch.dict("os.environ", {"PARO_PID": "43"}, clear=True):
                with mock.patch("harness.process_probe.subprocess.check_output", fake_check_output):
                    probe = resolve_parod_process("auto", root_dir=root, require=True)

            self.assertEqual(probe.pid, 43)
            self.assertEqual(probe.source, "PARO_PID")

    def test_auto_pid_can_probe_listening_port(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "config.toml").write_text(
                """
[connection]
host = "127.0.0.1"
port = 7654
""",
                encoding="utf-8",
            )

            def fake_check_output(command, **_kwargs):
                if command[0] == "lsof":
                    self.assertIn("-iTCP:7654", command)
                    return "55\n"
                if command[0] == "ps":
                    return "/tmp/parod --listen 127.0.0.1:7654"
                raise OSError(command)

            with mock.patch.dict("os.environ", {}, clear=True):
                with mock.patch("harness.process_probe.subprocess.check_output", fake_check_output):
                    probe = resolve_parod_process("auto", root_dir=root, require=True)

            self.assertEqual(probe.pid, 55)
            self.assertEqual(probe.source, "listen 127.0.0.1:7654")

    def test_optional_probe_returns_zero_when_not_found(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch("harness.process_probe.subprocess.check_output", side_effect=OSError):
                probe = resolve_parod_process("auto", root_dir=Path(tmp), require=False)

        self.assertEqual(probe.pid, 0)
        self.assertEqual(probe.source, "none")


if __name__ == "__main__":
    unittest.main()
