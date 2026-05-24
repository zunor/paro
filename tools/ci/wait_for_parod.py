# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Wait for a parod process to accept TCP connections."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import socket
import subprocess
import sys
import time


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--pid-file", type=Path, required=True)
    parser.add_argument("--timeout", type=int, required=True)
    parser.add_argument("--log", type=Path, required=True)
    args = parser.parse_args()

    pid = int(args.pid_file.read_text(encoding="utf-8").strip())
    deadline = time.time() + args.timeout
    while time.time() < deadline:
        try:
            os.kill(pid, 0)
        except OSError:
            _tail_log_and_exit(args.log, "parod exited before becoming ready")

        if _is_zombie(pid):
            _tail_log_and_exit(args.log, "parod became a zombie before becoming ready")

        try:
            with socket.create_connection((args.host, args.port), timeout=1):
                print(f"parod is ready on {args.host}:{args.port}")
                return 0
        except OSError:
            time.sleep(1)

    _tail_log_and_exit(args.log, "parod health check failed")
    return 1


def _is_zombie(pid: int) -> bool:
    proc_status = Path(f"/proc/{pid}/status")
    if proc_status.exists():
        try:
            for line in proc_status.read_text(encoding="utf-8", errors="replace").splitlines():
                if line.startswith("State:"):
                    return "\tZ" in line or " zombie" in line.lower()
        except OSError:
            return False
        return False

    try:
        state = subprocess.check_output(
            ("ps", "-p", str(pid), "-o", "state="),
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=1,
        ).strip()
    except (OSError, subprocess.SubprocessError):
        return False
    return state.startswith("Z")


def _tail_log_and_exit(log_path: Path, message: str) -> None:
    print(message, file=sys.stderr)
    if log_path.exists():
        print("--- parod.log (tail) ---", file=sys.stderr)
        print(
            "".join(log_path.read_text(encoding="utf-8", errors="replace").splitlines(True)[-80:]),
            file=sys.stderr,
            end="",
        )
    raise SystemExit(1)


if __name__ == "__main__":
    raise SystemExit(main())
