# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Find and validate the parod process used by benchmark gates."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import re
import shlex
import subprocess
import tomllib


class ProcessProbeError(ValueError):
    """Raised when the benchmark harness cannot identify a valid parod process."""


@dataclass(frozen=True)
class ProcessProbe:
    pid: int
    command: str
    source: str


def resolve_parod_process(
    pid_value: str | int | None,
    *,
    root_dir: Path,
    require: bool,
) -> ProcessProbe:
    """Resolve a parod PID from CLI/env/pid-file/port and validate the command."""

    raw_value = str(pid_value if pid_value is not None else "auto").strip()
    if raw_value and raw_value not in {"auto", "0"}:
        return _probe_pid(raw_value, source="cli --pid")

    errors: list[str] = []
    env_pid = os.getenv("PARO_PID")
    if env_pid:
        try:
            return _probe_pid(env_pid, source="PARO_PID")
        except ProcessProbeError as exc:
            errors.append(str(exc))

    pid_file = root_dir.parent / ".ci" / "parod.pid"
    if pid_file.exists():
        try:
            return _probe_pid(pid_file.read_text(encoding="utf-8").strip(), source=str(pid_file))
        except (OSError, ProcessProbeError) as exc:
            errors.append(str(exc))

    host, port = _connection_target(root_dir)
    for pid in _listening_pids(port):
        try:
            return _probe_pid(str(pid), source=f"listen {host}:{port}")
        except ProcessProbeError as exc:
            errors.append(str(exc))

    if require:
        detail = f"; rejected candidates: {'; '.join(errors)}" if errors else ""
        raise ProcessProbeError(
            "parod pid is required for this gate; provide --pid, PARO_PID, "
            f"{pid_file}, or a detectable listener on {host}:{port}{detail}"
        )
    return ProcessProbe(pid=0, command="", source="none")


def _probe_pid(raw_pid: str, *, source: str) -> ProcessProbe:
    try:
        pid = int(raw_pid.strip())
    except ValueError as exc:
        raise ProcessProbeError(f"{source} is not a valid pid: {raw_pid!r}") from exc
    if pid <= 0:
        raise ProcessProbeError(f"{source} pid must be > 0")
    command = _process_command(pid)
    if not command:
        raise ProcessProbeError(f"{source} pid {pid} is not running or has no readable command")
    if not _looks_like_parod(command):
        raise ProcessProbeError(f"{source} pid {pid} is not parod: {command}")
    return ProcessProbe(pid=pid, command=command, source=source)


def _process_command(pid: int) -> str:
    try:
        return subprocess.check_output(
            ("ps", "-p", str(pid), "-o", "command="),
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=2,
        ).strip()
    except (OSError, subprocess.SubprocessError):
        return ""


def _looks_like_parod(command: str) -> bool:
    try:
        parts = shlex.split(command)
    except ValueError:
        parts = command.split()
    if not parts:
        return False
    executable = Path(parts[0]).name
    return executable == "parod" or executable.startswith("parod-")


def _connection_target(root_dir: Path) -> tuple[str, int]:
    config_path = root_dir / "config.toml"
    host = "localhost"
    port = 6432
    try:
        payload = tomllib.loads(config_path.read_text(encoding="utf-8"))
        connection = payload.get("connection")
        if isinstance(connection, dict):
            raw_host = connection.get("host")
            raw_port = connection.get("port")
            if isinstance(raw_host, str) and raw_host.strip():
                host = raw_host.strip()
            if isinstance(raw_port, int) and not isinstance(raw_port, bool):
                port = raw_port
    except (OSError, tomllib.TOMLDecodeError):
        pass
    host = os.getenv("PARO_HOST", host)
    raw_port = os.getenv("PARO_PORT")
    if raw_port:
        try:
            port = int(raw_port)
        except ValueError:
            pass
    return host, port


def _listening_pids(port: int) -> tuple[int, ...]:
    candidates = [*_lsof_pids(port), *_ss_pids(port)]
    result: list[int] = []
    for pid in candidates:
        if pid > 0 and pid not in result:
            result.append(pid)
    return tuple(result)


def _lsof_pids(port: int) -> tuple[int, ...]:
    try:
        output = subprocess.check_output(
            ("lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN", "-t"),
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.SubprocessError):
        return ()
    return tuple(_parse_positive_ints(output.splitlines()))


def _ss_pids(port: int) -> tuple[int, ...]:
    try:
        output = subprocess.check_output(
            ("ss", "-ltnp", f"sport = :{port}"),
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.SubprocessError):
        return ()
    return tuple(int(match.group(1)) for match in re.finditer(r"pid=(\d+)", output))


def _parse_positive_ints(values: list[str]) -> list[int]:
    result: list[int] = []
    for value in values:
        try:
            parsed = int(value.strip())
        except ValueError:
            continue
        if parsed > 0:
            result.append(parsed)
    return result
