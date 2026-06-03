# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Platform and build fingerprints used by performance gate baselines."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import platform
import shlex
import subprocess
import tomllib
from typing import Any

from ..runtime_contract import runtime_contract_payload


UNKNOWN = "unknown"


class FingerprintError(ValueError):
    """Raised when a fingerprint cannot satisfy a gate contract."""


@dataclass(frozen=True)
class GateFingerprint:
    system: dict[str, Any]
    build: dict[str, Any]
    runtime: dict[str, Any]
    audit: dict[str, Any]


def platform_key() -> str:
    system = platform.system()
    machine = platform.machine().lower()
    if system == "Darwin" and machine in {"arm64", "aarch64"}:
        return "macos-arm64"
    if system == "Linux" and machine in {"x86_64", "amd64"}:
        return "linux-amd64"
    if system == "Linux" and machine in {"arm64", "aarch64"}:
        return "linux-arm64"
    return f"{system.lower()}-{machine}"


def collect_fingerprint(root_dir: Path, *, pid: int) -> GateFingerprint:
    command = _process_command(pid) if pid > 0 else ""
    return GateFingerprint(
        system=_system_fingerprint(),
        build=_build_fingerprint(command, root_dir),
        runtime=_runtime_fingerprint(command),
        audit={
            "parod_command": command,
            "runner_run_id": os.getenv("GITHUB_RUN_ID", UNKNOWN),
        },
    )


def validate_bless_fingerprint(
    fingerprint: GateFingerprint,
    *,
    require_fresh_runtime: bool = True,
) -> None:
    profile = fingerprint.build.get("cargo_profile")
    if profile != "release":
        raise FingerprintError(
            "gate bless requires a release build; set PARO_BENCH_CARGO_PROFILE=release "
            "or run against target/release/parod"
        )
    data_dir_mode = fingerprint.runtime.get("data_dir_mode")
    if require_fresh_runtime and data_dir_mode != "fresh":
        raise FingerprintError(
            "gate bless requires a fresh data directory; set PARO_BENCH_DATA_DIR_MODE=fresh "
            "when the benchmark server was started from a clean data dir"
        )


def _system_fingerprint() -> dict[str, Any]:
    return {
        "os": platform.system(),
        "release": platform.release(),
        "arch": platform.machine(),
        "cpu_model": platform.processor() or UNKNOWN,
        "cores": os.cpu_count() or 0,
        "memory_gb": _physical_memory_gb(),
    }


def _build_fingerprint(command: str, root_dir: Path) -> dict[str, Any]:
    rust_version = _command_output("rustc", "--version")
    rust_verbose = _command_output("rustc", "-vV")
    cargo_version = _command_output("cargo", "--version")
    target_triple = _rust_host_triple(rust_verbose)
    rustflags = shlex.split(os.getenv("RUSTFLAGS", ""))
    profile = os.getenv("PARO_BENCH_CARGO_PROFILE") or _profile_from_command(command)
    profile_settings = _cargo_profile_settings(root_dir, profile or UNKNOWN)
    target_cpu_mode = os.getenv("PARO_BENCH_TARGET_CPU_MODE", "default")
    linker = os.getenv("PARO_BENCH_LINKER") or _first_command_output(
        ("ld", "--version"),
        ("ld", "-v"),
        first_line=True,
    )
    return {
        "rust_toolchain": rust_version,
        "cargo": cargo_version,
        "target_triple": target_triple,
        "cargo_profile": profile or UNKNOWN,
        "cargo_features": _split_csv_env("PARO_BENCH_CARGO_FEATURES"),
        "opt_level": os.getenv("PARO_BENCH_OPT_LEVEL") or profile_settings["opt_level"],
        "lto": os.getenv("PARO_BENCH_LTO") or profile_settings["lto"],
        "codegen_units": os.getenv("PARO_BENCH_CODEGEN_UNITS") or profile_settings["codegen_units"],
        "panic": os.getenv("PARO_BENCH_PANIC") or profile_settings["panic"],
        "target_cpu_mode": target_cpu_mode,
        "target_cpu_resolved": os.getenv("PARO_BENCH_TARGET_CPU_RESOLVED")
        or ("default" if target_cpu_mode == "default" else UNKNOWN),
        "rustflags": rustflags,
        "linker": linker,
        "compiler_frontend": _command_output("cc", "--version", first_line=True),
    }


def _runtime_fingerprint(command: str) -> dict[str, Any]:
    data_dir_mode = os.getenv("PARO_BENCH_DATA_DIR_MODE") or os.getenv("PARO_DATA_DIR_MODE") or UNKNOWN
    return {
        "allocator": os.getenv("PARO_BENCH_ALLOCATOR", "system"),
        "parod_args": shlex.split(command) if command else [],
        "thread_pool": os.getenv("PARO_BENCH_THREAD_POOL", "default"),
        "data_dir_mode": data_dir_mode,
        **runtime_contract_payload(include_environment=True),
    }


def _physical_memory_gb() -> float | str:
    if platform.system() == "Darwin":
        output = _command_output("sysctl", "-n", "hw.memsize")
        try:
            return round(int(output) / (1024 ** 3), 2)
        except (TypeError, ValueError):
            return UNKNOWN
    meminfo = Path("/proc/meminfo")
    if meminfo.exists():
        for line in meminfo.read_text(encoding="utf-8", errors="ignore").splitlines():
            if line.startswith("MemTotal:"):
                parts = line.split()
                if len(parts) >= 2:
                    return round(int(parts[1]) / (1024 ** 2), 2)
    return UNKNOWN


def _process_command(pid: int) -> str:
    output = _command_output("ps", "-p", str(pid), "-o", "command=", first_line=True)
    return output if output != UNKNOWN else ""


def _command_output(*args: str, first_line: bool = False) -> str:
    try:
        output = subprocess.check_output(
            args,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=2,
        ).strip()
    except (OSError, subprocess.SubprocessError):
        return UNKNOWN
    if first_line:
        return output.splitlines()[0] if output else UNKNOWN
    return output or UNKNOWN


def _first_command_output(*commands: tuple[str, ...], first_line: bool = False) -> str:
    for command in commands:
        output = _command_output(*command, first_line=first_line)
        if output != UNKNOWN:
            return output
    return UNKNOWN


def _rust_host_triple(rust_verbose: str) -> str:
    for line in rust_verbose.splitlines():
        if line.startswith("host:"):
            return line.split(":", 1)[1].strip()
    return UNKNOWN


def _profile_from_command(command: str) -> str | None:
    if "target/release/parod" in command:
        return "release"
    if "target/debug/parod" in command:
        return "debug"
    return None


def _cargo_profile_settings(root_dir: Path, profile: str) -> dict[str, str]:
    defaults = _default_profile_settings(profile)
    manifest = root_dir.parent / "Cargo.toml"
    try:
        cargo = tomllib.loads(manifest.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError):
        return defaults
    profiles = cargo.get("profile")
    if not isinstance(profiles, dict):
        return defaults
    table = profiles.get(profile)
    if profile == "bench" and not isinstance(table, dict):
        table = profiles.get("release")
    if not isinstance(table, dict):
        return defaults
    return {
        "opt_level": _profile_value(table, "opt-level", defaults["opt_level"]),
        "lto": _profile_value(table, "lto", defaults["lto"]),
        "codegen_units": _profile_value(table, "codegen-units", defaults["codegen_units"]),
        "panic": _profile_value(table, "panic", defaults["panic"]),
    }


def _default_profile_settings(profile: str) -> dict[str, str]:
    if profile in {"release", "bench"}:
        return {
            "opt_level": "3",
            "lto": "false",
            "codegen_units": "16",
            "panic": "unwind",
        }
    if profile in {"dev", "test", "debug"}:
        return {
            "opt_level": "0",
            "lto": "false",
            "codegen_units": "256",
            "panic": "unwind",
        }
    return {
        "opt_level": UNKNOWN,
        "lto": UNKNOWN,
        "codegen_units": UNKNOWN,
        "panic": UNKNOWN,
    }


def _profile_value(table: dict[str, Any], key: str, default: str) -> str:
    value = table.get(key, default)
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, (int, float, str)):
        return str(value)
    return default


def _split_csv_env(key: str) -> list[str]:
    value = os.getenv(key, "")
    return [item.strip() for item in value.split(",") if item.strip()]
