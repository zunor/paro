#!/usr/bin/env python3
# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Reproducibility and paired-statistics helpers for corpus benchmarks."""

from __future__ import annotations

import hashlib
import os
import random
import socket
import statistics
import subprocess
import time
from pathlib import Path
from typing import Any, Sequence


def content_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def tree_digest(root: Path, include: tuple[str, ...] | None = None) -> str:
    digest = hashlib.sha256()
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        if include is not None and path.suffix.lower() not in include:
            continue
        digest.update(str(path.relative_to(root)).encode("utf-8"))
        digest.update(b"\0")
        digest.update(bytes.fromhex(content_digest(path)))
    return digest.hexdigest()


def repository_identity(root: Path) -> dict[str, Any]:
    def git_bytes(*arguments: str) -> bytes:
        return subprocess.check_output(
            ["git", *arguments], cwd=root, stderr=subprocess.DEVNULL
        )

    try:
        commit = git_bytes("rev-parse", "HEAD").decode().strip()
        status = git_bytes("status", "--porcelain=v1", "-z", "--untracked-files=all")
        patch = git_bytes("diff", "--binary", "HEAD")
        digest = hashlib.sha256(patch)
        paths = []
        entries = [entry for entry in status.split(b"\0") if entry]
        for entry in entries:
            text = entry.decode("utf-8", errors="surrogateescape")
            paths.append(text)
            if text.startswith("?? "):
                path = root / text[3:]
                if path.is_file():
                    digest.update(text[3:].encode("utf-8"))
                    digest.update(bytes.fromhex(content_digest(path)))
        return {
            "commit": commit,
            "dirty": bool(entries),
            "status": paths,
            "working_tree_sha256": digest.hexdigest(),
        }
    except (OSError, subprocess.CalledProcessError):
        return {
            "commit": None,
            "dirty": None,
            "status": [],
            "working_tree_sha256": None,
        }


def paired_order_balanced_ratio(
    paro_samples: Sequence[float],
    duckdb_samples: Sequence[float],
    paro_ran_first: Sequence[bool],
    bootstrap_samples: int = 10_000,
) -> dict[str, Any]:
    buckets = {"paro_first": [], "paro_second": []}
    for paro_ms, duckdb_ms, ran_first in zip(
        paro_samples, duckdb_samples, paro_ran_first
    ):
        buckets["paro_first" if ran_first else "paro_second"].append(
            paro_ms / duckdb_ms
        )
    non_empty = [ratios for ratios in buckets.values() if ratios]
    if not non_empty:
        raise ValueError("paired comparison has no samples")

    def statistic(sampled: dict[str, list[float]]) -> float:
        medians = [statistics.median(values) for values in sampled.values() if values]
        return statistics.geometric_mean(medians)

    ratio = statistic(buckets)
    rng = random.Random(0)
    bootstrap = []
    for _ in range(bootstrap_samples):
        sampled = {
            name: [values[rng.randrange(len(values))] for _ in values] if values else []
            for name, values in buckets.items()
        }
        bootstrap.append(statistic(sampled))
    bootstrap.sort()
    low = bootstrap[int(0.025 * (len(bootstrap) - 1))]
    high = bootstrap[int(0.975 * (len(bootstrap) - 1))]
    return {
        "ratio": round(ratio, 6),
        "paired_confidence_interval_95": [round(low, 6), round(high, 6)],
        "order_medians": {
            name: round(statistics.median(values), 6)
            for name, values in buckets.items()
            if values
        },
        "samples": {name: len(values) for name, values in buckets.items()},
        "bootstrap_samples": bootstrap_samples,
    }


def hierarchical_abba_ratio(
    blocks: Sequence[dict[str, Any]],
    bootstrap_samples: int = 10_000,
) -> dict[str, Any]:
    """Estimate a ratio without pretending same-process samples are IID.

    The top-level resampling unit is a fresh process block. Samples within a
    selected block are resampled only after that block has been selected, so
    process-level cache/layout effects remain correlated.
    """
    if not blocks:
        raise ValueError("ABBA comparison has no process blocks")
    normalized = []
    for block in blocks:
        paro = [float(value) for value in block.get("paro_ms", [])]
        duckdb = [float(value) for value in block.get("duckdb_ms", [])]
        if len(paro) < 2 or len(paro) != len(duckdb) or len(paro) % 2 != 0:
            raise ValueError(
                "each process block must contain the same positive number of "
                "complete ABBA-round samples per engine"
            )
        if any(value <= 0.0 for value in [*paro, *duckdb]):
            raise ValueError("ABBA timings must be positive")
        normalized.append((paro, duckdb))

    def block_ratio(paro: Sequence[float], duckdb: Sequence[float]) -> float:
        return statistics.geometric_mean(paro) / statistics.geometric_mean(duckdb)

    observed_blocks = [block_ratio(paro, duckdb) for paro, duckdb in normalized]
    ratio = statistics.geometric_mean(observed_blocks)
    rng = random.Random(0)
    bootstrap = []
    for _ in range(bootstrap_samples):
        sampled_ratios = []
        for _ in normalized:
            paro, duckdb = normalized[rng.randrange(len(normalized))]
            sampled_paro = [paro[rng.randrange(len(paro))] for _ in paro]
            sampled_duckdb = [duckdb[rng.randrange(len(duckdb))] for _ in duckdb]
            sampled_ratios.append(block_ratio(sampled_paro, sampled_duckdb))
        bootstrap.append(statistics.geometric_mean(sampled_ratios))
    bootstrap.sort()
    low = bootstrap[int(0.025 * (len(bootstrap) - 1))]
    high = bootstrap[int(0.975 * (len(bootstrap) - 1))]
    return {
        "ratio": round(ratio, 6),
        "hierarchical_confidence_interval_95": [round(low, 6), round(high, 6)],
        "process_block_ratios": [round(value, 6) for value in observed_blocks],
        "process_blocks": len(normalized),
        "samples_per_engine": sum(len(paro) for paro, _ in normalized),
        "bootstrap_samples": bootstrap_samples,
        "resampling_unit": "fresh_process_block_then_within_block_sample",
    }


def build_benchmark_server(
    repo_root: Path, jobs: int, *, features: tuple[str, ...] = ()
) -> tuple[Path, dict[str, Any]]:
    """Build the exact server image used by the owned benchmark process."""
    before = repository_identity(repo_root)
    command = ["cargo", "build", "--release", "--locked", "--bin", "parod"]
    if features:
        command.extend(["--features", ",".join(features)])
    environment = os.environ.copy()
    environment["CARGO_BUILD_JOBS"] = str(max(1, jobs))
    started = time.perf_counter_ns()
    subprocess.run(command, cwd=repo_root, env=environment, check=True)
    elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000
    after = repository_identity(repo_root)
    if before != after:
        raise RuntimeError("source tree changed while building benchmark server")
    binary = (repo_root / "target/release/parod").resolve()
    if not binary.is_file():
        raise RuntimeError(f"cargo did not produce {binary}")
    return binary, {
        "command": command,
        "cargo": subprocess.check_output(["cargo", "--version"], text=True).strip(),
        "jobs": max(1, jobs),
        "elapsed_ms": round(elapsed_ms, 3),
        "source": after,
        "binary_path": str(binary),
        "binary_sha256": content_digest(binary),
    }


class ManagedParoServer:
    def __init__(
        self,
        binary: Path,
        data_dir: Path,
        listen: str,
        log_path: Path,
        *,
        max_memory: str,
        threads: int,
    ) -> None:
        self.binary = binary.resolve()
        self.data_dir = data_dir.resolve()
        self.listen = listen
        self.log_path = log_path.resolve()
        self.max_memory = max_memory
        self.threads = max(1, threads)
        self.process: subprocess.Popen[bytes] | None = None
        self._log = None
        self._started_ns: int | None = None

    def start(self) -> None:
        if not self.binary.is_file() or not os.access(self.binary, os.X_OK):
            raise ValueError(f"Paro server binary is not executable: {self.binary}")
        if not self.data_dir.is_dir():
            raise ValueError(f"Paro data directory does not exist: {self.data_dir}")
        host, port_text = self.listen.rsplit(":", 1)
        port = int(port_text)
        try:
            with socket.create_connection((host, port), timeout=0.1):
                raise RuntimeError(
                    f"refusing to start benchmark server on occupied address {self.listen}"
                )
        except (ConnectionRefusedError, TimeoutError, OSError):
            pass
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self._log = self.log_path.open("wb")
        self.process = subprocess.Popen(
            [
                str(self.binary),
                "--data-dir",
                str(self.data_dir),
                "--listen",
                self.listen,
                "--max-memory",
                self.max_memory,
                "--threads",
                str(self.threads),
                "--log-level",
                "warn",
            ],
            stdout=self._log,
            stderr=subprocess.STDOUT,
            cwd=self.binary.parent,
        )
        self._started_ns = time.time_ns()
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(
                    f"Paro server exited with {self.process.returncode}; see {self.log_path}"
                )
            try:
                with socket.create_connection((host, port), timeout=0.25):
                    return
            except OSError:
                time.sleep(0.05)
        raise TimeoutError(f"Paro server did not listen on {self.listen}")

    def stop(self) -> None:
        process = self.process
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        if self._log is not None:
            self._log.close()

    def identity(self) -> dict[str, Any]:
        if self.process is None or self.process.poll() is not None:
            raise RuntimeError("owned Paro server is not running")
        return {
            "pid": self.process.pid,
            "path": str(self.binary),
            "sha256": content_digest(self.binary),
            "data_dir": str(self.data_dir),
            "listen": self.listen,
            "log": str(self.log_path),
            "owned_by_harness": True,
            "launch_argv": list(self.process.args),
            "started_unix_ns": self._started_ns,
            "binary_stat": {
                "device": self.binary.stat().st_dev,
                "inode": self.binary.stat().st_ino,
                "size": self.binary.stat().st_size,
                "mtime_ns": self.binary.stat().st_mtime_ns,
            },
        }

    def __enter__(self) -> "ManagedParoServer":
        try:
            self.start()
        except Exception:
            self.stop()
            raise
        return self

    def __exit__(self, *_: Any) -> None:
        self.stop()
