#!/usr/bin/env python3
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


class ManagedParoServer:
    def __init__(
        self,
        binary: Path,
        data_dir: Path,
        listen: str,
        log_path: Path,
    ) -> None:
        self.binary = binary.resolve()
        self.data_dir = data_dir.resolve()
        self.listen = listen
        self.log_path = log_path.resolve()
        self.process: subprocess.Popen[bytes] | None = None
        self._log = None

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
                "--log-level",
                "warn",
            ],
            stdout=self._log,
            stderr=subprocess.STDOUT,
            cwd=self.binary.parent,
        )
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
