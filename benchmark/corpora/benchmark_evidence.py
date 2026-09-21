#!/usr/bin/env python3
# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Reproducibility and paired-statistics helpers for corpus benchmarks."""

from __future__ import annotations

import hashlib
from contextlib import contextmanager
from dataclasses import dataclass
import os
import json
import random
import re
import socket
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence


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


_STATEMENT_TRACE_FIELD = re.compile(
    r"(?P<key>process_id|session_id|operation_id|trace_sample_id|schema_version|"
    r"statement_id|statement_index|query_len|query_fingerprint|sequence|phase|event|"
    r"elapsed_us|duration_us|has_duration|value|has_value)=(?P<value>\"[^\"]*\"|\S+)"
)
_ANSI_ESCAPE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
STATEMENT_TRACE_SCHEMA_VERSION = 2


def parse_statement_trace_log(path: Path) -> list[dict[str, Any]]:
    """Decode trace events without repairing malformed ordering or identity."""
    traces: dict[tuple[int, int, int], dict[str, Any]] = {}
    if not path.is_file():
        return []
    for line_number, raw_line in enumerate(
        path.read_text(encoding="utf-8", errors="replace").splitlines(), start=1
    ):
        # tracing_subscriber may retain ANSI styling when stdout/stderr is
        # redirected to a harness log. Styling can wrap both field names and
        # separators, so normalize before applying the machine-field parser.
        line = _ANSI_ESCAPE.sub("", raw_line)
        if "paro::statement_trace" not in line:
            continue
        if "statement trace event" not in line:
            raise ValueError(f"line {line_number}: malformed statement trace record")
        record = line.split("statement trace event", 1)[1]
        matches = list(_STATEMENT_TRACE_FIELD.finditer(record))
        fields = {
            match.group("key"): match.group("value").strip('"') for match in matches
        }
        if len(fields) != len(matches):
            raise ValueError(f"line {line_number}: duplicate statement trace field")
        required = (
            "process_id", "session_id", "operation_id", "trace_sample_id",
            "schema_version", "statement_id", "statement_index", "query_len",
            "query_fingerprint", "sequence", "phase", "event", "elapsed_us",
            "duration_us", "has_duration", "value", "has_value",
        )
        missing = [key for key in required if key not in fields]
        if missing:
            raise ValueError(
                f"line {line_number}: missing statement trace fields: {', '.join(missing)}"
            )
        try:
            process_id = int(fields["process_id"])
            session_id = int(fields["session_id"])
            operation_id = int(fields["operation_id"])
            schema_version = int(fields["schema_version"])
            statement_id = int(fields["statement_id"])
            statement_index = int(fields["statement_index"])
            query_len = int(fields["query_len"])
            query_fingerprint = int(fields["query_fingerprint"])
            sequence = int(fields["sequence"])
            elapsed_us = int(fields["elapsed_us"])
            duration_us = (
                int(fields["duration_us"]) if fields["has_duration"] == "true" else None
            )
            value = int(fields["value"]) if fields["has_value"] == "true" else None
        except ValueError as error:
            raise ValueError(f"line {line_number}: invalid statement trace integer") from error
        if fields["has_duration"] not in ("true", "false"):
            raise ValueError(f"line {line_number}: invalid has_duration flag")
        if fields["has_value"] not in ("true", "false"):
            raise ValueError(f"line {line_number}: invalid has_value flag")
        if operation_id != statement_id:
            raise ValueError(f"line {line_number}: operation and statement identities differ")
        key = (process_id, session_id, statement_id)
        trace = traces.setdefault(
            key,
            {
                "process_id": process_id,
                "session_id": session_id,
                "operation_id": operation_id,
                "trace_sample_id": fields["trace_sample_id"],
                "schema_version": schema_version,
                "statement_id": statement_id,
                "statement_index": statement_index,
                "query_len": query_len,
                "query_fingerprint": query_fingerprint,
                "events": [],
            },
        )
        identity = {
            "trace_sample_id": fields["trace_sample_id"],
            "schema_version": schema_version,
            "statement_index": statement_index,
            "query_len": query_len,
            "query_fingerprint": query_fingerprint,
        }
        if any(trace[field] != value for field, value in identity.items()):
            raise ValueError(f"line {line_number}: statement trace identity changed")
        trace["events"].append({
            "sequence": sequence,
            "phase": fields["phase"],
            "event": fields["event"],
            "elapsed_us": elapsed_us,
            "duration_us": duration_us,
            "value": value,
        })
    return sorted(
        traces.values(),
        key=lambda trace: (trace["process_id"], trace["session_id"], trace["statement_id"]),
    )


def statement_fingerprint(value: str) -> int:
    """Match the deterministic FNV-1a fingerprint used by the Rust trace."""
    result = 0xCBF29CE484222325
    for byte in value.encode("utf-8"):
        result ^= byte
        result = (result * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return result


def fetch_compile_document(
    connection: Any,
    query: str,
    *,
    detail: bool = False,
    analyze: bool = False,
) -> tuple[str, dict[str, Any]]:
    """Fetch the typed EXPLAIN(COMPILE) document for one target statement.

    Corpus collectors use this single producer boundary for diagnostic
    material.  It deliberately does not run the target a second time and it
    never reconstructs a compile document from logs or ``paro_optimizers``
    rows.  The raw JSON is retained alongside the decoded object so a later
    validator can distinguish an encoding problem from a semantic one.
    """
    stripped = query.strip()
    while stripped.endswith(";"):
        stripped = stripped[:-1].rstrip()
    if not stripped:
        raise ValueError("compile document query is empty")
    options = ["COMPILE"]
    if analyze:
        options.append("ANALYZE")
    if detail:
        options.append("DETAIL")
    statement = f"EXPLAIN ({', '.join(options)}, FORMAT JSON) {stripped}"
    with connection.cursor() as cursor:
        cursor.execute(statement)
        rows = cursor.fetchall()
    payload = "\n".join(
        str(row[0]) for row in rows if row and row[0] is not None
    )
    if not payload.strip():
        raise ValueError("EXPLAIN (COMPILE) returned an empty document")
    try:
        document = json.loads(payload)
    except json.JSONDecodeError as error:
        raise ValueError("EXPLAIN (COMPILE) returned invalid JSON") from error
    if not isinstance(document, dict):
        raise ValueError("EXPLAIN (COMPILE) document is not an object")
    try:
        from benchmark.harness.receipt_contract import (
            ReceiptContractError,
            validate_compile_document,
        )
    except ModuleNotFoundError:  # pragma: no cover - script-only import path
        from harness.receipt_contract import ReceiptContractError, validate_compile_document
    try:
        validate_compile_document(document)
    except ReceiptContractError as error:
        raise ValueError(f"EXPLAIN (COMPILE) document violates its contract: {error}") from error
    return payload, document


def validate_statement_trace(
    trace: dict[str, Any],
    *,
    expected_process_id: int | None = None,
    expected_sample_id: str | None = None,
    expected_query_fingerprint: int | None = None,
    require_complete: bool = True,
) -> None:
    """Validate one protocol operation; never merge event names across traces."""
    if trace.get("schema_version") != STATEMENT_TRACE_SCHEMA_VERSION:
        raise ValueError("unsupported statement trace schema")
    if trace.get("operation_id") != trace.get("statement_id"):
        raise ValueError("operation identity does not match statement identity")
    if expected_process_id is not None and trace.get("process_id") != expected_process_id:
        raise ValueError("statement trace belongs to another process")
    if expected_sample_id is not None and trace.get("trace_sample_id") != expected_sample_id:
        raise ValueError("statement trace belongs to another sample")
    if (expected_query_fingerprint is not None
            and trace.get("query_fingerprint") != expected_query_fingerprint):
        raise ValueError("statement trace query identity differs from sample")
    events = trace.get("events")
    if not isinstance(events, list) or not events:
        raise ValueError("statement trace has no events")
    sequences = [event.get("sequence") for event in events]
    if sequences != list(range(len(events))):
        raise ValueError("statement trace sequence is not unique and contiguous")
    elapsed: list[int] = []
    positions: dict[str, list[int]] = {}
    for index, event in enumerate(events):
        if not isinstance(event.get("phase"), str) or not event["phase"]:
            raise ValueError("statement trace has an invalid phase")
        if not isinstance(event.get("event"), str) or not event["event"]:
            raise ValueError("statement trace has an invalid event")
        event_elapsed = event.get("elapsed_us")
        if (isinstance(event_elapsed, bool) or not isinstance(event_elapsed, int)
                or event_elapsed < 0):
            raise ValueError("statement trace elapsed time is not finite and non-negative")
        elapsed.append(event_elapsed)
        for field in ("duration_us", "value"):
            field_value = event.get(field)
            if (field_value is not None
                    and (isinstance(field_value, bool)
                         or not isinstance(field_value, int)
                         or field_value < 0)):
                raise ValueError(f"statement trace {field} is invalid")
        positions.setdefault(event["event"], []).append(index)
    if elapsed != sorted(elapsed):
        raise ValueError("statement trace elapsed time is not monotonic")
    terminal = [
        name for name in ("statement_complete", "statement_error", "statement_aborted")
        if name in positions
    ]
    if len(terminal) != 1:
        raise ValueError("statement trace has no unique terminal state")
    if require_complete and terminal != ["statement_complete"]:
        raise ValueError("cold statement trace did not complete successfully")
    if positions[terminal[0]][0] != len(events) - 1:
        raise ValueError("statement trace terminal state is not last")
    if require_complete:
        for required in (
            "parse_entry", "compiler_call_entry", "compiler_call_return",
            "statement_scope_begin", "statement_scope_return",
        ):
            if len(positions.get(required, [])) != 1:
                raise ValueError(f"statement trace requires exactly one {required}")
        if len(positions.get("plan_cache_miss", [])) != 1:
            raise ValueError("cold statement trace does not prove one cache miss")
        if any(positions.get(name) for name in ("plan_cache_hit", "instance_plan_cache_hit")):
            raise ValueError("cold statement trace contains a cache hit")
        order = {name: values[0] for name, values in positions.items() if len(values) == 1}
        if not (order["parse_entry"] < order["compiler_call_entry"]
                < order["compiler_call_return"]):
            raise ValueError("parse/compiler trace order is invalid")
        if not (order["statement_scope_begin"] < order["statement_scope_return"]
                < order[terminal[0]]):
            raise ValueError("statement lifecycle order is invalid")


def _validate_seed_files(root: Path) -> None:
    if not root.is_dir():
        raise ValueError(f"benchmark seed directory does not exist: {root}")
    for path in root.rglob("*"):
        # A retained symlink can direct a private server back into the seed or
        # another user's database. Special files are not a persistent snapshot.
        if path.is_symlink() or not (path.is_file() or path.is_dir()):
            raise ValueError(f"benchmark seed contains a link or special file: {path}")


@dataclass(frozen=True)
class DataSnapshot:
    path: Path
    seed_path: Path
    sha256: str

    def identity(self) -> dict[str, str]:
        return {"policy": "private_copy_per_process", "seed_path": str(self.seed_path),
                "seed_sha256": self.sha256, "initial_sha256": self.sha256}


@dataclass(frozen=True)
class ImmutableDataSeed:
    """A read-only input contract, not a directory a server may open in place."""

    path: Path
    sha256: str

    @classmethod
    def capture(cls, path: Path) -> "ImmutableDataSeed":
        path = path.resolve()
        _validate_seed_files(path)
        return cls(path, tree_digest(path))

    def verify_unchanged(self) -> None:
        _validate_seed_files(self.path)
        if tree_digest(self.path) != self.sha256:
            raise RuntimeError("immutable benchmark seed changed during measurements")

    @contextmanager
    def snapshot(self) -> Iterator[DataSnapshot]:
        with tempfile.TemporaryDirectory(prefix="paro-benchmark-snapshot-") as temporary:
            path = Path(temporary).resolve() / "data"
            command = ["cp", "-cR"] if sys.platform == "darwin" else ["cp", "-R", "--reflink=auto"]
            subprocess.run([*command, str(self.path), str(path)], check=True)
            _validate_seed_files(path)
            # Verify the copy, rather than trusting path/size or assuming a
            # concurrently changed input was copied as one coherent snapshot.
            if tree_digest(path) != self.sha256:
                raise RuntimeError("benchmark snapshot differs from its declared seed")
            try:
                yield DataSnapshot(path, self.path, self.sha256)
            finally:
                self.verify_unchanged()


@contextmanager
def isolated_paro_server(binary: Path, seed: ImmutableDataSeed, listen: str,
                         log_path: Path | None, *, max_memory: str,
                         threads: int,
                         statement_trace: bool = False,
                         trace_sample_id: str | None = None,
                         cache_evidence: bool = False,
                         optimizer_environment: Mapping[str, str | None] | None = None
                         ) -> Iterator["ManagedParoServer"]:
    """Every oracle and measurement process starts from the same verified input."""
    if log_path is not None and log_path.resolve().is_relative_to(seed.path):
        raise ValueError("benchmark logs must not write into the immutable seed")
    with seed.snapshot() as snapshot:
        with ManagedParoServer(binary, snapshot.path, listen, log_path,
                               max_memory=max_memory, threads=threads,
                               input_snapshot=snapshot.identity(),
                               statement_trace=statement_trace,
                               trace_sample_id=trace_sample_id,
                               cache_evidence=cache_evidence,
                               optimizer_environment=optimizer_environment) as server:
            yield server


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
        log_path: Path | None,
        *,
        max_memory: str,
        threads: int,
        input_snapshot: dict[str, str] | None = None,
        statement_trace: bool = False,
        trace_sample_id: str | None = None,
        cache_evidence: bool = False,
        optimizer_environment: Mapping[str, str | None] | None = None,
    ) -> None:
        self.binary = binary.resolve()
        self.data_dir = data_dir.resolve()
        self.listen = listen
        self.log_path = log_path.resolve() if log_path is not None else None
        self.max_memory = max_memory
        self.threads = max(1, threads)
        self.input_snapshot = input_snapshot
        self.statement_trace = statement_trace
        self.trace_sample_id = trace_sample_id
        self.cache_evidence = cache_evidence
        self.optimizer_environment = dict(optimizer_environment or {})
        self.process: subprocess.Popen[bytes] | None = None
        self._log = None
        self._started_ns: int | None = None
        self._started_monotonic_ns: int | None = None
        self._ready_ns: int | None = None
        self._ready_monotonic_ns: int | None = None

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
        if self.log_path is not None:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            self._log = self.log_path.open("wb")
        environment = os.environ.copy()
        if self.statement_trace:
            environment["PARO_STATEMENT_TRACE"] = "1"
            if self.trace_sample_id is not None:
                environment["PARO_STATEMENT_TRACE_SAMPLE"] = self.trace_sample_id
            else:
                environment.pop("PARO_STATEMENT_TRACE_SAMPLE", None)
        else:
            # A benchmark process may inherit the diagnostic setting from its
            # parent shell. False must be an effective child configuration,
            # not merely a report label.
            environment.pop("PARO_STATEMENT_TRACE", None)
            environment.pop("PARO_STATEMENT_TRACE_SAMPLE", None)
        if self.cache_evidence:
            environment["PARO_STATEMENT_CACHE_EVIDENCE"] = "1"
        else:
            environment.pop("PARO_STATEMENT_CACHE_EVIDENCE", None)
        for name, value in self.optimizer_environment.items():
            if value is None:
                environment.pop(name, None)
            else:
                environment[name] = value
        self._started_ns = time.time_ns()
        self._started_monotonic_ns = time.monotonic_ns()
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
                "info" if self.statement_trace else "warn",
            ],
            stdout=self._log if self._log is not None else subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
            cwd=self.binary.parent,
            env=environment,
        )
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(
                    f"Paro server exited with {self.process.returncode}"
                    + (f"; see {self.log_path}" if self.log_path is not None else "")
                )
            try:
                with socket.create_connection((host, port), timeout=0.25):
                    self._ready_ns = time.time_ns()
                    self._ready_monotonic_ns = time.monotonic_ns()
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
            "input_snapshot": self.input_snapshot,
            "listen": self.listen,
            "log": str(self.log_path) if self.log_path is not None else None,
            "statement_trace": self.statement_trace,
            "statement_trace_sample_id": self.trace_sample_id,
            "statement_cache_evidence": self.cache_evidence,
            "owned_by_harness": True,
            "launch_argv": list(self.process.args),
            "started_unix_ns": self._started_ns,
            "ready_unix_ns": self._ready_ns,
            "startup_to_ready_ms": (
                (self._ready_monotonic_ns - self._started_monotonic_ns) / 1_000_000
                if (self._started_monotonic_ns is not None
                    and self._ready_monotonic_ns is not None)
                else None
            ),
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
