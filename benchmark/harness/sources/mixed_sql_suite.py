# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Mixed-concurrency SQL suite measurement source."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from queue import SimpleQueue
import statistics
import threading
import time
from typing import Any

from ..executor import BenchmarkExecutor, RssSampler
from ..validator import BenchmarkValidator
from .context import SourceContext, SourceMeasurement


DEFAULT_CONCURRENCY = 4
DEFAULT_INTERNAL_ITERATIONS = 3


@dataclass(frozen=True)
class _Scenario:
    id: str
    samples_ms: list[float]
    validation: dict[str, Any]
    rss: dict[str, int | None] | None
    mixed: dict[str, Any]
    error: str | None = None


class MixedSqlSuiteSource:
    source_type = "mixed_sql_suite"

    def execute(self, source, context: SourceContext) -> SourceMeasurement:
        if not source.suite:
            raise ValueError(f"source '{source.name}' is missing suite")

        runner = context.runner_module
        args = runner.BenchmarkInvocation(suite=source.suite, pid=context.pid)
        config = runner.resolve_config(args)
        workloads = runner.load_selected_workloads(config, args, {})
        if len(workloads) != 1:
            raise ValueError(
                f"mixed_sql_suite '{source.name}' expects exactly one workload, got {len(workloads)}"
            )
        workload = workloads[0]
        query_by_id = {query.id: query for query in workload.queries}
        _require_queries(
            query_by_id,
            "small_point_lookup",
            "large_olap",
            "scan_filter",
            "hash_join",
            "temp_sort",
        )

        executor = BenchmarkExecutor(
            connection=config.connection,
            iterations=config.iterations,
            warmup=config.warmup,
            timeout_seconds=config.timeout_seconds,
            collect_memory=False,
            profile_pid=context.pid,
        )
        validator = BenchmarkValidator(
            executor.connection_factory,
            timeout_seconds=config.timeout_seconds,
        )

        conn = None
        teardown_conn = None
        scenarios: list[_Scenario] = []
        setup_error: str | None = None
        teardown_error: str | None = None
        try:
            conn = executor.connection_factory()
            executor.execute_script(conn, workload.setup_sql)
            for _ in range(config.warmup):
                _run_warmup(executor, query_by_id)
            scenarios = [
                _run_small_plus_large(
                    executor,
                    validator,
                    query_by_id,
                    iterations=config.iterations,
                    concurrency=_int_param(workload.params, "mixed_concurrency", DEFAULT_CONCURRENCY),
                    internal_iterations=_int_param(
                        workload.params,
                        "mixed_iterations",
                        DEFAULT_INTERNAL_ITERATIONS,
                    ),
                    pid=context.pid,
                ),
                _run_scan_join_mixed(
                    executor,
                    validator,
                    query_by_id,
                    iterations=config.iterations,
                    pid=context.pid,
                ),
                _run_temp_dir_contention(
                    executor,
                    validator,
                    query_by_id,
                    iterations=config.iterations,
                    concurrency=_int_param(workload.params, "mixed_concurrency", DEFAULT_CONCURRENCY),
                    pid=context.pid,
                ),
            ]
        except Exception as exc:
            setup_error = _format_error(exc)
        finally:
            try:
                teardown_conn = conn if conn is not None else executor.connection_factory()
                executor.execute_script(teardown_conn, workload.teardown_sql)
            except Exception as exc:
                teardown_error = _format_error(exc)
            finally:
                _safe_close(conn)
                if teardown_conn is not None and teardown_conn is not conn:
                    _safe_close(teardown_conn)

        payload = _payload(
            source=source,
            workload_name=workload.name,
            params=workload.params,
            scenarios=scenarios,
            setup_error=setup_error,
            teardown_error=teardown_error,
        )
        report_dir = context.root_dir / "report" / _safe_path_name(source.name)
        from ..reporter import BenchmarkReporter

        reporter = BenchmarkReporter(context.root_dir)
        result_path, summary_path = reporter.write_reports(payload, report_dir / "result.json")
        failed = bool(setup_error or teardown_error or any(s.error for s in scenarios))
        failed = failed or any(s.validation.get("result") != "PASS" for s in scenarios)
        return SourceMeasurement(
            source=source,
            payload=payload,
            result_path=result_path,
            summary_path=summary_path,
            failed=failed,
        )


def _run_warmup(executor: BenchmarkExecutor, query_by_id: dict[str, Any]) -> None:
    conn = executor.connection_factory()
    try:
        for query_id in ("small_point_lookup", "large_olap", "scan_filter", "hash_join", "temp_sort"):
            executor.execute_sql(conn, query_by_id[query_id].sql, fetch=True)
    finally:
        _safe_close(conn)


def _run_small_plus_large(
    executor: BenchmarkExecutor,
    validator: BenchmarkValidator,
    query_by_id: dict[str, Any],
    *,
    iterations: int,
    concurrency: int,
    internal_iterations: int,
    pid: int,
) -> _Scenario:
    small = query_by_id["small_point_lookup"]
    large = query_by_id["large_olap"]
    samples: list[float] = []
    small_ops = 0
    errors: SimpleQueue[str] = SimpleQueue()
    validation_detail: str | None = None
    rss_peak: int | None = None

    for _ in range(iterations):
        stop = threading.Event()
        lock = threading.Lock()
        run_ops = 0

        def small_worker() -> None:
            nonlocal run_ops
            conn = executor.connection_factory()
            local_ops = 0
            try:
                while True:
                    if stop.is_set() and local_ops >= internal_iterations:
                        break
                    rows = _rows(executor, conn, small.sql)
                    outcome = validator.validate_query(small, rows)
                    if outcome.status != "PASS":
                        errors.put(outcome.detail or "small query validation failed")
                        stop.set()
                        break
                    local_ops += 1
            except Exception as exc:
                errors.put(_format_error(exc))
                stop.set()
            finally:
                with lock:
                    run_ops += local_ops
                _safe_close(conn)

        workers = [threading.Thread(target=small_worker, daemon=True) for _ in range(max(concurrency, 1))]
        for worker in workers:
            worker.start()

        conn = executor.connection_factory()
        sampler = RssSampler(pid)
        sampler.start()
        start = time.perf_counter()
        try:
            rows = _rows(executor, conn, large.sql)
            outcome = validator.validate_query(large, rows)
            if outcome.status != "PASS":
                validation_detail = outcome.detail
                errors.put(outcome.detail or "large query validation failed")
        except Exception as exc:
            errors.put(_format_error(exc))
        finally:
            stop.set()
            for worker in workers:
                worker.join()
            elapsed = (time.perf_counter() - start) * 1000.0
            sampler.stop()
            _safe_close(conn)

        samples.append(elapsed)
        small_ops += run_ops
        if sampler.peak_kb is not None:
            rss_peak = max(rss_peak or 0, sampler.peak_kb)

    error_list = _drain_errors(errors)
    return _Scenario(
        id="small_plus_large_olap",
        samples_ms=samples,
        validation=_validation(error_list, validation_detail),
        rss=_rss_payload(rss_peak),
        mixed={
            "small_query": small.id,
            "large_query": large.id,
            "small_worker_count": max(concurrency, 1),
            "small_ops_total": small_ops,
            "internal_iterations": internal_iterations,
        },
        error="; ".join(error_list) if error_list else None,
    )


def _run_scan_join_mixed(
    executor: BenchmarkExecutor,
    validator: BenchmarkValidator,
    query_by_id: dict[str, Any],
    *,
    iterations: int,
    pid: int,
) -> _Scenario:
    queries = [query_by_id["scan_filter"], query_by_id["hash_join"]]
    samples: list[float] = []
    errors: SimpleQueue[str] = SimpleQueue()
    rss_peak: int | None = None

    for _ in range(iterations):
        sampler = RssSampler(pid)
        sampler.start()
        start = time.perf_counter()
        threads = [
            threading.Thread(
                target=_run_validated_query,
                args=(executor, validator, query, errors),
                daemon=True,
            )
            for query in queries
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        samples.append((time.perf_counter() - start) * 1000.0)
        sampler.stop()
        if sampler.peak_kb is not None:
            rss_peak = max(rss_peak or 0, sampler.peak_kb)

    error_list = _drain_errors(errors)
    return _Scenario(
        id="scan_join_mixed",
        samples_ms=samples,
        validation=_validation(error_list, None),
        rss=_rss_payload(rss_peak),
        mixed={"queries": [query.id for query in queries], "parallelism": len(queries)},
        error="; ".join(error_list) if error_list else None,
    )


def _run_temp_dir_contention(
    executor: BenchmarkExecutor,
    validator: BenchmarkValidator,
    query_by_id: dict[str, Any],
    *,
    iterations: int,
    concurrency: int,
    pid: int,
) -> _Scenario:
    query = query_by_id["temp_sort"]
    samples: list[float] = []
    errors: SimpleQueue[str] = SimpleQueue()
    rss_peak: int | None = None
    parallelism = max(concurrency, 1)

    for _ in range(iterations):
        sampler = RssSampler(pid)
        sampler.start()
        start = time.perf_counter()
        threads = [
            threading.Thread(
                target=_run_validated_query,
                args=(executor, validator, query, errors),
                daemon=True,
            )
            for _ in range(parallelism)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        samples.append((time.perf_counter() - start) * 1000.0)
        sampler.stop()
        if sampler.peak_kb is not None:
            rss_peak = max(rss_peak or 0, sampler.peak_kb)

    error_list = _drain_errors(errors)
    return _Scenario(
        id="temp_dir_contention",
        samples_ms=samples,
        validation=_validation(error_list, None),
        rss=_rss_payload(rss_peak),
        mixed={"query": query.id, "parallelism": parallelism},
        error="; ".join(error_list) if error_list else None,
    )


def _run_validated_query(
    executor: BenchmarkExecutor,
    validator: BenchmarkValidator,
    query: Any,
    errors: SimpleQueue[str],
) -> None:
    conn = executor.connection_factory()
    try:
        rows = _rows(executor, conn, query.sql)
        outcome = validator.validate_query(query, rows)
        if outcome.status != "PASS":
            errors.put(outcome.detail or f"{query.id} validation failed")
    except Exception as exc:
        errors.put(_format_error(exc))
    finally:
        _safe_close(conn)


def _rows(executor: BenchmarkExecutor, conn: Any, sql: str) -> list[list[Any]]:
    return [list(row) for row in executor.execute_sql(conn, sql, fetch=True)]


def _payload(
    *,
    source: Any,
    workload_name: str,
    params: dict[str, Any],
    scenarios: list[_Scenario],
    setup_error: str | None,
    teardown_error: str | None,
) -> dict[str, Any]:
    workload: dict[str, Any] = {
        "name": workload_name,
        "params": params,
        "queries": [],
    }
    for scenario in scenarios:
        query: dict[str, Any] = {
            "id": scenario.id,
            "samples_ms": scenario.samples_ms,
            "samples_count": len(scenario.samples_ms),
            "stats": _compute_stats(scenario.samples_ms),
            "rss": scenario.rss,
            "validation": scenario.validation,
            "mixed": scenario.mixed,
            "error": scenario.error,
        }
        workload["queries"].append(query)
    return {
        "version": 2,
        "timestamp": datetime.now().astimezone().isoformat(timespec="seconds"),
        "source": {
            "name": source.name,
            "type": source.type,
            "suite": source.suite,
            "measurement_class": source.measurement_class,
        },
        "setup_error": setup_error,
        "teardown_error": teardown_error,
        "workloads": [workload],
    }


def _compute_stats(samples: list[float]) -> dict[str, float]:
    if not samples:
        return {}
    ordered = sorted(samples)
    median = float(statistics.median(ordered))
    return {
        "min": float(ordered[0]),
        "median": median,
        "p50": median,
        "mean": float(statistics.mean(ordered)),
        "p90": _percentile(ordered, 0.9),
        "p99": _percentile(ordered, 0.99),
        "p999": _percentile(ordered, 0.999),
        "max": float(ordered[-1]),
        "throughput_per_second": 1000.0 / median if median > 0.0 else 0.0,
    }


def _percentile(sorted_samples: list[float], percentile: float) -> float:
    if not sorted_samples:
        return 0.0
    if len(sorted_samples) == 1:
        return float(sorted_samples[0])
    index = (len(sorted_samples) - 1) * percentile
    lower = int(index)
    upper = min(lower + 1, len(sorted_samples) - 1)
    weight = index - lower
    return float(sorted_samples[lower] * (1 - weight) + sorted_samples[upper] * weight)


def _validation(errors: list[str], detail: str | None) -> dict[str, Any]:
    if errors:
        return {"result": "FAIL", "detail": detail or errors[0]}
    return {"result": "PASS", "detail": None}


def _drain_errors(errors: SimpleQueue[str]) -> list[str]:
    drained = []
    while not errors.empty():
        drained.append(errors.get())
    return drained


def _rss_payload(peak_kb: int | None) -> dict[str, int | None] | None:
    if peak_kb is None:
        return None
    return {"before_kb": None, "after_kb": None, "peak_kb": peak_kb}


def _require_queries(query_by_id: dict[str, Any], *query_ids: str) -> None:
    missing = [query_id for query_id in query_ids if query_id not in query_by_id]
    if missing:
        raise ValueError(f"mixed SQL suite is missing required queries: {', '.join(missing)}")


def _int_param(params: dict[str, Any], key: str, default: int) -> int:
    value = params.get(key, default)
    if isinstance(value, bool):
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _safe_path_name(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in value)


def _format_error(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"


def _safe_close(conn: Any) -> None:
    if conn is None:
        return
    try:
        conn.close()
    except Exception:
        pass
