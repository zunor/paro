# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Paro benchmark runner."""

from __future__ import annotations

from dataclasses import dataclass
import argparse
import os
from pathlib import Path
import sys
import tomllib
from typing import Any, Mapping

from harness import (
    BenchmarkExecutor,
    BenchmarkReporter,
    BenchmarkValidator,
    WorkloadDef,
    load_named_workload,
    load_workloads,
    select_queries_exact,
)


TRUE_SET = {"1", "true", "yes", "on"}
FALSE_SET = {"0", "false", "no", "off"}


class RunnerError(RuntimeError):
    """Raised when runner configuration is invalid."""


@dataclass(frozen=True)
class RunnerConfig:
    root_dir: Path
    config_path: Path
    connection: dict[str, Any]
    iterations: int
    warmup: int
    timeout_seconds: int
    collect_memory: bool
    collect_explain_profile: bool
    regression_enabled: bool
    threshold_percent: float
    baseline_file: str
    output_path: Path

    @property
    def workloads_dir(self) -> Path:
        return self.root_dir / "workloads"

    @property
    def suites_dir(self) -> Path:
        return self.root_dir / "suites"


@dataclass(frozen=True)
class SuiteInclude:
    workload: str
    query_ids: tuple[str, ...] | None


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Paro benchmark runner")

    parser.add_argument("--config", type=Path, help="Path to benchmark config.toml")

    parser.add_argument("--host", help="Paro host")
    parser.add_argument("--port", type=int, help="Paro port")
    parser.add_argument("--database", help="Database name")
    parser.add_argument("--user", help="Database user")
    parser.add_argument("--password", help="Database password")

    parser.add_argument("--iterations", type=int, help="Timed iterations per query")
    parser.add_argument("--warmup", type=int, help="Warmup rounds per query")
    parser.add_argument("--timeout-seconds", type=int, help="Per-query timeout in seconds")
    parser.add_argument(
        "--collect-memory",
        dest="collect_memory",
        action="store_true",
        default=None,
        help="Collect paro_memory() snapshots",
    )
    parser.add_argument(
        "--no-collect-memory",
        dest="collect_memory",
        action="store_false",
        default=None,
        help="Disable memory snapshot collection",
    )
    parser.add_argument(
        "--collect-explain-profile",
        dest="collect_explain_profile",
        action="store_true",
        default=None,
        help="Collect EXPLAIN ANALYZE FORMAT JSON sidecars when queries allow re-execution",
    )
    parser.add_argument(
        "--no-collect-explain-profile",
        dest="collect_explain_profile",
        action="store_false",
        default=None,
        help="Disable EXPLAIN ANALYZE sidecar collection",
    )

    parser.add_argument("--workload", help="Run only one workload by name")
    parser.add_argument("--filter", help="Filter queries by substring")
    parser.add_argument("--suite", help="Run a checked-in suite from benchmark/suites/<name>.toml")
    parser.add_argument(
        "--param",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Override workload parameter",
    )

    parser.add_argument("--output", type=Path, help="JSON report path (default: benchmark/report/result.json)")
    parser.add_argument("--check", type=Path, help="Compare run against baseline JSON")
    parser.add_argument("--bless", type=Path, help="Write current result as baseline JSON")
    parser.add_argument("--pid", type=int, default=0, help="Reserved for future peak RSS support")
    return parser.parse_args(argv)


def resolve_config(args: argparse.Namespace, *, env: Mapping[str, str] | None = None) -> RunnerConfig:
    root_dir = Path(__file__).resolve().parent
    environment = os.environ if env is None else env

    config_path = args.config if args.config is not None else root_dir / "config.toml"
    config_data = _load_toml(config_path)

    connection_table = _get_table(config_data, "connection", required=False)
    defaults_table = _get_table(config_data, "defaults", required=False)
    regression_table = _get_table(config_data, "regression", required=False)

    connection = {
        "host": _as_str(connection_table.get("host", "localhost"), field="connection.host"),
        "port": _as_int(connection_table.get("port", 6432), field="connection.port"),
        "database": _as_str(connection_table.get("database", "postgres"), field="connection.database"),
        "user": _as_str(connection_table.get("user", "postgres"), field="connection.user"),
        "password": _as_str(connection_table.get("password", ""), field="connection.password"),
    }

    iterations = _as_int(defaults_table.get("iterations", 5), field="defaults.iterations")
    warmup = _as_int(defaults_table.get("warmup", 1), field="defaults.warmup")
    timeout_seconds = _as_int(
        defaults_table.get("timeout_seconds", 120),
        field="defaults.timeout_seconds",
    )
    collect_memory = _as_bool(
        defaults_table.get("collect_memory", True),
        field="defaults.collect_memory",
    )
    collect_explain_profile = _as_bool(
        defaults_table.get("collect_explain_profile", False),
        field="defaults.collect_explain_profile",
    )

    regression_enabled = _as_bool(
        regression_table.get("enabled", False),
        field="regression.enabled",
    )
    threshold_percent = _as_float(
        regression_table.get("threshold_percent", 15),
        field="regression.threshold_percent",
    )
    baseline_file = _as_str(
        regression_table.get("baseline_file", ""),
        field="regression.baseline_file",
    )

    if "PARO_HOST" in environment:
        connection["host"] = environment["PARO_HOST"]
    if "PARO_PORT" in environment:
        connection["port"] = _parse_env_int(environment["PARO_PORT"], "PARO_PORT")
    if "PARO_DATABASE" in environment:
        connection["database"] = environment["PARO_DATABASE"]
    if "PARO_USER" in environment:
        connection["user"] = environment["PARO_USER"]
    if "PARO_PASSWORD" in environment:
        connection["password"] = environment["PARO_PASSWORD"]

    if "BENCH_ITERATIONS" in environment:
        iterations = _parse_env_int(environment["BENCH_ITERATIONS"], "BENCH_ITERATIONS")
    if "BENCH_WARMUP" in environment:
        warmup = _parse_env_int(environment["BENCH_WARMUP"], "BENCH_WARMUP")
    if "BENCH_TIMEOUT_SECONDS" in environment:
        timeout_seconds = _parse_env_int(environment["BENCH_TIMEOUT_SECONDS"], "BENCH_TIMEOUT_SECONDS")
    if "BENCH_COLLECT_MEMORY" in environment:
        collect_memory = _parse_bool(environment["BENCH_COLLECT_MEMORY"], "BENCH_COLLECT_MEMORY")
    if "BENCH_COLLECT_EXPLAIN_PROFILE" in environment:
        collect_explain_profile = _parse_bool(
            environment["BENCH_COLLECT_EXPLAIN_PROFILE"],
            "BENCH_COLLECT_EXPLAIN_PROFILE",
        )
    if "BENCH_REGRESSION_ENABLED" in environment:
        regression_enabled = _parse_bool(
            environment["BENCH_REGRESSION_ENABLED"],
            "BENCH_REGRESSION_ENABLED",
        )
    if "BENCH_THRESHOLD_PERCENT" in environment:
        threshold_percent = _parse_env_float(environment["BENCH_THRESHOLD_PERCENT"], "BENCH_THRESHOLD_PERCENT")
    if "BENCH_BASELINE_FILE" in environment:
        baseline_file = environment["BENCH_BASELINE_FILE"]

    if args.host is not None:
        connection["host"] = args.host
    if args.port is not None:
        connection["port"] = args.port
    if args.database is not None:
        connection["database"] = args.database
    if args.user is not None:
        connection["user"] = args.user
    if args.password is not None:
        connection["password"] = args.password
    if args.iterations is not None:
        iterations = args.iterations
    if args.warmup is not None:
        warmup = args.warmup
    if args.timeout_seconds is not None:
        timeout_seconds = args.timeout_seconds
    if args.collect_memory is not None:
        collect_memory = args.collect_memory
    if args.collect_explain_profile is not None:
        collect_explain_profile = args.collect_explain_profile

    output_path = args.output if args.output is not None else root_dir / "report" / "result.json"
    if not output_path.is_absolute():
        output_path = (Path.cwd() / output_path).resolve()

    if iterations <= 0:
        raise RunnerError("--iterations must be > 0")
    if warmup < 0:
        raise RunnerError("--warmup must be >= 0")
    if timeout_seconds <= 0:
        raise RunnerError("--timeout-seconds must be > 0")

    return RunnerConfig(
        root_dir=root_dir,
        config_path=config_path,
        connection=connection,
        iterations=iterations,
        warmup=warmup,
        timeout_seconds=timeout_seconds,
        collect_memory=collect_memory,
        collect_explain_profile=collect_explain_profile,
        regression_enabled=regression_enabled,
        threshold_percent=threshold_percent,
        baseline_file=baseline_file,
        output_path=output_path,
    )


def parse_param_overrides(raw_params: list[str]) -> dict[str, Any]:
    overrides: dict[str, Any] = {}
    for raw in raw_params:
        if "=" not in raw:
            raise RunnerError(f"invalid --param '{raw}', expected KEY=VALUE")
        key, value = raw.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            raise RunnerError(f"invalid --param '{raw}', empty key")
        overrides[key] = _parse_param_value(value)
    return overrides


def load_selected_workloads(
    config: RunnerConfig,
    args: argparse.Namespace,
    param_overrides: Mapping[str, Any],
) -> list[WorkloadDef]:
    if args.suite is not None:
        if args.workload is not None or args.filter is not None:
            raise RunnerError("--suite cannot be combined with --workload or --filter")
        return _load_suite_workloads(
            config,
            suite_name=args.suite,
            param_overrides=param_overrides,
        )

    return load_workloads(
        config.workloads_dir,
        workload_name=args.workload,
        query_filter=args.filter,
        param_overrides=param_overrides,
        default_collect_explain_profile=config.collect_explain_profile,
    )


def _load_suite_workloads(
    config: RunnerConfig,
    *,
    suite_name: str,
    param_overrides: Mapping[str, Any],
) -> list[WorkloadDef]:
    suite_path = config.suites_dir / f"{suite_name}.toml"
    if not suite_path.exists():
        raise RunnerError(f"suite not found: {suite_name}")

    suite_data = _load_toml(suite_path)
    includes = _parse_suite_includes(suite_data, suite_path)
    return _materialize_suite_workloads(
        config,
        includes=includes,
        param_overrides=param_overrides,
    )


def _parse_suite_includes(data: Mapping[str, Any], suite_path: Path) -> list[SuiteInclude]:
    include_items = data.get("include")
    if not isinstance(include_items, list) or not include_items:
        raise RunnerError(f"{suite_path}: [[include]] is required")

    includes: list[SuiteInclude] = []
    for index, item in enumerate(include_items, start=1):
        if not isinstance(item, dict):
            raise RunnerError(f"{suite_path}: include #{index} must be a table")

        workload = item.get("workload")
        if not isinstance(workload, str) or not workload.strip():
            raise RunnerError(f"{suite_path}: include #{index} workload must be a non-empty string")

        raw_queries = item.get("queries")
        query_ids: tuple[str, ...] | None = None
        if raw_queries is not None:
            if not isinstance(raw_queries, list) or not all(isinstance(query_id, str) for query_id in raw_queries):
                raise RunnerError(
                    f"{suite_path}: include '{workload}' queries must be an array of strings"
                )
            normalized = tuple(query_id.strip() for query_id in raw_queries if query_id.strip())
            if not normalized:
                raise RunnerError(f"{suite_path}: include '{workload}' queries must not be empty")
            query_ids = normalized

        includes.append(SuiteInclude(workload=workload.strip(), query_ids=query_ids))

    return includes


def _materialize_suite_workloads(
    config: RunnerConfig,
    *,
    includes: list[SuiteInclude],
    param_overrides: Mapping[str, Any],
) -> list[WorkloadDef]:
    selections: dict[str, list[str] | None] = {}
    order: list[str] = []

    for include in includes:
        if include.workload not in selections:
            selections[include.workload] = [] if include.query_ids is not None else None
            order.append(include.workload)

        selected_queries = selections[include.workload]
        if selected_queries is None:
            continue
        if include.query_ids is None:
            selections[include.workload] = None
            continue
        for query_id in include.query_ids:
            if query_id not in selected_queries:
                selected_queries.append(query_id)

    workloads = []
    for workload_name in order:
        workload = load_named_workload(
            config.workloads_dir,
            workload_name,
            param_overrides=param_overrides,
            default_collect_explain_profile=config.collect_explain_profile,
        )
        selected_queries = selections[workload_name]
        if selected_queries is not None:
            workload = select_queries_exact(workload, selected_queries)
        workloads.append(workload)
    return workloads


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        config = resolve_config(args)
        param_overrides = parse_param_overrides(args.param)
    except RunnerError as exc:
        print(f"runner error: {exc}", file=sys.stderr)
        return 2

    try:
        workloads = load_selected_workloads(config, args, param_overrides)
    except Exception as exc:
        print(f"failed to load workloads: {exc}", file=sys.stderr)
        return 2

    if not workloads:
        print("no workloads selected", file=sys.stderr)
        return 2

    executor = BenchmarkExecutor(
        connection=config.connection,
        iterations=config.iterations,
        warmup=config.warmup,
        timeout_seconds=config.timeout_seconds,
        collect_memory=config.collect_memory,
    )
    validator = BenchmarkValidator(
        executor.connection_factory,
        timeout_seconds=config.timeout_seconds,
    )
    reporter = BenchmarkReporter(config.root_dir)

    workload_results = [executor.run_workload(workload, validator) for workload in workloads]
    payload = reporter.build_payload(
        workloads=workload_results,
        iterations=config.iterations,
        warmup=config.warmup,
        timeout_seconds=config.timeout_seconds,
        collect_memory=config.collect_memory,
        collect_explain_profile=config.collect_explain_profile,
    )
    result_path, summary_path = reporter.write_reports(payload, config.output_path)
    reporter.print_terminal_summary(workload_results, result_path)

    exit_code = 1 if reporter.has_failures(workload_results) else 0

    check_path = args.check
    if check_path is None and config.regression_enabled and config.baseline_file:
        check_path = Path(config.baseline_file)
    if check_path is not None:
        if not check_path.exists():
            print(f"baseline not found: {check_path}", file=sys.stderr)
            return 2
        entries = reporter.compare_with_baseline(payload, check_path, config.threshold_percent)
        reporter.print_regression(entries, config.threshold_percent)
        reporter.append_regression_to_summary(summary_path, entries, config.threshold_percent)
        if reporter.has_regression(entries):
            exit_code = 1

    if args.bless is not None:
        reporter.bless_baseline(result_path, args.bless)
        print(f"baseline updated: {args.bless}")

    return exit_code


def _load_toml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise RunnerError(f"config file not found: {path}")
    try:
        return tomllib.loads(path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as exc:
        raise RunnerError(f"invalid TOML in {path}: {exc}") from exc


def _get_table(data: Mapping[str, Any], key: str, *, required: bool) -> dict[str, Any]:
    raw = data.get(key)
    if raw is None:
        if required:
            raise RunnerError(f"missing [{key}] section")
        return {}
    if not isinstance(raw, dict):
        raise RunnerError(f"[{key}] must be a table")
    return dict(raw)


def _as_str(value: Any, *, field: str) -> str:
    if isinstance(value, str):
        return value
    raise RunnerError(f"{field} must be a string")


def _as_int(value: Any, *, field: str) -> int:
    if isinstance(value, bool):
        raise RunnerError(f"{field} must be an integer")
    if isinstance(value, int):
        return value
    raise RunnerError(f"{field} must be an integer")


def _as_float(value: Any, *, field: str) -> float:
    if isinstance(value, bool):
        raise RunnerError(f"{field} must be numeric")
    if isinstance(value, (int, float)):
        return float(value)
    raise RunnerError(f"{field} must be numeric")


def _as_bool(value: Any, *, field: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in TRUE_SET:
            return True
        if lowered in FALSE_SET:
            return False
    raise RunnerError(f"{field} must be boolean")


def _parse_bool(text: str, option_name: str) -> bool:
    lowered = text.strip().lower()
    if lowered in TRUE_SET:
        return True
    if lowered in FALSE_SET:
        return False
    raise RunnerError(f"{option_name} must be one of: {sorted(TRUE_SET | FALSE_SET)}")


def _parse_env_int(text: str, option_name: str) -> int:
    try:
        return int(text.strip())
    except ValueError as exc:
        raise RunnerError(f"{option_name} must be an integer") from exc


def _parse_env_float(text: str, option_name: str) -> float:
    try:
        return float(text.strip())
    except ValueError as exc:
        raise RunnerError(f"{option_name} must be numeric") from exc


def _parse_param_value(value: str) -> Any:
    if value == "":
        return ""
    try:
        parsed = tomllib.loads(f"v = {value}")
        return parsed["v"]
    except tomllib.TOMLDecodeError:
        lowered = value.lower()
        if lowered in TRUE_SET:
            return True
        if lowered in FALSE_SET:
            return False
        try:
            return int(value)
        except ValueError:
            pass
        try:
            return float(value)
        except ValueError:
            return value


def main() -> int:
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
