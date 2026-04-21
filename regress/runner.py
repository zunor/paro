# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Standalone SQL regression runner for Paro."""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
import argparse
import logging
import os
import re
import shlex
import signal
import subprocess
import shutil
import sys
import time
import traceback
import tomllib
from typing import Any, Iterable, Mapping, List

from harness.comparator import ResultMismatch, ResultParseError, build_transcript, compare_result_file
from harness.executor import ExecutionError, execute_blocks
from harness.parser import ParseError, parse_sql_file

_TRUE_SET = {"1", "true", "yes", "on"}
_FALSE_SET = {"0", "false", "no", "off"}

_STATUS_PASS = "PASS"
_STATUS_FAIL = "FAIL"
_STATUS_SKIP = "SKIP"
_STATUS_NEW = "NEW"
_FIXTURE_TOKEN_RE = re.compile(r"\{\{\s*fixture:([^}]+?)\s*\}\}")


def _is_color_enabled() -> bool:
    return sys.stdout.isatty() and os.getenv("NO_COLOR") is None


_COLOR_ENABLED = _is_color_enabled()
_RESET = "\033[0m"
_RED = "\033[31m"
_GREEN = "\033[32m"
_YELLOW = "\033[33m"
_BLUE = "\033[34m"
_CYAN = "\033[36m"


class RunnerError(RuntimeError):
    """Raised for runner-level errors (config, setup, connection)."""


@dataclass(frozen=True)
class ConnectionTarget:
    host: str
    port: int
    database: str
    user: str
    password: str


@dataclass(frozen=True)
class RuntimeProfile:
    name: str
    env: Mapping[str, str]
    unset: tuple[str, ...] = ()


@dataclass(frozen=True)
class RunnerConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    float_precision: int
    update: bool
    write_actual: bool
    jobs: int
    verbose: bool
    filter_pattern: str | None
    config_path: Path
    root_dir: Path
    report_dir: Path
    runtime_profiles: Mapping[str, RuntimeProfile]
    managed_runtime_env: tuple[str, ...]

    @property
    def cases_dir(self) -> Path:
        return self.root_dir / "cases"

    @property
    def log_path(self) -> Path:
        return self.report_dir / "regress.log"

    @property
    def report_txt_path(self) -> Path:
        return self.report_dir / "report.txt"

    @property
    def error_txt_path(self) -> Path:
        return self.report_dir / "error.txt"

    @property
    def actuals_dir(self) -> Path:
        return self.report_dir / "actuals"

    @property
    def fixtures_dir(self) -> Path:
        return self.root_dir / "fixtures"

    @property
    def staged_fixtures_dir(self) -> Path:
        return self.report_dir / "fixtures"

    @property
    def default_connection(self) -> ConnectionTarget:
        return ConnectionTarget(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
        )


@dataclass(frozen=True)
class CaseOutcome:
    path: Path
    status: str
    elapsed_seconds: float
    detail: str | None = None
    error_info: dict[str, Any] | None = None


@dataclass
class CaseSummary:
    total: int = 0
    success: int = 0
    failed: int = 0
    ignored: int = 0
    abnormal: int = 0

    @property
    def success_rate(self) -> int:
        if self.total == 0:
            return 100
        return int((self.success / self.total) * 100)


@dataclass
class Summary:
    passed: int
    failed: int
    skipped: int
    new: int
    elapsed_seconds: float
    case_summaries: dict[Path, CaseSummary]


class Reporter:
    def __init__(self, config: RunnerConfig):
        self.config = config
        self.outcomes: list[CaseOutcome] = []
        self.case_stats: dict[Path, CaseSummary] = {}
        
        # Initialize files
        self.config.report_txt_path.write_text("", encoding="utf-8")
        self.config.error_txt_path.write_text("", encoding="utf-8")

    def record(self, outcome: CaseOutcome, stats: CaseSummary):
        self.outcomes.append(outcome)
        self.case_stats[outcome.path] = stats
        
        # Append to report.txt (except summary line which comes last)
        rel_path = outcome.path.relative_to(self.config.root_dir).as_posix()
        line = (f"[{rel_path}] COST : {outcome.elapsed_seconds:.3f}s, "
                f"TOTAL :{stats.total}, SUCCESS :{stats.success}, FAILED :{stats.failed}, "
                f"IGNORED :{stats.ignored}, ABNORMAL :{stats.abnormal}, "
                f"SUCCESS RATE : {stats.success_rate}%\n")
        
        with self.config.report_txt_path.open("a", encoding="utf-8") as f:
            f.write(line)

        if outcome.status == _STATUS_FAIL:
            self._record_error(outcome)

    def _record_error(self, outcome: CaseOutcome):
        with self.config.error_txt_path.open("a", encoding="utf-8") as f:
            f.write("[ERROR]\n")
            f.write(f"[SCRIPT   FILE]: {outcome.path.relative_to(self.config.root_dir).as_posix()}\n")
            if outcome.error_info:
                info = outcome.error_info
                f.write(f"[ROW    NUMBER]: {info.get('line_no', 0)}\n")
                f.write(f"[SQL STATEMENT]: {info.get('sql', '')}\n")
                f.write(f"[EXPECT RESULT]:\n{info.get('expected', '')}\n")
                f.write(f"[ACTUAL RESULT]:\n{info.get('actual', '')}\n")
            else:
                f.write(f"{outcome.detail}\n")
            f.write("\n")

    def finalize(self, summary: Summary):
        # Prepend global summary to report.txt
        content = self.config.report_txt_path.read_text(encoding="utf-8")
        header = (f"[SUMMARY] COST : {summary.elapsed_seconds:.2f}s, "
                  f"TOTAL :{summary.passed + summary.failed + summary.skipped + summary.new}, "
                  f"SUCCESS :{summary.passed}, FAILED :{summary.failed}, "
                  f"IGNORED :{summary.skipped}, ABNORMAL :0, "
                  f"SUCCESS RATE : {int((summary.passed / (summary.passed + summary.failed + summary.new or 1)) * 100)}%\n")
        
        self.config.report_txt_path.write_text(header + content, encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Paro SQL regression runner")
    parser.add_argument("--host", help="Paro Server host")
    parser.add_argument("--port", type=int, help="Paro Server port")
    parser.add_argument("--database", help="Database name")
    parser.add_argument("--user", help="Database user")
    parser.add_argument("--password", help="Database password")
    parser.add_argument(
        "--update",
        action="store_true",
        default=None,
        help="Update .result files",
    )
    parser.add_argument(
        "--filter",
        dest="filter_pattern",
        help="Substring filter for case path",
    )
    parser.add_argument("--jobs", type=int, help="Worker count (M1 runs serially)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument(
        "--config",
        type=Path,
        help="Path to config.toml (default: regress/config.toml)",
    )
    parser.add_argument(
        "--no-write-actual",
        dest="write_actual",
        action="store_false",
        default=None,
        help="Disable writing *.result.actual on mismatch",
    )
    return parser.parse_args(argv)


def resolve_config(
    args: argparse.Namespace,
    *,
    env: Mapping[str, str] | None = None,
    root_dir: Path | None = None,
) -> RunnerConfig:
    root = root_dir or Path(__file__).resolve().parent
    environment = os.environ if env is None else env

    config_path = args.config if args.config is not None else root / "config.toml"
    config_data = _load_toml(config_path)
    config_dir = config_path.parent

    connection = _get_table(config_data, "connection")
    test = _get_table(config_data, "test")
    runtime_profiles, managed_runtime_env = _parse_runtime_profiles(
        config_data,
        config_dir=config_dir,
    )

    host = _as_str(connection.get("host", "127.0.0.1"), field="connection.host")
    port = _as_int(connection.get("port", 6432), field="connection.port")
    database = _as_str(connection.get("database", "postgres"), field="connection.database")
    user = _as_str(connection.get("user", "postgres"), field="connection.user")
    password = _as_str(connection.get("password", ""), field="connection.password")

    float_precision = _as_int(test.get("float_precision", 6), field="test.float_precision")
    update = _as_bool(test.get("update", False), field="test.update")
    write_actual = _as_bool(test.get("write_actual", True), field="test.write_actual")
    jobs = _as_int(test.get("jobs", 1), field="test.jobs")

    if "PARO_HOST" in environment:
        host = environment["PARO_HOST"]
    if "PARO_PORT" in environment:
        port = _parse_int_env(environment["PARO_PORT"], "PARO_PORT")
    if "PARO_DATABASE" in environment:
        database = environment["PARO_DATABASE"]
    if "PARO_USER" in environment:
        user = environment["PARO_USER"]
    if "PARO_PASSWORD" in environment:
        password = environment["PARO_PASSWORD"]
    if "PARO_UPDATE" in environment:
        update = _parse_bool(environment["PARO_UPDATE"], option_name="PARO_UPDATE")
    if "PARO_WRITE_ACTUAL" in environment:
        write_actual = _parse_bool(
            environment["PARO_WRITE_ACTUAL"],
            option_name="PARO_WRITE_ACTUAL",
        )

    if args.host is not None:
        host = args.host
    if args.port is not None:
        port = args.port
    if args.database is not None:
        database = args.database
    if args.user is not None:
        user = args.user
    if args.password is not None:
        password = args.password
    if args.update is not None:
        update = bool(args.update)
    if args.write_actual is not None:
        write_actual = bool(args.write_actual)
    if args.jobs is not None:
        jobs = args.jobs

    if port <= 0:
        raise RunnerError(f"invalid port: {port}")
    if float_precision < 0:
        raise RunnerError(f"invalid float_precision: {float_precision}")
    if jobs <= 0:
        raise RunnerError(f"invalid jobs: {jobs}")

    report_dir = root / "report"

    return RunnerConfig(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        float_precision=float_precision,
        update=update,
        write_actual=write_actual,
        jobs=jobs,
        verbose=bool(args.verbose),
        filter_pattern=args.filter_pattern,
        config_path=config_path,
        root_dir=root,
        report_dir=report_dir,
        runtime_profiles=runtime_profiles,
        managed_runtime_env=managed_runtime_env,
    )


def _parse_runtime_profiles(
    config_data: Mapping[str, Any],
    *,
    config_dir: Path,
) -> tuple[dict[str, RuntimeProfile], tuple[str, ...]]:
    runtime_profiles_data = _get_optional_table(config_data, "runtime_profiles")
    runtime_profiles: dict[str, RuntimeProfile] = {
        "default": RuntimeProfile(name="default", env={}, unset=())
    }
    managed_env: list[str] = []

    for profile_name, raw_profile in runtime_profiles_data.items():
        if not isinstance(raw_profile, dict):
            raise RunnerError(
                f"[runtime_profiles.{profile_name}] must be a table"
            )

        env_table = _get_optional_table(raw_profile, "env")
        env_map: dict[str, str] = {}
        for key, value in env_table.items():
            env_value = _as_str(
                value,
                field=f"runtime_profiles.{profile_name}.env.{key}",
            )
            env_map[key] = _resolve_runtime_profile_value(env_value, config_dir=config_dir)
            if key not in managed_env:
                managed_env.append(key)

        unset_values = raw_profile.get("unset", [])
        if not isinstance(unset_values, list) or any(
            not isinstance(value, str) for value in unset_values
        ):
            raise RunnerError(
                f"runtime_profiles.{profile_name}.unset must be an array of strings"
            )

        unset = tuple(unset_values)
        for key in unset:
            if key not in managed_env:
                managed_env.append(key)

        runtime_profiles[profile_name] = RuntimeProfile(
            name=profile_name,
            env=env_map,
            unset=unset,
        )

    return runtime_profiles, tuple(managed_env)


def _resolve_runtime_profile_value(value: str, *, config_dir: Path) -> str:
    candidate = Path(value)
    if candidate.is_absolute():
        return candidate.as_posix()
    resolved = (config_dir / candidate).resolve()
    if resolved.exists():
        return resolved.as_posix()
    return value


def discover_case_files(cases_dir: Path, filter_pattern: str | None = None) -> list[Path]:
    if not cases_dir.exists():
        raise RunnerError(f"cases directory does not exist: {cases_dir}")

    files = sorted(
        (path for path in cases_dir.rglob("*.sql") if path.is_file()),
        key=lambda path: path.as_posix(),
    )

    if filter_pattern:
        files = [path for path in files if filter_pattern in path.as_posix()]

    return files


def prepare_report_dir(report_dir: Path):
    if report_dir.exists():
        shutil.rmtree(report_dir)
    report_dir.mkdir(parents=True)
    (report_dir / "actuals").mkdir()
    (report_dir / "fixtures").mkdir()


def setup_logging(log_path: Path, verbose: bool):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(sys.stdout) if False else logging.NullHandler() # We handle stdout manually
        ]
    )
    # Redirect some manual prints to logger if needed, 
    # but for now we just use logging.info in the runner.


def run_single_case(conn: Any, case_path: Path, config: RunnerConfig) -> tuple[CaseOutcome, CaseSummary]:
    started = time.perf_counter()
    stats = CaseSummary()
    logging.info(f"Running case: {case_path}")

    try:
        blocks = parse_sql_file(case_path)
        blocks = _prepare_case_blocks(case_path, blocks, config)
        stats.total = len([b for b in blocks if b.kind in ("query", "statement")]) # Simplified count

        execution = execute_blocks(
            conn,
            blocks,
            engine="paro",
            float_precision=config.float_precision,
            control_handler=lambda active_conn, block: _handle_control_block(
                active_conn, block, config
            ),
        )

        if execution.skipped_due_to_setup_error:
            elapsed = time.perf_counter() - started
            stats.ignored = 1 # Mark entire file as ignored
            return CaseOutcome(
                path=case_path,
                status=_STATUS_SKIP,
                elapsed_seconds=elapsed,
                detail=execution.setup_error or "setup failed",
            ), stats

        result_path = case_path.with_suffix(".result")
        transcript = build_transcript(
            execution.query_outputs,
            precision=config.float_precision,
        )

        if config.update:
            result_path.write_text(transcript, encoding="utf-8")
            elapsed = time.perf_counter() - started
            stats.success = stats.total
            return CaseOutcome(path=case_path, status=_STATUS_PASS, elapsed_seconds=elapsed), stats

        if not result_path.exists():
            _maybe_write_actual(case_path, transcript, config)
            elapsed = time.perf_counter() - started
            stats.failed = 1 # or total? usually one missing result fails the whole file
            return CaseOutcome(
                path=case_path,
                status=_STATUS_NEW,
                elapsed_seconds=elapsed,
                detail="missing .result file (run with --update)",
            ), stats

        try:
            compare_result_file(
                expected_path=result_path,
                query_outputs=execution.query_outputs,
                precision=config.float_precision,
                write_actual=False, # We handle it ourselves to redirect
            )
            stats.success = stats.total
        except ResultMismatch as exc:
            _maybe_write_actual(case_path, transcript, config)
            raise exc

        elapsed = time.perf_counter() - started
        return CaseOutcome(path=case_path, status=_STATUS_PASS, elapsed_seconds=elapsed), stats

    except (ParseError, ExecutionError, ResultMismatch, ResultParseError) as exc:
        elapsed = time.perf_counter() - started
        stats.failed = 1 # Simplified: any error fails the file
        logging.error(f"Case failed: {case_path}\n{exc}")
        
        error_info = None
        if isinstance(exc, ResultMismatch):
            error_info = {
                "sql": exc.sql,
                "line_no": exc.line_no,
                "expected": exc.expected,
                "actual": exc.actual,
            }
            
        return CaseOutcome(
            path=case_path,
            status=_STATUS_FAIL,
            elapsed_seconds=elapsed,
            detail=str(exc),
            error_info=error_info
        ), stats


def _prepare_case_blocks(case_path: Path, blocks: List[Any], config: RunnerConfig) -> List[Any]:
    staged_fixtures = _stage_case_fixtures(case_path, blocks, config)
    if not staged_fixtures:
        return list(blocks)

    prepared_blocks = []
    for block in blocks:
        sql = _substitute_fixture_tokens(
            block.sql,
            staged_fixtures,
            case_path=case_path,
            line_no=block.line_no,
        )
        if sql == block.sql:
            prepared_blocks.append(block)
        else:
            prepared_blocks.append(replace(block, sql=sql))
    return prepared_blocks


def _stage_case_fixtures(
    case_path: Path,
    blocks: Iterable[Any],
    config: RunnerConfig,
) -> dict[str, Path]:
    fixture_refs = []
    for block in blocks:
        for fixture_ref in getattr(block, "fixture_refs", ()):
            if fixture_ref not in fixture_refs:
                fixture_refs.append(fixture_ref)

    if not fixture_refs:
        return {}

    case_stage_root = config.staged_fixtures_dir / case_path.relative_to(config.cases_dir).with_suffix("")
    staged_paths: dict[str, Path] = {}
    for fixture_ref in fixture_refs:
        source = config.fixtures_dir / fixture_ref
        if not source.exists():
            raise ExecutionError(
                f"fixture '{fixture_ref}' declared by {case_path.relative_to(config.root_dir).as_posix()} "
                f"does not exist under {config.fixtures_dir.as_posix()}"
            )

        destination = case_stage_root / fixture_ref
        destination.parent.mkdir(parents=True, exist_ok=True)
        if source.is_dir():
            shutil.copytree(source, destination, dirs_exist_ok=True)
        else:
            shutil.copy2(source, destination)
        staged_paths[fixture_ref] = destination
    return staged_paths


def _substitute_fixture_tokens(
    sql: str,
    staged_fixtures: Mapping[str, Path],
    *,
    case_path: Path,
    line_no: int,
) -> str:
    def replace_token(match: re.Match[str]) -> str:
        fixture_ref = match.group(1).strip()
        staged_path = staged_fixtures.get(fixture_ref)
        if staged_path is None:
            raise ExecutionError(
                f"fixture token '{fixture_ref}' at line {line_no} in "
                f"{case_path.as_posix()} was not staged"
            )
        return staged_path.as_posix()

    return _FIXTURE_TOKEN_RE.sub(replace_token, sql)


def run_cases(conn: Any, case_files: Iterable[Path], config: RunnerConfig, reporter: Reporter) -> list[CaseOutcome]:
    outcomes: list[CaseOutcome] = []
    for case_path in case_files:
        if config.verbose:
            rel_path = case_path.relative_to(config.root_dir).as_posix()
            prefix = _colorize("RUN ", _CYAN)
            print(f"  {prefix}  {rel_path}")
        
        outcome, stats = run_single_case(conn, case_path, config)
        outcomes.append(outcome)
        reporter.record(outcome, stats)
        _print_case_outcome(outcome, root_dir=config.root_dir)
    return outcomes


def summarize(outcomes: list[CaseOutcome], elapsed_seconds: float, reporter: Reporter) -> Summary:
    passed = sum(1 for outcome in outcomes if outcome.status == _STATUS_PASS)
    failed = sum(1 for outcome in outcomes if outcome.status == _STATUS_FAIL)
    skipped = sum(1 for outcome in outcomes if outcome.status == _STATUS_SKIP)
    new = sum(1 for outcome in outcomes if outcome.status == _STATUS_NEW)
    return Summary(
        passed=passed,
        failed=failed,
        skipped=skipped,
        new=new,
        elapsed_seconds=elapsed_seconds,
        case_summaries=reporter.case_stats
    )


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        config = resolve_config(args)

        # 1. Prepare report dir
        prepare_report_dir(config.report_dir)

        # 2. Setup logging
        setup_logging(config.log_path, config.verbose)
        logging.info("Regress started")

        if config.jobs != 1:
            _print_warning("--jobs > 1 is reserved for M2; running serially in current implementation")

        case_files = discover_case_files(config.cases_dir, filter_pattern=config.filter_pattern)
        print(f"Running {len(case_files)} test cases...")

        reporter = Reporter(config)
        started = time.perf_counter()
        
        outcomes = []
        for case_path in case_files:
            if config.verbose:
                rel_path = case_path.relative_to(config.root_dir).as_posix()
                prefix = _colorize("RUN ", _CYAN)
                print(f"  {prefix}  {rel_path}")
            
            conn = _open_connection(config)
            try:
                outcome, stats = run_single_case(conn, case_path, config)
                outcomes.append(outcome)
                reporter.record(outcome, stats)
                _print_case_outcome(outcome, root_dir=config.root_dir)
            finally:
                conn.close()

        elapsed = time.perf_counter() - started
        summary = summarize(outcomes, elapsed_seconds=elapsed, reporter=reporter)
        reporter.finalize(summary)
        
        _print_summary(summary)
        print(f"\nDetailed report: {config.report_txt_path}")
        print(f"Error details:   {config.error_txt_path}")
        print(f"Log file:        {config.log_path}")

        if summary.failed > 0 or summary.new > 0:
            return 1
        return 0

    except RunnerError as exc:
        _print_runner_error(str(exc))
        return 2
    except Exception:
        _print_runner_error("unexpected runner error")
        traceback.print_exc()
        return 2


def _open_connection(
    config: RunnerConfig,
    target: ConnectionTarget | None = None,
) -> Any:
    psycopg = _import_psycopg()
    connection = target or config.default_connection
    try:
        conn = psycopg.connect(
            host=connection.host,
            port=connection.port,
            dbname=connection.database,
            user=connection.user,
            password=connection.password,
        )
    except Exception as exc:  # pragma: no cover - depends on runtime DB environment.
        raise RunnerError(
            "failed to connect to Paro server "
            f"({connection.host}:{connection.port}/{connection.database} as {connection.user}): {exc}"
        ) from exc

    conn.autocommit = True
    return conn


def _handle_control_block(conn: Any, block: Any, config: RunnerConfig) -> Any:
    action = (block.control_action or "").lower()
    options = _parse_control_options(block)
    if action == "restart":
        return _restart_server(conn, config, options=options)
    if action == "connect":
        return _reconnect(conn, config, options=options)
    raise ExecutionError(
        f"unsupported control action at line {block.line_no}: {block.control_action}"
    )


def _restart_server(conn: Any, config: RunnerConfig, *, options: Mapping[str, str]) -> Any:
    if config.host not in {"localhost", "127.0.0.1"}:
        raise ExecutionError(
            f"restart control only supports local Paro servers, got host={config.host!r}"
        )

    profile_name = options.get("profile", "default")
    profile = config.runtime_profiles.get(profile_name)
    if profile is None:
        raise ExecutionError(f"unknown runtime profile: {profile_name}")

    connection = _connection_target_from_active(conn, config)
    listener_pid = _discover_listener_pid(config.port)
    command = _discover_process_command(listener_pid)

    try:
        conn.close()
    except Exception:
        pass

    os.kill(listener_pid, signal.SIGTERM)
    _wait_for_process_exit(listener_pid, timeout_seconds=10.0)
    _wait_for_port_state(config.port, listening=False, timeout_seconds=10.0)

    log_handle = config.log_path.open("a", encoding="utf-8")
    try:
        process = subprocess.Popen(
            shlex.split(command),
            cwd=config.root_dir.parent,
            env=_build_runtime_profile_env(config, profile),
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            text=True,
        )
    finally:
        log_handle.close()
    logging.info("Restarted Paro listener pid=%s with command=%s", process.pid, command)

    deadline = time.time() + 15.0
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            return _open_connection(config, connection)
        except Exception as exc:  # pragma: no cover - depends on runtime DB environment.
            last_error = exc
            time.sleep(0.2)

    raise ExecutionError(f"failed to reconnect after restart: {last_error}")


def _reconnect(conn: Any, config: RunnerConfig, *, options: Mapping[str, str]) -> Any:
    try:
        current = _connection_target_from_active(conn, config)
    finally:
        try:
            conn.close()
        except Exception:
            pass

    target = ConnectionTarget(
        host=options.get("host", current.host),
        port=_parse_control_port(options.get("port", str(current.port))),
        database=options.get("database", current.database),
        user=options.get("user", current.user),
        password=options.get("password", current.password),
    )
    return _open_connection(config, target)


def _parse_control_options(block: Any) -> dict[str, str]:
    options: dict[str, str] = {}
    for raw_arg in getattr(block, "control_args", ()):
        if "=" not in raw_arg:
            raise ExecutionError(
                f"control argument '{raw_arg}' at line {block.line_no} must use key=value syntax"
            )
        key, value = raw_arg.split("=", 1)
        key = key.strip().lower()
        if not key:
            raise ExecutionError(
                f"control argument '{raw_arg}' at line {block.line_no} is missing a key"
            )
        options[key] = value
    return options


def _connection_target_from_active(conn: Any, config: RunnerConfig) -> ConnectionTarget:
    info = getattr(conn, "info", None)
    host = _first_non_empty(getattr(info, "host", None), config.host)
    database = _first_non_empty(getattr(info, "dbname", None), config.database)
    user = _first_non_empty(getattr(info, "user", None), config.user)
    port_value = getattr(info, "port", None)
    try:
        port = int(port_value) if port_value not in (None, "") else config.port
    except (TypeError, ValueError):
        port = config.port
    return ConnectionTarget(
        host=host,
        port=port,
        database=database,
        user=user,
        password=config.password,
    )


def _build_runtime_profile_env(
    config: RunnerConfig,
    profile: RuntimeProfile,
) -> dict[str, str]:
    environment = dict(os.environ)
    for key in config.managed_runtime_env:
        environment.pop(key, None)
    for key in profile.unset:
        environment.pop(key, None)
    environment.update(profile.env)
    return environment


def _first_non_empty(value: Any, fallback: str) -> str:
    if value is None:
        return fallback
    text = str(value)
    return text if text else fallback


def _parse_control_port(raw_port: str) -> int:
    try:
        port = int(raw_port)
    except ValueError as exc:
        raise ExecutionError(f"invalid control port value: {raw_port!r}") from exc
    if port <= 0:
        raise ExecutionError(f"invalid control port value: {raw_port!r}")
    return port


def _discover_listener_pid(port: int) -> int:
    result = subprocess.run(
        ["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN", "-t"],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0 or not result.stdout.strip():
        raise ExecutionError(f"failed to find Paro listener pid on port {port}")
    try:
        return int(result.stdout.strip().splitlines()[0])
    except ValueError as exc:
        raise ExecutionError(f"invalid listener pid output for port {port}: {result.stdout!r}") from exc


def _discover_process_command(pid: int) -> str:
    result = subprocess.run(
        ["ps", "-o", "command=", "-p", str(pid)],
        check=False,
        capture_output=True,
        text=True,
    )
    command = result.stdout.strip()
    if result.returncode != 0 or not command:
        raise ExecutionError(f"failed to inspect Paro command line for pid {pid}")
    return command


def _wait_for_process_exit(pid: int, *, timeout_seconds: float) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        result = subprocess.run(
            ["ps", "-o", "stat=", "-p", str(pid)],
            check=False,
            capture_output=True,
            text=True,
        )
        status = result.stdout.strip()
        if result.returncode != 0 or not status or status.startswith("Z"):
            return
        time.sleep(0.1)
    raise ExecutionError(f"timed out waiting for pid {pid} to exit")


def _wait_for_port_state(port: int, *, listening: bool, timeout_seconds: float) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        result = subprocess.run(
            ["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN", "-t"],
            check=False,
            capture_output=True,
            text=True,
        )
        is_listening = bool(result.stdout.strip())
        if is_listening == listening:
            return
        time.sleep(0.1)
    target = "listening" if listening else "closed"
    raise ExecutionError(f"timed out waiting for port {port} to become {target}")


def _import_psycopg() -> Any:
    try:
        import psycopg  # type: ignore
    except ImportError as exc:  # pragma: no cover - import failure is environment dependent.
        raise RunnerError(
            "psycopg is required for SQL testing. Run 'make setup' in regress/ first."
        ) from exc

    return psycopg


def _maybe_write_actual(case_path: Path, transcript: str, config: RunnerConfig) -> None:
    if not config.write_actual:
        return
    # Flatten the case path for the actuals directory to avoid nested subdirectories
    rel_path_str = case_path.relative_to(config.cases_dir).as_posix().replace("/", "_")
    actual_path = config.actuals_dir / (rel_path_str + ".actual")
    actual_path.parent.mkdir(parents=True, exist_ok=True)
    actual_path.write_text(transcript, encoding="utf-8")
    logging.info(f"Wrote actual result to: {actual_path}")


def _print_case_outcome(outcome: CaseOutcome, *, root_dir: Path) -> None:
    rel_path = outcome.path.relative_to(root_dir).as_posix()
    status = _color_status(outcome.status)
    print(f"  {status:<4}  {rel_path:<58} ({outcome.elapsed_seconds:.2f}s)")

    if outcome.detail:
        for line in outcome.detail.splitlines():
            print(f"        {line}")


def _print_summary(summary: Summary) -> None:
    print("\n" + "-" * 50)
    print(
        "Results: "
        f"{summary.passed} passed, "
        f"{summary.failed} failed, "
        f"{summary.skipped} skipped, "
        f"{summary.new} new"
    )
    print(f"Time:    {summary.elapsed_seconds:.2f}s")


def _print_warning(message: str) -> None:
    prefix = _colorize("WARN", _YELLOW)
    print(f"{prefix}: {message}", file=sys.stderr)


def _print_runner_error(message: str) -> None:
    prefix = _colorize("RUNNER ERROR", _RED)
    print(f"{prefix}: {message}", file=sys.stderr)


def _color_status(status: str) -> str:
    if status == _STATUS_PASS:
        return _colorize(status, _GREEN)
    if status == _STATUS_FAIL:
        return _colorize(status, _RED)
    if status == _STATUS_SKIP:
        return _colorize(status, _YELLOW)
    if status == _STATUS_NEW:
        return _colorize(status, _BLUE)
    return status


def _colorize(text: str, color: str) -> str:
    if not _COLOR_ENABLED:
        return text
    return f"{color}{text}{_RESET}"


def _load_toml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise RunnerError(f"config file does not exist: {path}")

    try:
        with path.open("rb") as handle:
            data = tomllib.load(handle)
    except tomllib.TOMLDecodeError as exc:
        raise RunnerError(f"invalid TOML in {path}: {exc}") from exc
    except OSError as exc:
        raise RunnerError(f"failed to read config file {path}: {exc}") from exc

    if not isinstance(data, dict):
        raise RunnerError(f"invalid config structure in {path}: root must be a table")

    return data


def _get_table(data: Mapping[str, Any], key: str) -> dict[str, Any]:
    value = data.get(key, {})
    if not isinstance(value, dict):
        raise RunnerError(f"config section [{key}] must be a table")
    return value


def _get_optional_table(data: Mapping[str, Any], key: str) -> dict[str, Any]:
    value = data.get(key)
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise RunnerError(f"{key} must be a table")
    return value


def _as_str(value: Any, *, field: str) -> str:
    if isinstance(value, str):
        return value
    raise RunnerError(f"{field} must be a string")


def _as_int(value: Any, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise RunnerError(f"{field} must be an integer")
    return value


def _as_bool(value: Any, *, field: str) -> bool:
    if isinstance(value, bool):
        return value
    raise RunnerError(f"{field} must be a boolean")


def _parse_bool(text: str, *, option_name: str) -> bool:
    lowered = text.strip().lower()
    if lowered in _TRUE_SET:
        return True
    if lowered in _FALSE_SET:
        return False
    raise RunnerError(f"invalid boolean value for {option_name}: {text!r}")


def _parse_int_env(text: str, option_name: str) -> int:
    try:
        return int(text)
    except ValueError as exc:
        raise RunnerError(f"invalid integer value for {option_name}: {text!r}") from exc


if __name__ == "__main__":
    raise SystemExit(main())
