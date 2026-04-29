# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Parser for sqltest case files."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import List, Optional, Tuple

from .normalizers import is_known_normalizer, normalizer_profiles

_DIRECTIVE_RE = re.compile(r"^\s*--\s*@([a-zA-Z_]+)\s*(.*?)\s*$")
_DOLLAR_QUOTE_RE = re.compile(r"(\$[A-Za-z_][A-Za-z0-9_]*\$|\$\$)")
_APPROX_RE = re.compile(r"^approx\(\s*([^)]+)\s*\)$", re.IGNORECASE)

_QUERY_MODES = {"nosort", "rowsort", "valuesort", "hash", "file", "json"}
_COPY_DIRECTIONS = {"in", "out"}
_QUERY_START_RE = re.compile(
    r"^\s*(SELECT|WITH|VALUES|SHOW|DESCRIBE|EXPLAIN|CALL)", re.IGNORECASE
)
_SESSION_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.-]*$")
_ASYNC_LABEL_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.-]*$")
_DURATION_RE = re.compile(r"^(\d+(?:\.\d+)?)(ms|s)?$", re.IGNORECASE)
_SESSION_OPTION_KEYS = {"host", "port", "database", "user", "password"}


def _is_likely_query(sql: str) -> bool:
    """Check if a SQL statement looks like it should return results."""
    return _QUERY_START_RE.match(sql) is not None


class ParseError(ValueError):
    """Raised when SQL testcase parsing fails."""


@dataclass(frozen=True)
class Block:
    """Structured directive block."""

    kind: str
    line_no: int
    sql: str
    fixture_refs: Tuple[str, ...] = ()

    statement_expect: Optional[str] = None
    expected_count: Optional[int] = None
    error_pattern: Optional[str] = None

    query_mode: Optional[str] = None
    epsilon: Optional[float] = None
    copy_direction: Optional[str] = None
    copy_data_lines: Tuple[str, ...] = ()
    copy_fail_message: Optional[str] = None

    normalizers: Tuple[str, ...] = ()
    engine: Optional[str] = None
    control_action: Optional[str] = None
    control_args: Tuple[str, ...] = ()
    session_name: Optional[str] = None
    session_args: Tuple[str, ...] = ()
    async_label: Optional[str] = None
    await_label: Optional[str] = None
    await_timeout_ms: Optional[int] = None
    sleep_ms: Optional[int] = None
    wait_expect_interval_ms: Optional[int] = None
    wait_expect_timeout_ms: Optional[int] = None


@dataclass(frozen=True)
class _PendingSession:
    name: str
    args: Tuple[str, ...] = ()
    async_label: Optional[str] = None


@dataclass(frozen=True)
class _PendingWaitExpect:
    interval_ms: int
    timeout_ms: int


@dataclass
class _ScanState:
    in_single_quote: bool = False
    in_double_quote: bool = False
    in_block_comment: bool = False
    dollar_quote_tag: Optional[str] = None


def parse_sql_file(path: str | Path) -> List[Block]:
    """Parse one .sql test case file into structured blocks."""
    file_path = Path(path)
    content = file_path.read_text(encoding="utf-8")
    return parse_sql_text(content, source=str(file_path))


def parse_sql_text(text: str, source: str = "<memory>") -> List[Block]:
    """Parse .sql text into blocks."""
    lines = text.splitlines()
    blocks: List[Block] = []
    pending_normalizers: Tuple[str, ...] = ()
    pending_fixtures: Tuple[str, ...] = ()
    pending_session: Optional[_PendingSession] = None
    pending_wait_expect: Optional[_PendingWaitExpect] = None
    seen_async_labels: set[str] = set()

    idx = 0
    while idx < len(lines):
        line = lines[idx]
        stripped = line.strip()

        if not stripped:
            idx += 1
            continue

        directive = _parse_directive_line(line)
        if directive is None:
            if stripped.startswith("--"):
                idx += 1
                continue
            # Auto-detect block kind if directive is missing
            sql, next_idx = _consume_sql_statement(lines, idx, source, idx + 1)
            if _is_likely_query(sql):
                blocks.append(
                    Block(
                        kind="query",
                        line_no=idx + 1,
                        sql=sql,
                        fixture_refs=pending_fixtures,
                        query_mode="nosort",
                        normalizers=pending_normalizers,
                        session_name=_pending_session_name(pending_session),
                        session_args=_pending_session_args(pending_session),
                        async_label=_pending_async_label(pending_session),
                        wait_expect_interval_ms=_pending_wait_interval_ms(
                            pending_wait_expect
                        ),
                        wait_expect_timeout_ms=_pending_wait_timeout_ms(
                            pending_wait_expect
                        ),
                    )
                )
            else:
                if pending_wait_expect is not None:
                    raise ParseError(
                        f"{source}:{idx + 1}: '@wait_expect' can only target a query block."
                    )
                blocks.append(
                    Block(
                        kind="statement",
                        line_no=idx + 1,
                        sql=sql,
                        fixture_refs=pending_fixtures,
                        statement_expect="ok",
                        normalizers=pending_normalizers,
                        session_name=_pending_session_name(pending_session),
                        session_args=_pending_session_args(pending_session),
                        async_label=_pending_async_label(pending_session),
                    )
                )
            pending_normalizers = ()
            pending_fixtures = ()
            pending_session = None
            pending_wait_expect = None
            idx = next_idx
            continue

        name, arg = directive
        name = name.lower()
        line_no = idx + 1

        if name == "fixture":
            _ensure_no_pending_wait_expect(pending_wait_expect, source, line_no, name)
            pending_fixtures = _append_fixture_ref(
                pending_fixtures,
                _parse_fixture_arg(arg, source, line_no),
            )
            idx += 1
            continue

        if name == "normalize":
            _ensure_no_pending_wait_expect(pending_wait_expect, source, line_no, name)
            pending_normalizers = _parse_normalize_arg(arg, source, line_no)
            idx += 1
            continue

        if name == "session":
            if pending_session is not None:
                raise ParseError(
                    f"{source}:{line_no}: duplicate '@session' before SQL block."
                )
            pending_session = _parse_session_arg(arg, source, line_no)
            if pending_session.async_label is not None:
                if pending_session.async_label in seen_async_labels:
                    raise ParseError(
                        f"{source}:{line_no}: duplicate async label "
                        f"{pending_session.async_label!r}."
                    )
                seen_async_labels.add(pending_session.async_label)
            idx += 1
            continue

        if name == "await":
            if pending_session is not None:
                raise ParseError(f"{source}:{line_no}: '@session' cannot target '@await'.")
            _ensure_no_pending_wait_expect(pending_wait_expect, source, line_no, name)
            label, timeout_ms = _parse_await_arg(arg, source, line_no)
            blocks.append(
                Block(
                    kind="await",
                    line_no=line_no,
                    sql="",
                    fixture_refs=pending_fixtures,
                    await_label=label,
                    await_timeout_ms=timeout_ms,
                )
            )
            pending_fixtures = ()
            idx += 1
            continue

        if name == "sleep":
            if pending_session is not None:
                raise ParseError(f"{source}:{line_no}: '@session' cannot target '@sleep'.")
            _ensure_no_pending_wait_expect(pending_wait_expect, source, line_no, name)
            blocks.append(
                Block(
                    kind="sleep",
                    line_no=line_no,
                    sql="",
                    fixture_refs=pending_fixtures,
                    sleep_ms=_parse_duration_ms(arg, source, line_no, directive="sleep"),
                )
            )
            pending_fixtures = ()
            idx += 1
            continue

        if name == "wait_expect":
            if pending_wait_expect is not None:
                raise ParseError(
                    f"{source}:{line_no}: duplicate '@wait_expect' before query block."
                )
            pending_wait_expect = _parse_wait_expect_arg(arg, source, line_no)
            idx += 1
            continue

        if name == "control":
            if pending_session is not None:
                raise ParseError(
                    f"{source}:{line_no}: '@session' cannot target '@control'."
                )
            _ensure_no_pending_wait_expect(pending_wait_expect, source, line_no, name)
            action, control_args = _parse_control_arg(arg, source, line_no)
            blocks.append(
                Block(
                    kind="control",
                    line_no=line_no,
                    sql="",
                    fixture_refs=pending_fixtures,
                    control_action=action,
                    control_args=control_args,
                )
            )
            pending_fixtures = ()
            idx += 1
            continue

        if name in {"setup", "teardown"}:
            if pending_session is not None:
                raise ParseError(
                    f"{source}:{line_no}: '@{name}' always runs on the default session."
                )
            _ensure_no_pending_wait_expect(pending_wait_expect, source, line_no, name)
            sql, next_idx = _consume_sql_statement(lines, idx + 1, source, line_no)
            blocks.append(
                Block(
                    kind=name,
                    line_no=line_no,
                    sql=sql,
                    fixture_refs=pending_fixtures,
                )
            )
            pending_fixtures = ()
            idx = next_idx
            continue

        if name == "statement":
            if pending_wait_expect is not None:
                raise ParseError(
                    f"{source}:{line_no}: '@wait_expect' can only target a query block."
                )
            expect, expected_count, error_pattern = _parse_statement_arg(arg, source, line_no)
            normalizers, statement_start_idx = _consume_trailing_normalize_directives(
                lines,
                idx + 1,
                source,
                line_no,
                pending_normalizers,
            )
            sql, next_idx = _consume_sql_statement(lines, statement_start_idx, source, line_no)
            blocks.append(
                Block(
                    kind="statement",
                    line_no=line_no,
                    sql=sql,
                    fixture_refs=pending_fixtures,
                    statement_expect=expect,
                    expected_count=expected_count,
                    error_pattern=error_pattern,
                    normalizers=normalizers,
                    session_name=_pending_session_name(pending_session),
                    session_args=_pending_session_args(pending_session),
                    async_label=_pending_async_label(pending_session),
                )
            )
            pending_normalizers = ()
            pending_fixtures = ()
            pending_session = None
            idx = next_idx
            continue

        if name == "query":
            mode, epsilon = _parse_query_arg(arg, source, line_no)
            normalizers, query_start_idx = _consume_trailing_normalize_directives(
                lines,
                idx + 1,
                source,
                line_no,
                pending_normalizers,
            )
            sql, next_idx = _consume_sql_statement(lines, query_start_idx, source, line_no)
            blocks.append(
                Block(
                    kind="query",
                    line_no=line_no,
                    sql=sql,
                    fixture_refs=pending_fixtures,
                    query_mode=mode,
                    epsilon=epsilon,
                    normalizers=normalizers,
                    session_name=_pending_session_name(pending_session),
                    session_args=_pending_session_args(pending_session),
                    async_label=_pending_async_label(pending_session),
                    wait_expect_interval_ms=_pending_wait_interval_ms(
                        pending_wait_expect
                    ),
                    wait_expect_timeout_ms=_pending_wait_timeout_ms(
                        pending_wait_expect
                    ),
                )
            )
            pending_normalizers = ()
            pending_fixtures = ()
            pending_session = None
            pending_wait_expect = None
            idx = next_idx
            continue

        if name == "copy":
            if pending_wait_expect is not None:
                raise ParseError(
                    f"{source}:{line_no}: '@wait_expect' can only target a query block."
                )
            direction = _parse_copy_arg(arg, source, line_no)
            normalizers, copy_start_idx = _consume_trailing_normalize_directives(
                lines,
                idx + 1,
                source,
                line_no,
                pending_normalizers,
            )
            sql, next_idx = _consume_sql_statement(lines, copy_start_idx, source, line_no)
            copy_data_lines: Tuple[str, ...] = ()
            copy_fail_message: Optional[str] = None
            if direction == "in":
                copy_data_lines, copy_fail_message, next_idx = _consume_copy_input_block(
                    lines,
                    next_idx,
                    source,
                    line_no,
                )
            blocks.append(
                Block(
                    kind="copy",
                    line_no=line_no,
                    sql=sql,
                    fixture_refs=pending_fixtures,
                    copy_direction=direction,
                    copy_data_lines=copy_data_lines,
                    copy_fail_message=copy_fail_message,
                    normalizers=normalizers,
                    session_name=_pending_session_name(pending_session),
                    session_args=_pending_session_args(pending_session),
                    async_label=_pending_async_label(pending_session),
                )
            )
            pending_normalizers = ()
            pending_fixtures = ()
            pending_session = None
            idx = next_idx
            continue

        if name in {"skipif", "onlyif"}:
            if pending_session is not None:
                raise ParseError(
                    f"{source}:{line_no}: '@session' cannot target '@{name}'."
                )
            _ensure_no_pending_wait_expect(pending_wait_expect, source, line_no, name)
            engine = arg.strip()
            if not engine:
                raise ParseError(f"{source}:{line_no}: '@{name}' requires an engine argument.")

            next_content = _next_content_line(lines, idx + 1)
            if next_content is None or _parse_directive_line(lines[next_content]) is not None:
                blocks.append(
                    Block(
                        kind=name,
                        line_no=line_no,
                        sql="",
                        fixture_refs=pending_fixtures,
                        engine=engine,
                    )
                )
                pending_fixtures = ()
                idx += 1
                continue

            sql, next_idx = _consume_sql_statement(lines, idx + 1, source, line_no)
            blocks.append(
                Block(
                    kind=name,
                    line_no=line_no,
                    sql=sql,
                    fixture_refs=pending_fixtures,
                    engine=engine,
                )
            )
            pending_fixtures = ()
            idx = next_idx
            continue

        raise ParseError(f"{source}:{line_no}: unsupported directive '@{name}'.")

    if pending_session is not None:
        raise ParseError(f"{source}: dangling '@session' directive without SQL block.")
    if pending_wait_expect is not None:
        raise ParseError(f"{source}: dangling '@wait_expect' directive without query block.")

    return blocks


def _parse_statement_arg(arg: str, source: str, line_no: int) -> Tuple[str, Optional[int], Optional[str]]:
    payload = arg.strip()
    if payload == "ok":
        return "ok", None, None

    if payload.startswith("count"):
        count_text = payload[len("count") :].strip()
        if not count_text:
            raise ParseError(f"{source}:{line_no}: '@statement count' requires a row count.")
        try:
            return "count", int(count_text), None
        except ValueError as exc:
            raise ParseError(
                f"{source}:{line_no}: invalid row count in '@statement count {count_text}'."
            ) from exc

    parts = payload.split(maxsplit=1)
    if parts and parts[0] == "error":
        pattern = parts[1].strip() if len(parts) > 1 else None
        return "error", None, pattern

    raise ParseError(
        f"{source}:{line_no}: unsupported '@statement' mode '{payload}'."
    )


def _parse_query_arg(arg: str, source: str, line_no: int) -> Tuple[str, Optional[float]]:
    payload = arg.strip().lower()
    if not payload:
        return "nosort", None

    if payload in _QUERY_MODES:
        return payload, None

    match = _APPROX_RE.match(payload)
    if match is not None:
        epsilon_text = match.group(1)
        try:
            epsilon = float(epsilon_text)
        except ValueError as exc:
            raise ParseError(
                f"{source}:{line_no}: invalid epsilon in '@query approx({epsilon_text})'."
            ) from exc
        return "approx", epsilon

    raise ParseError(f"{source}:{line_no}: unsupported '@query' mode '{arg.strip()}'.")


def _parse_copy_arg(arg: str, source: str, line_no: int) -> str:
    direction = arg.strip().lower()
    if direction in _COPY_DIRECTIONS:
        return direction
    raise ParseError(
        f"{source}:{line_no}: unsupported '@copy' direction '{arg.strip()}'."
    )


def _parse_control_arg(arg: str, source: str, line_no: int) -> Tuple[str, Tuple[str, ...]]:
    parts = arg.strip().split()
    if not parts:
        raise ParseError(f"{source}:{line_no}: '@control' requires an action.")
    return parts[0].lower(), tuple(parts[1:])


def _parse_await_arg(arg: str, source: str, line_no: int) -> tuple[str, int]:
    parts = arg.strip().split()
    if not parts:
        raise ParseError(f"{source}:{line_no}: '@await' requires an async label.")
    label = parts[0]
    if not _ASYNC_LABEL_RE.match(label):
        raise ParseError(
            f"{source}:{line_no}: invalid '@await' label {label!r}; "
            "expected an identifier-like label."
        )

    timeout_ms: int | None = None
    seen_keys: set[str] = set()
    for raw_arg in parts[1:]:
        if "=" not in raw_arg:
            raise ParseError(
                f"{source}:{line_no}: '@await' argument {raw_arg!r} must use key=value syntax."
            )
        key, value = raw_arg.split("=", 1)
        key = key.strip().lower()
        if key != "timeout":
            raise ParseError(
                f"{source}:{line_no}: unsupported '@await' option {key!r}; "
                "known options: timeout."
            )
        if key in seen_keys:
            raise ParseError(f"{source}:{line_no}: duplicate '@await' option {key!r}.")
        seen_keys.add(key)
        timeout_ms = _parse_duration_ms(value, source, line_no, directive="await timeout")

    if timeout_ms is None:
        raise ParseError(f"{source}:{line_no}: '@await' requires timeout=<duration>.")
    return label, timeout_ms


def _parse_wait_expect_arg(arg: str, source: str, line_no: int) -> _PendingWaitExpect:
    interval_ms: int | None = None
    timeout_ms: int | None = None
    seen_keys: set[str] = set()

    for raw_arg in arg.strip().split():
        if "=" not in raw_arg:
            raise ParseError(
                f"{source}:{line_no}: '@wait_expect' argument {raw_arg!r} "
                "must use key=value syntax."
            )
        key, value = raw_arg.split("=", 1)
        key = key.strip().lower()
        if key not in {"interval", "timeout"}:
            raise ParseError(
                f"{source}:{line_no}: unsupported '@wait_expect' option {key!r}; "
                "known options: interval, timeout."
            )
        if key in seen_keys:
            raise ParseError(
                f"{source}:{line_no}: duplicate '@wait_expect' option {key!r}."
            )
        seen_keys.add(key)
        duration = _parse_duration_ms(value, source, line_no, directive=f"wait_expect {key}")
        if key == "interval":
            interval_ms = duration
        else:
            timeout_ms = duration

    if interval_ms is None or timeout_ms is None:
        raise ParseError(
            f"{source}:{line_no}: '@wait_expect' requires interval=<duration> "
            "and timeout=<duration>."
        )
    return _PendingWaitExpect(interval_ms=interval_ms, timeout_ms=timeout_ms)


def _parse_duration_ms(arg: str, source: str, line_no: int, *, directive: str) -> int:
    payload = arg.strip().lower()
    match = _DURATION_RE.match(payload)
    if match is None:
        raise ParseError(
            f"{source}:{line_no}: '@{directive}' duration must be like 100ms or 5s."
        )
    amount = float(match.group(1))
    unit = match.group(2) or "ms"
    multiplier = 1000.0 if unit == "s" else 1.0
    duration_ms = int(amount * multiplier)
    if duration_ms <= 0:
        raise ParseError(f"{source}:{line_no}: '@{directive}' duration must be positive.")
    return duration_ms


def _ensure_no_pending_wait_expect(
    pending_wait_expect: Optional[_PendingWaitExpect],
    source: str,
    line_no: int,
    directive: str,
) -> None:
    if pending_wait_expect is not None:
        raise ParseError(
            f"{source}:{line_no}: '@wait_expect' cannot target '@{directive}'."
        )


def _parse_session_arg(arg: str, source: str, line_no: int) -> _PendingSession:
    parts = arg.strip().split()
    if not parts:
        raise ParseError(f"{source}:{line_no}: '@session' requires a session name.")

    name = parts[0]
    if not _SESSION_NAME_RE.match(name):
        raise ParseError(
            f"{source}:{line_no}: invalid '@session' name {name!r}; "
            "expected an identifier-like label."
        )

    args: list[str] = []
    seen_keys: set[str] = set()
    async_label: str | None = None
    for raw_arg in parts[1:]:
        if "=" not in raw_arg:
            raise ParseError(
                f"{source}:{line_no}: session argument {raw_arg!r} must use key=value syntax."
            )
        key, value = raw_arg.split("=", 1)
        key = key.strip().lower()
        if key == "async":
            if async_label is not None:
                raise ParseError(f"{source}:{line_no}: duplicate '@session' option 'async'.")
            if value == "" or not _ASYNC_LABEL_RE.match(value):
                raise ParseError(
                    f"{source}:{line_no}: invalid async label {value!r}; "
                    "expected an identifier-like label."
                )
            async_label = value
            continue
        if key not in _SESSION_OPTION_KEYS:
            allowed = ", ".join(sorted(_SESSION_OPTION_KEYS | {"async"}))
            raise ParseError(
                f"{source}:{line_no}: unsupported '@session' option {key!r}; "
                f"known options: {allowed}."
            )
        if key in seen_keys:
            raise ParseError(f"{source}:{line_no}: duplicate '@session' option {key!r}.")
        if value == "":
            raise ParseError(f"{source}:{line_no}: '@session' option {key!r} is empty.")
        seen_keys.add(key)
        args.append(f"{key}={value}")

    return _PendingSession(name=name, args=tuple(args), async_label=async_label)


def _pending_session_name(session: Optional[_PendingSession]) -> Optional[str]:
    return None if session is None else session.name


def _pending_session_args(session: Optional[_PendingSession]) -> Tuple[str, ...]:
    return () if session is None else session.args


def _pending_async_label(session: Optional[_PendingSession]) -> Optional[str]:
    return None if session is None else session.async_label


def _pending_wait_interval_ms(wait: Optional[_PendingWaitExpect]) -> Optional[int]:
    return None if wait is None else wait.interval_ms


def _pending_wait_timeout_ms(wait: Optional[_PendingWaitExpect]) -> Optional[int]:
    return None if wait is None else wait.timeout_ms


def _parse_normalize_arg(arg: str, source: str, line_no: int) -> Tuple[str, ...]:
    payload = arg.strip()
    if not payload:
        raise ParseError(f"{source}:{line_no}: '@normalize' requires at least one profile name.")

    profiles = [part.strip().lower() for part in payload.split(",")]
    if any(not profile for profile in profiles):
        raise ParseError(
            f"{source}:{line_no}: invalid '@normalize' list; expected comma-separated profile names."
        )
    for profile in profiles:
        if not is_known_normalizer(profile):
            allowed = ", ".join(normalizer_profiles())
            raise ParseError(
                f"{source}:{line_no}: unknown '@normalize' profile {profile!r}; "
                f"known profiles: {allowed}."
            )
    return tuple(profiles)


def _parse_fixture_arg(arg: str, source: str, line_no: int) -> str:
    fixture = arg.strip()
    if not fixture:
        raise ParseError(f"{source}:{line_no}: '@fixture' requires a fixture path.")
    fixture_path = Path(fixture)
    if fixture_path.is_absolute() or any(part == ".." for part in fixture_path.parts):
        raise ParseError(
            f"{source}:{line_no}: fixture path must stay within regress/fixtures, got {fixture!r}."
        )
    return fixture_path.as_posix()


def _append_fixture_ref(existing: Tuple[str, ...], fixture: str) -> Tuple[str, ...]:
    if fixture in existing:
        return existing
    return existing + (fixture,)


def _consume_trailing_normalize_directives(
    lines: List[str],
    start_idx: int,
    source: str,
    directive_line_no: int,
    base_normalizers: Tuple[str, ...],
) -> Tuple[Tuple[str, ...], int]:
    normalizers = base_normalizers
    idx = start_idx

    while True:
        next_content = _next_content_line(lines, idx)
        if next_content is None:
            raise ParseError(
                f"{source}:{directive_line_no}: directive is missing SQL statement body."
            )

        nested_directive = _parse_directive_line(lines[next_content])
        if nested_directive is None:
            return normalizers, idx

        nested_name, nested_arg = nested_directive
        if nested_name.lower() != "normalize":
            raise ParseError(
                f"{source}:{directive_line_no}: directive is missing SQL before next directive."
            )

        normalizers = _parse_normalize_arg(nested_arg, source, next_content + 1)
        idx = next_content + 1


def _consume_sql_statement(
    lines: List[str],
    start_idx: int,
    source: str,
    directive_line_no: int,
) -> Tuple[str, int]:
    idx = _next_content_line(lines, start_idx)
    if idx is None:
        raise ParseError(
            f"{source}:{directive_line_no}: directive is missing SQL statement body."
        )

    if _parse_directive_line(lines[idx]) is not None:
        raise ParseError(
            f"{source}:{directive_line_no}: directive is missing SQL before next directive."
        )

    state = _ScanState()
    collected: List[str] = []

    while idx < len(lines):
        line = lines[idx]
        collected.append(line)

        if _scan_line_for_terminator(line, state):
            sql = "\n".join(collected).strip()
            return sql, idx + 1

        idx += 1

    raise ParseError(
        f"{source}:{directive_line_no}: SQL statement must end with ';'."
    )


def _consume_copy_input_block(
    lines: List[str],
    start_idx: int,
    source: str,
    directive_line_no: int,
) -> Tuple[Tuple[str, ...], Optional[str], int]:
    idx = _next_content_line(lines, start_idx)
    if idx is None:
        raise ParseError(
            f"{source}:{directive_line_no}: COPY FROM STDIN block requires '-- @copydata' and '-- @endcopy'."
        )

    directive = _parse_directive_line(lines[idx])
    if directive is None or directive[0].lower() != "copydata":
        raise ParseError(
            f"{source}:{directive_line_no}: COPY FROM STDIN block requires '-- @copydata' after SQL."
        )

    idx += 1
    data_lines: List[str] = []
    copy_fail_message: Optional[str] = None

    while idx < len(lines):
        directive = _parse_directive_line(lines[idx])
        if directive is not None:
            name, arg = directive
            lowered = name.lower()
            if lowered == "endcopy":
                return tuple(data_lines), copy_fail_message, idx + 1
            if lowered == "copyfail":
                if copy_fail_message is not None:
                    raise ParseError(
                        f"{source}:{idx + 1}: duplicate '@copyfail' in COPY FROM STDIN block."
                    )
                copy_fail_message = arg.strip() or None
                idx += 1
                continue
            if lowered == "copydata":
                raise ParseError(
                    f"{source}:{idx + 1}: duplicate '@copydata' in COPY FROM STDIN block."
                )

        data_lines.append(lines[idx])
        idx += 1

    raise ParseError(
        f"{source}:{directive_line_no}: COPY FROM STDIN block is missing '-- @endcopy'."
    )


def _scan_line_for_terminator(line: str, state: _ScanState) -> bool:
    i = 0
    length = len(line)

    while i < length:
        if state.in_block_comment:
            end = line.find("*/", i)
            if end == -1:
                return False
            state.in_block_comment = False
            i = end + 2
            continue

        if state.dollar_quote_tag is not None:
            tag = state.dollar_quote_tag
            assert tag is not None
            if line.startswith(tag, i):
                state.dollar_quote_tag = None
                i += len(tag)
            else:
                i += 1
            continue

        ch = line[i]
        pair = line[i : i + 2]

        if state.in_single_quote:
            if pair == "''":
                i += 2
                continue
            if ch == "'":
                state.in_single_quote = False
            i += 1
            continue

        if state.in_double_quote:
            if ch == '"':
                state.in_double_quote = False
            i += 1
            continue

        if pair == "--":
            break

        if pair == "/*":
            state.in_block_comment = True
            i += 2
            continue

        if ch == "$":
            match = _DOLLAR_QUOTE_RE.match(line[i:])
            if match is not None:
                state.dollar_quote_tag = match.group(1)
                i += len(match.group(1))
                continue

        if ch == "'":
            state.in_single_quote = True
            i += 1
            continue

        if ch == '"':
            state.in_double_quote = True
            i += 1
            continue

        if ch == ";":
            return True

        i += 1

    return False


def _next_content_line(lines: List[str], start_idx: int) -> Optional[int]:
    idx = start_idx
    while idx < len(lines):
        if lines[idx].strip():
            return idx
        idx += 1
    return None


def _parse_directive_line(line: str) -> Optional[Tuple[str, str]]:
    match = _DIRECTIVE_RE.match(line)
    if match is None:
        return None
    return match.group(1), match.group(2)


__all__ = ["Block", "ParseError", "parse_sql_file", "parse_sql_text"]
