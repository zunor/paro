# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Execution engine for parsed SQL test blocks."""

from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError as FutureTimeout
from dataclasses import dataclass, field
from pathlib import Path
import re
import time
from typing import Any, Callable, Iterable, Mapping, Sequence

from .normalize import normalize_rows
from .parser import Block


class ExecutionError(AssertionError):
    """Raised when one block execution does not meet expectation."""


@dataclass(frozen=True)
class QueryOutput:
    """One executed query block and its normalized result."""

    block_index: int
    line_no: int
    sql: str
    mode: str
    epsilon: float | None
    columns: list[str]
    rows: list[list[str]]
    raw_rows: list[tuple[Any, ...]]
    normalizers: tuple[str, ...] = ()
    # For statements that don't return rows (e.g. CREATE TABLE)
    is_statement: bool = False
    status: str | None = None
    copy_direction: str | None = None
    copy_data_lines: tuple[str, ...] = ()
    copy_fail_message: str | None = None
    session_name: str | None = None


@dataclass
class ExecutionResult:
    """Execution result for one SQL case file."""

    query_outputs: list[QueryOutput] = field(default_factory=list)
    skipped_due_to_setup_error: bool = False
    setup_error: str | None = None


class _ClientCopyAbort(RuntimeError):
    """Sentinel exception used to trigger COPY FROM STDIN aborts."""


@dataclass
class _ManagedSession:
    name: str
    conn: Any
    options: Mapping[str, str]
    close_on_cleanup: bool


@dataclass
class _AsyncTask:
    label: str
    session_name: str
    line_no: int
    future: Future[QueryOutput]


WaitExpectMatcher = Callable[[int, QueryOutput], str | None]


def execute_blocks(
    conn: Any,
    blocks: Iterable[Block],
    *,
    engine: str = "paro",
    float_precision: int = 6,
    control_handler: Callable[[Any, Block], Any] | None = None,
    session_connection_factory: Callable[[str, Mapping[str, str]], Any] | None = None,
    wait_expect_matcher: WaitExpectMatcher | None = None,
) -> ExecutionResult:
    """Execute parsed blocks with setup/main/teardown state semantics."""
    conn.autocommit = True
    ordered_blocks = list(blocks)
    session_manager = _build_session_manager(
        conn,
        ordered_blocks,
        session_connection_factory=session_connection_factory,
    )
    async_manager = _AsyncManager()

    setup_blocks = [block for block in ordered_blocks if block.kind == "setup"]
    main_blocks = [block for block in ordered_blocks if block.kind not in {"setup", "teardown"}]
    teardown_blocks = [block for block in ordered_blocks if block.kind == "teardown"]

    result = ExecutionResult()
    setup_failed = False
    main_error: Exception | None = None

    try:
        for block in setup_blocks:
            try:
                _run_plain_sql(conn, block.sql)
            except Exception as exc:  # pragma: no cover - exercised by unit tests via fake errors
                setup_failed = True
                result.skipped_due_to_setup_error = True
                result.setup_error = (
                    f"setup failed at line {block.line_no}: {exc.__class__.__name__}: {exc}"
                )
                break

        if not setup_failed:
            _execute_main_blocks(
                conn,
                main_blocks,
                result,
                engine=engine,
                float_precision=float_precision,
                control_handler=control_handler,
                session_manager=session_manager,
                async_manager=async_manager,
                wait_expect_matcher=wait_expect_matcher,
            )
    except Exception as exc:
        main_error = exc
    finally:
        teardown_error = _execute_teardown(conn, teardown_blocks)
        async_cleanup_error = async_manager.cleanup_and_validate(session_manager)
        cleanup_error = (
            session_manager.cleanup_and_validate() if session_manager is not None else None
        )

    if teardown_error is not None:
        if main_error is not None:
            raise ExecutionError(
                f"{main_error}\n\nteardown error: {teardown_error}"
            ) from teardown_error
        raise teardown_error

    if async_cleanup_error is not None:
        if main_error is not None:
            raise ExecutionError(
                f"{main_error}\n\nasync cleanup error: {async_cleanup_error}"
            )
        raise async_cleanup_error

    if cleanup_error is not None:
        if main_error is not None:
            raise ExecutionError(f"{main_error}\n\nsession cleanup error: {cleanup_error}")
        raise cleanup_error

    if main_error is not None:
        raise main_error

    return result


class _SessionManager:
    def __init__(
        self,
        default_conn: Any,
        session_connection_factory: Callable[[str, Mapping[str, str]], Any],
    ) -> None:
        self._factory = session_connection_factory
        self._sessions: dict[str, _ManagedSession] = {
            "default": _ManagedSession(
                name="default",
                conn=default_conn,
                options={},
                close_on_cleanup=False,
            )
        }

    def replace_default(self, conn: Any) -> None:
        conn.autocommit = True
        self._sessions["default"] = _ManagedSession(
            name="default",
            conn=conn,
            options={},
            close_on_cleanup=False,
        )

    def connection_for(self, block: Block) -> Any:
        name = _block_session_name(block)
        options = _block_session_options(block)
        if name == "default":
            if options:
                raise ExecutionError(
                    f"default session at line {block.line_no} cannot override connection options."
                )
            return self._sessions["default"].conn

        existing = self._sessions.get(name)
        if existing is not None:
            if dict(existing.options) != options:
                raise ExecutionError(
                    f"session {name!r} at line {block.line_no} was already opened "
                    "with different options."
                )
            return existing.conn

        conn = self._factory(name, options)
        conn.autocommit = True
        self._sessions[name] = _ManagedSession(
            name=name,
            conn=conn,
            options=options,
            close_on_cleanup=True,
        )
        return conn

    def abort_session(self, name: str) -> None:
        session = self._sessions.pop(name, None)
        if session is None:
            return
        _safe_rollback(session.conn)
        _safe_close(session.conn)

    def cleanup_and_validate(self) -> ExecutionError | None:
        issues: list[str] = []
        for session in self._sessions.values():
            status = _transaction_status_name(session.conn)
            if status in {"ACTIVE", "INTRANS", "INERROR"}:
                if status == "INERROR":
                    issues.append(f"session {session.name!r} ended in failed transaction state")
                else:
                    issues.append(
                        f"session {session.name!r} left an open transaction ({status})"
                    )

        for session in self._sessions.values():
            _safe_rollback(session.conn)
        for session in self._sessions.values():
            if session.close_on_cleanup:
                _safe_close(session.conn)

        if issues:
            return ExecutionError("; ".join(issues))
        return None


class _AsyncManager:
    def __init__(self) -> None:
        self._executor: ThreadPoolExecutor | None = None
        self._tasks: dict[str, _AsyncTask] = {}
        self._busy_sessions: set[str] = set()

    def submit(
        self,
        *,
        label: str,
        session_name: str,
        line_no: int,
        run: Callable[[], QueryOutput],
    ) -> None:
        if label in self._tasks:
            raise ExecutionError(f"duplicate async label {label!r} at line {line_no}.")
        if session_name in self._busy_sessions:
            raise ExecutionError(
                f"session {session_name!r} is already running an async block."
            )
        if self._executor is None:
            self._executor = ThreadPoolExecutor(
                max_workers=8,
                thread_name_prefix="paro-regress-async",
            )
        future = self._executor.submit(run)
        self._tasks[label] = _AsyncTask(
            label=label,
            session_name=session_name,
            line_no=line_no,
            future=future,
        )
        self._busy_sessions.add(session_name)

    def ensure_session_idle(self, block: Block) -> None:
        session_name = _block_session_name(block)
        if session_name in self._busy_sessions:
            raise ExecutionError(
                f"session {session_name!r} is still running an async block "
                f"before line {block.line_no}; use '@await' first."
            )

    def await_output(
        self,
        label: str,
        *,
        timeout_ms: int,
        session_manager: "_SessionManager | None",
    ) -> QueryOutput:
        task = self._tasks.get(label)
        if task is None:
            raise ExecutionError(f"unknown async label {label!r}.")

        try:
            output = task.future.result(timeout=timeout_ms / 1000.0)
        except FutureTimeout as exc:
            task.future.cancel()
            if session_manager is not None:
                session_manager.abort_session(task.session_name)
            self._tasks.pop(label, None)
            self._busy_sessions.discard(task.session_name)
            raise ExecutionError(
                f"async block {label!r} at line {task.line_no} timed out "
                f"after {timeout_ms}ms; session {task.session_name!r} was disconnected."
            ) from exc
        except Exception as exc:
            self._tasks.pop(label, None)
            self._busy_sessions.discard(task.session_name)
            raise exc

        self._tasks.pop(label, None)
        self._busy_sessions.discard(task.session_name)
        return output

    def cleanup_and_validate(
        self,
        session_manager: "_SessionManager | None",
    ) -> ExecutionError | None:
        try:
            if not self._tasks:
                return None

            labels = sorted(self._tasks)
            for task in self._tasks.values():
                task.future.cancel()
                if session_manager is not None:
                    session_manager.abort_session(task.session_name)
            return ExecutionError(
                "unawaited async task(s): " + ", ".join(labels)
            )
        finally:
            if self._executor is not None:
                self._executor.shutdown(wait=False, cancel_futures=True)


def _build_session_manager(
    conn: Any,
    blocks: Sequence[Block],
    *,
    session_connection_factory: Callable[[str, Mapping[str, str]], Any] | None,
) -> _SessionManager | None:
    if not any(block.session_name is not None for block in blocks):
        return None
    if session_connection_factory is None:
        raise ExecutionError("multi-session regress case requires a session connection factory.")
    return _SessionManager(conn, session_connection_factory)


def _block_session_name(block: Block) -> str:
    return block.session_name or "default"


def _output_session_name(block: Block) -> str | None:
    name = _block_session_name(block)
    return None if name == "default" else name


def _block_session_options(block: Block) -> dict[str, str]:
    options: dict[str, str] = {}
    for raw_arg in block.session_args:
        key, value = raw_arg.split("=", 1)
        options[key] = value
    return options


def _execute_main_blocks(
    conn: Any,
    main_blocks: Sequence[Block],
    result: ExecutionResult,
    *,
    engine: str,
    float_precision: int,
    control_handler: Callable[[Any, Block], Any] | None,
    session_manager: "_SessionManager | None" = None,
    async_manager: "_AsyncManager | None" = None,
    wait_expect_matcher: WaitExpectMatcher | None = None,
) -> None:
    pending_conditions: list[Block] = []
    async_manager = async_manager or _AsyncManager()

    for index, block in enumerate(main_blocks, start=1):
        if block.kind in {"skipif", "onlyif"}:
            if block.sql:
                if _condition_allows_block(block, engine):
                    _run_plain_sql(conn, block.sql)
                continue
            pending_conditions.append(block)
            continue

        if pending_conditions:
            should_execute = all(
                _condition_allows_block(condition, engine) for condition in pending_conditions
            )
            pending_conditions.clear()
            if not should_execute:
                continue

        if block.kind == "sleep":
            if block.sleep_ms is None:
                raise ExecutionError(f"sleep block at line {block.line_no} has no duration.")
            time.sleep(block.sleep_ms / 1000.0)
            continue

        if block.kind == "await":
            if block.await_label is None or block.await_timeout_ms is None:
                raise ExecutionError(f"await block at line {block.line_no} is incomplete.")
            output = async_manager.await_output(
                block.await_label,
                timeout_ms=block.await_timeout_ms,
                session_manager=session_manager,
            )
            result.query_outputs.append(output)
            continue

        if block.kind == "control":
            if session_manager is not None and block.session_name not in (None, "default"):
                raise ExecutionError(
                    f"control block at line {block.line_no} must run on the default session."
                )
            if control_handler is None:
                raise ExecutionError(
                    f"control block at line {block.line_no} requires a control handler."
                )
            conn = control_handler(conn, block)
            if session_manager is not None:
                session_manager.replace_default(conn)
            continue

        if block.async_label is not None:
            if session_manager is None:
                raise ExecutionError(
                    f"async block at line {block.line_no} requires a named session."
                )
            session_name = _block_session_name(block)
            if session_name == "default":
                raise ExecutionError(
                    f"async block at line {block.line_no} must use a named session."
                )
            if block.wait_expect_interval_ms is not None:
                raise ExecutionError(
                    f"async block at line {block.line_no} cannot also use '@wait_expect'."
                )
            active_conn = session_manager.connection_for(block)
            async_manager.submit(
                label=block.async_label,
                session_name=session_name,
                line_no=block.line_no,
                run=lambda active_conn=active_conn, block=block, index=index: _execute_output_block(
                    active_conn,
                    block,
                    block_index=index,
                    output_index=None,
                    float_precision=float_precision,
                    wait_expect_matcher=None,
                ),
            )
            continue

        async_manager.ensure_session_idle(block)
        active_conn = (
            session_manager.connection_for(block) if session_manager is not None else conn
        )

        output = _execute_output_block(
            active_conn,
            block,
            block_index=index,
            output_index=len(result.query_outputs) + 1,
            float_precision=float_precision,
            wait_expect_matcher=wait_expect_matcher,
        )
        if output is not None:
            result.query_outputs.append(output)
            continue

        raise ExecutionError(f"unsupported block kind at line {block.line_no}: {block.kind}")


def _execute_output_block(
    conn: Any,
    block: Block,
    *,
    block_index: int,
    output_index: int | None,
    float_precision: int,
    wait_expect_matcher: WaitExpectMatcher | None,
) -> QueryOutput | None:
    if block.kind == "statement":
        return _execute_statement(conn, block, index=block_index)

    if block.kind == "copy":
        return _execute_copy(conn, block, block_index=block_index)

    if block.kind == "query":
        if (block.query_mode or "nosort") == "file":
            if block.wait_expect_interval_ms is not None:
                raise ExecutionError(
                    f"file query at line {block.line_no} cannot use '@wait_expect'."
                )
            return _execute_file_query(
                block,
                block_index=block_index,
                float_precision=float_precision,
            )

        if block.wait_expect_interval_ms is not None:
            if output_index is None:
                raise ExecutionError(
                    f"query at line {block.line_no} cannot use '@wait_expect' in async mode."
                )
            return _execute_query_with_wait_expect(
                conn,
                block,
                block_index=block_index,
                output_index=output_index,
                float_precision=float_precision,
                wait_expect_matcher=wait_expect_matcher,
            )

        return _execute_query(
            conn,
            block,
            block_index=block_index,
            float_precision=float_precision,
        )

    return None


def _execute_query_with_wait_expect(
    conn: Any,
    block: Block,
    *,
    block_index: int,
    output_index: int,
    float_precision: int,
    wait_expect_matcher: WaitExpectMatcher | None,
) -> QueryOutput:
    if block.wait_expect_interval_ms is None or block.wait_expect_timeout_ms is None:
        raise ExecutionError(f"wait_expect query at line {block.line_no} is incomplete.")

    # Update mode has no stable baseline to poll against. Execute once so the
    # generated result records the final product state chosen by the case author.
    if wait_expect_matcher is None:
        return _execute_query(
            conn,
            block,
            block_index=block_index,
            float_precision=float_precision,
        )

    interval_seconds = block.wait_expect_interval_ms / 1000.0
    timeout_seconds = block.wait_expect_timeout_ms / 1000.0
    deadline = time.monotonic() + timeout_seconds
    last_mismatch: str | None = None

    while True:
        output = _execute_query(
            conn,
            block,
            block_index=block_index,
            float_precision=float_precision,
        )
        mismatch = wait_expect_matcher(output_index, output)
        if mismatch is None:
            return output

        last_mismatch = mismatch
        if time.monotonic() >= deadline:
            rendered = last_mismatch or "result did not match before timeout"
            raise ExecutionError(
                f"wait_expect timed out at line {block.line_no} after "
                f"{block.wait_expect_timeout_ms}ms.\n{rendered}"
            )
        time.sleep(interval_seconds)


def _execute_teardown(conn: Any, teardown_blocks: Sequence[Block]) -> ExecutionError | None:
    for block in teardown_blocks:
        try:
            _run_plain_sql(conn, block.sql)
        except Exception as exc:  # pragma: no cover - trivial forwarding
            return ExecutionError(
                f"teardown failed at line {block.line_no}: {exc.__class__.__name__}: {exc}"
            )
    return None


def _execute_statement(conn: Any, block: Block, index: int) -> QueryOutput:
    expect = block.statement_expect
    if expect is None:
        raise ExecutionError(f"statement block at line {block.line_no} has no expectation.")

    if expect == "error":
        return _execute_statement_expect_error(conn, block, index)

    try:
        with conn.cursor() as cursor:
            cursor.execute(block.sql)
            status = cursor.statusmessage or "OK"
            if expect == "count":
                if block.expected_count is None:
                    raise ExecutionError(
                        f"statement count block at line {block.line_no} has no expected rowcount."
                    )
                if cursor.rowcount != block.expected_count:
                    raise ExecutionError(
                        "statement count mismatch at line "
                        f"{block.line_no}: expected {block.expected_count}, got {cursor.rowcount}\n"
                        f"SQL: {block.sql}"
                    )
            elif expect != "ok":
                raise ExecutionError(f"unsupported statement expectation at line {block.line_no}: {expect}")

            return QueryOutput(
                block_index=index,
                line_no=block.line_no,
                sql=block.sql,
                mode="nosort",
                epsilon=None,
                columns=[],
                rows=[],
                raw_rows=[],
                normalizers=block.normalizers,
                is_statement=True,
                status=status,
                session_name=_output_session_name(block),
            )
    except Exception as exc:
        if expect == "ok":
            _safe_rollback(conn)
            # Capture error as result for comparison
            return _capture_error_as_output(block, index, exc)
        raise exc


def _execute_copy(conn: Any, block: Block, *, block_index: int) -> QueryOutput:
    direction = block.copy_direction
    if direction == "out":
        return _execute_copy_out(conn, block, block_index=block_index)
    if direction == "in":
        return _execute_copy_in(conn, block, block_index=block_index)

    raise ExecutionError(
        f"copy block at line {block.line_no} has unsupported direction: {direction}"
    )


def _execute_copy_out(conn: Any, block: Block, *, block_index: int) -> QueryOutput:
    try:
        with conn.cursor() as cursor:
            copy_chunks: list[str] = []
            with cursor.copy(block.sql) as copy:
                while True:
                    data = copy.read()
                    if not data:
                        break
                    copy_chunks.append(_decode_copy_chunk(data))

            payload = "".join(copy_chunks)
            status = cursor.statusmessage or "OK"
            return QueryOutput(
                block_index=block_index,
                line_no=block.line_no,
                sql=block.sql,
                mode="nosort",
                epsilon=None,
                columns=[],
                rows=[],
                raw_rows=[],
                normalizers=block.normalizers,
                is_statement=True,
                status=status,
                copy_direction="out",
                copy_data_lines=tuple(payload.splitlines()),
                session_name=_output_session_name(block),
            )
    except Exception as exc:
        _safe_rollback(conn)
        return _capture_copy_error_as_output(block, block_index, exc)


def _execute_copy_in(conn: Any, block: Block, *, block_index: int) -> QueryOutput:
    payload = _encode_copy_payload(block.copy_data_lines)

    try:
        with conn.cursor() as cursor:
            try:
                with cursor.copy(block.sql) as copy:
                    if payload:
                        copy.write(payload)
                    if block.copy_fail_message is not None:
                        raise _ClientCopyAbort(block.copy_fail_message)
            except _ClientCopyAbort as exc:
                return QueryOutput(
                    block_index=block_index,
                    line_no=block.line_no,
                    sql=block.sql,
                    mode="nosort",
                    epsilon=None,
                    columns=[],
                    rows=[],
                    raw_rows=[],
                    normalizers=block.normalizers,
                    is_statement=True,
                    status=f"COPYFAIL: {exc}",
                    copy_direction="in",
                    copy_data_lines=block.copy_data_lines,
                    copy_fail_message=block.copy_fail_message,
                    session_name=_output_session_name(block),
                )

            status = cursor.statusmessage or "OK"
            return QueryOutput(
                block_index=block_index,
                line_no=block.line_no,
                sql=block.sql,
                mode="nosort",
                epsilon=None,
                columns=[],
                rows=[],
                raw_rows=[],
                normalizers=block.normalizers,
                is_statement=True,
                status=status,
                copy_direction="in",
                copy_data_lines=block.copy_data_lines,
                copy_fail_message=block.copy_fail_message,
                session_name=_output_session_name(block),
            )
    except Exception as exc:
        _safe_rollback(conn)
        return _capture_copy_error_as_output(block, block_index, exc)

def _capture_error_as_output(block: Block, index: int, exc: Exception) -> QueryOutput:
    error_msg = _format_error_summary(exc)
    return QueryOutput(
        block_index=index,
        line_no=block.line_no,
        sql=block.sql,
        mode="nosort",
        epsilon=None,
        columns=[],
        rows=[],
        raw_rows=[],
        normalizers=block.normalizers,
        is_statement=True,
        status=f"ERROR: {error_msg}",
        session_name=_output_session_name(block),
    )


def _capture_copy_error_as_output(block: Block, index: int, exc: Exception) -> QueryOutput:
    error_msg = _format_error_summary(exc)
    return QueryOutput(
        block_index=index,
        line_no=block.line_no,
        sql=block.sql,
        mode="nosort",
        epsilon=None,
        columns=[],
        rows=[],
        raw_rows=[],
        normalizers=block.normalizers,
        is_statement=True,
        status=f"ERROR: {error_msg}",
        copy_direction=block.copy_direction,
        copy_data_lines=block.copy_data_lines,
        copy_fail_message=block.copy_fail_message,
        session_name=_output_session_name(block),
    )


def _execute_statement_expect_error(conn: Any, block: Block, index: int) -> QueryOutput:
    pattern = block.error_pattern

    try:
        with conn.cursor() as cursor:
            cursor.execute(block.sql)
            # If it succeeded, we'll raise ExecutionError below
    except Exception as exc:
        _safe_rollback(conn)
        error_text = _format_error_text(exc)
        if pattern is not None:
            if re.search(pattern, error_text, flags=re.IGNORECASE) is None:
                sqlstate = _extract_sqlstate(exc)
                raise ExecutionError(
                    "statement error pattern mismatch at line "
                    f"{block.line_no}\nSQL: {block.sql}\n"
                    f"Pattern: {pattern}\n"
                    f"Message: {exc}\n"
                    f"SQLSTATE: {sqlstate or '(none)'}\n"
                    f"Exception: {exc.__class__.__name__}"
                ) from exc
        
        # Return the error message as the "result"
        error_msg = _format_error_summary(exc)
        return QueryOutput(
            block_index=index,
            line_no=block.line_no,
            sql=block.sql,
            mode="nosort",
            epsilon=None,
            columns=[],
            rows=[],
            raw_rows=[],
            normalizers=block.normalizers,
            is_statement=True,
            status=f"ERROR: {error_msg}",
            session_name=_output_session_name(block),
        )

    raise ExecutionError(
        f"statement at line {block.line_no} expected error pattern '{pattern}', but succeeded.\n"
        f"SQL: {block.sql}"
    )


def _execute_query(
    conn: Any,
    block: Block,
    *,
    block_index: int,
    float_precision: int,
) -> QueryOutput:
    mode = block.query_mode or "nosort"

    try:
        with conn.cursor() as cursor:
            cursor.execute(block.sql)
            description = cursor.description
            if description is None:
                raise ExecutionError(f"query block at line {block.line_no} returned no columns.")
            raw_rows = [tuple(row) for row in cursor.fetchall()]

        columns = _extract_column_names(description)
        normalized_rows = normalize_rows(raw_rows, precision=float_precision)

        if mode == "rowsort":
            paired = sorted(
                zip(normalized_rows, raw_rows, strict=True),
                key=lambda item: item[0],
            )
            normalized_rows = [item[0] for item in paired]
            raw_rows = [item[1] for item in paired]
        elif mode == "valuesort":
            paired = sorted(
                zip(normalized_rows, raw_rows, strict=True),
                key=lambda item: item[0],
            )
            normalized_rows = [item[0] for item in paired]
            raw_rows = [item[1] for item in paired]

        return QueryOutput(
            block_index=block_index,
            line_no=block.line_no,
            sql=block.sql,
            mode=mode,
            epsilon=block.epsilon,
            columns=columns,
            rows=normalized_rows,
            raw_rows=raw_rows,
            normalizers=block.normalizers,
            session_name=_output_session_name(block),
        )
    except Exception as exc:
        _safe_rollback(conn)
        return _capture_error_as_output(block, block_index, exc)


_FILE_QUERY_RE = re.compile(r"^\s*file\s+'([^']*)'\s*;?\s*$", re.IGNORECASE)
_STRING_QUERY_RE = re.compile(r"^\s*'([^']*)'\s*;?\s*$")


def _parse_file_query_path(sql: str, line_no: int) -> str:
    match = _FILE_QUERY_RE.match(sql)
    if match is not None:
        return match.group(1)
    match = _STRING_QUERY_RE.match(sql)
    if match is not None:
        return match.group(1)
    raise ExecutionError(
        "file query expects FILE '<path>'; or a single-quoted path literal "
        f"(line {line_no})"
    )


def _execute_file_query(
    block: Block,
    *,
    block_index: int,
    float_precision: int,
) -> QueryOutput:
    try:
        path = _parse_file_query_path(block.sql, block.line_no)
        text = Path(path).read_text(encoding="utf-8")
        lines = text.splitlines()
        raw_rows = [(line,) for line in lines]
        normalized_rows = normalize_rows(raw_rows, precision=float_precision)
        return QueryOutput(
            block_index=block_index,
            line_no=block.line_no,
            sql=block.sql,
            mode="file",
            epsilon=None,
            columns=["line"],
            rows=normalized_rows,
            raw_rows=raw_rows,
            normalizers=block.normalizers,
            session_name=_output_session_name(block),
        )
    except Exception as exc:
        return _capture_error_as_output(block, block_index, exc)


def _decode_copy_chunk(data: Any) -> str:
    if isinstance(data, str):
        return data
    if isinstance(data, memoryview):
        return data.tobytes().decode("utf-8")
    if isinstance(data, (bytes, bytearray)):
        return bytes(data).decode("utf-8")
    return bytes(data).decode("utf-8")


def _encode_copy_payload(lines: tuple[str, ...]) -> str:
    if not lines:
        return ""
    return "\n".join(lines) + "\n"


def _run_plain_sql(conn: Any, sql: str) -> None:
    with conn.cursor() as cursor:
        cursor.execute(sql)


def _condition_allows_block(block: Block, engine: str) -> bool:
    target = (block.engine or "").strip().lower()
    current = engine.strip().lower()

    if block.kind == "skipif":
        return target != current
    if block.kind == "onlyif":
        return target == current

    raise ExecutionError(f"unsupported condition block kind: {block.kind}")


def _safe_rollback(conn: Any) -> None:
    rollback = getattr(conn, "rollback", None)
    if rollback is None:
        return
    try:
        rollback()
    except Exception:
        pass


def _safe_close(conn: Any) -> None:
    close = getattr(conn, "close", None)
    if close is None:
        return
    try:
        close()
    except Exception:
        pass


def _transaction_status_name(conn: Any) -> str:
    info = getattr(conn, "info", None)
    status = getattr(info, "transaction_status", None)
    if status is None:
        return "IDLE"
    name = getattr(status, "name", None)
    if name is not None:
        return str(name).upper()
    text = str(status).upper()
    for candidate in ("IDLE", "ACTIVE", "INTRANS", "INERROR", "UNKNOWN"):
        if candidate in text:
            return candidate
    return "IDLE"


def _extract_sqlstate(exc: Exception) -> str | None:
    sqlstate = getattr(exc, "sqlstate", None)
    if sqlstate:
        return str(sqlstate)

    diag = getattr(exc, "diag", None)
    sqlstate = getattr(diag, "sqlstate", None)
    if sqlstate:
        return str(sqlstate)
    return None


def _format_error_text(exc: Exception) -> str:
    sqlstate = _extract_sqlstate(exc)
    if sqlstate:
        return f"{exc}\nSQLSTATE={sqlstate}"
    return str(exc)


def _format_error_summary(exc: Exception) -> str:
    diag = getattr(exc, "diag", None)
    primary = getattr(diag, "message_primary", None) or str(exc)
    primary = str(primary).splitlines()[0]

    suffixes: list[str] = []
    sqlstate = _extract_sqlstate(exc)
    if sqlstate:
        suffixes.append(f"SQLSTATE={sqlstate}")

    detail = getattr(diag, "message_detail", None)
    if detail:
        suffixes.append(f"DETAIL={str(detail).splitlines()[0]}")

    hint = getattr(diag, "message_hint", None)
    if hint:
        suffixes.append(f"HINT={str(hint).splitlines()[0]}")

    if suffixes:
        return f"{primary} ({'; '.join(suffixes)})"
    return primary


def _extract_column_names(description: Sequence[Any]) -> list[str]:
    names: list[str] = []
    for column in description:
        name = getattr(column, "name", None)
        if name is None and isinstance(column, Sequence) and column:
            name = column[0]
        names.append(str(name))
    return names


__all__ = [
    "ExecutionError",
    "ExecutionResult",
    "QueryOutput",
    "execute_blocks",
]
