"""Execution engine for parsed SQL test blocks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import re
from typing import Any, Callable, Iterable, Sequence

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


@dataclass
class ExecutionResult:
    """Execution result for one SQL case file."""

    query_outputs: list[QueryOutput] = field(default_factory=list)
    skipped_due_to_setup_error: bool = False
    setup_error: str | None = None


class _ClientCopyAbort(RuntimeError):
    """Sentinel exception used to trigger COPY FROM STDIN aborts."""


def execute_blocks(
    conn: Any,
    blocks: Iterable[Block],
    *,
    engine: str = "paro",
    float_precision: int = 6,
    control_handler: Callable[[Any, Block], Any] | None = None,
) -> ExecutionResult:
    """Execute parsed blocks with setup/main/teardown state semantics."""
    conn.autocommit = True
    ordered_blocks = list(blocks)

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
            )
    except Exception as exc:
        main_error = exc
    finally:
        teardown_error = _execute_teardown(conn, teardown_blocks)

    if teardown_error is not None:
        if main_error is not None:
            raise ExecutionError(
                f"{main_error}\n\nteardown error: {teardown_error}"
            ) from teardown_error
        raise teardown_error

    if main_error is not None:
        raise main_error

    return result


def _execute_main_blocks(
    conn: Any,
    main_blocks: Sequence[Block],
    result: ExecutionResult,
    *,
    engine: str,
    float_precision: int,
    control_handler: Callable[[Any, Block], Any] | None,
) -> None:
    pending_conditions: list[Block] = []

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

        if block.kind == "control":
            if control_handler is None:
                raise ExecutionError(
                    f"control block at line {block.line_no} requires a control handler."
                )
            conn = control_handler(conn, block)
            continue

        if block.kind == "statement":
            output = _execute_statement(conn, block, index=index)
            if output:
                result.query_outputs.append(output)
            continue

        if block.kind == "copy":
            result.query_outputs.append(_execute_copy(conn, block, block_index=index))
            continue

        if block.kind == "query":
            if (block.query_mode or "nosort") == "file":
                result.query_outputs.append(
                    _execute_file_query(
                        block,
                        block_index=index,
                        float_precision=float_precision,
                    )
                )
            else:
                result.query_outputs.append(
                    _execute_query(
                        conn,
                        block,
                        block_index=index,
                        float_precision=float_precision,
                    )
                )
            continue

        raise ExecutionError(f"unsupported block kind at line {block.line_no}: {block.kind}")


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
