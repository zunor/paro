"""Result serialization and comparison for SQL query outputs."""

from __future__ import annotations

from dataclasses import dataclass
import json
import hashlib
import math
from pathlib import Path
import re
from typing import Any, Sequence
import difflib

from .normalize import normalize_value
from .normalizers import apply_normalizers
from .executor import QueryOutput

_DIRECTIVE_RE = re.compile(r"^\s*--\s*@(query|statement|copy)(?:\s+(.*?))?\s*$", re.IGNORECASE)
_NORMALIZE_DIRECTIVE_RE = re.compile(r"^\s*--\s*@normalize\s+(.+?)\s*$", re.IGNORECASE)
_APPROX_RE = re.compile(r"^approx\(\s*([^)]+)\s*\)$", re.IGNORECASE)
_HASH_LINE_RE = re.compile(r"^\s*(\d+)\s+values\s+hashing\s+to\s+([0-9a-f]{32})\s*$", re.IGNORECASE)
_RESERVED_TOKENS = {"NULL", "(empty)"}

_QUERY_MODES = {"nosort", "rowsort", "valuesort", "hash", "approx", "file", "json"}


class ResultParseError(ValueError):
    """Raised when a .result transcript cannot be parsed."""


class ResultMismatch(AssertionError):
    """Raised when actual query output differs from expected .result."""
    def __init__(self, message: str, sql: str = "", line_no: int = 0, expected: str = "", actual: str = ""):
        super().__init__(message)
        self.sql = sql
        self.line_no = line_no
        self.expected = expected
        self.actual = actual


@dataclass(frozen=True)
class ResultBlock:
    """Parsed one result transcript block."""

    index: int
    line_no: int
    kind: str
    directive_line: str
    mode: str
    epsilon: float | None
    copy_direction: str | None
    sql: str
    columns: list[str]
    rows: list[list[Any]]
    raw_result_lines: list[str]
    normalizers: tuple[str, ...] = ()


@dataclass
class _ScanState:
    in_single_quote: bool = False
    in_double_quote: bool = False
    in_block_comment: bool = False
    dollar_quote_tag: str | None = None


def parse_result_file(path: str | Path) -> list[ResultBlock]:
    file_path = Path(path)
    return parse_result_text(file_path.read_text(encoding="utf-8"), source=str(file_path))


def parse_result_text(text: str, source: str = "<memory>") -> list[ResultBlock]:
    """Parse transcript text into structured blocks."""
    lines = text.splitlines()
    idx = 0
    blocks: list[ResultBlock] = []

    while idx < len(lines):
        if not lines[idx].strip():
            idx += 1
            continue

        line_no = idx + 1
        kind: str | None = None
        mode = "nosort"
        epsilon: float | None = None
        copy_direction: str | None = None
        normalizers: tuple[str, ...] = ()
        directive_lines: list[str] = []

        while idx < len(lines):
            query_or_statement = _parse_directive_line_optional(lines[idx])
            if query_or_statement is not None:
                kind, mode, epsilon, copy_direction = query_or_statement
                directive_lines.append(lines[idx].strip())
                idx += 1
                continue

            normalize_profiles = _parse_normalize_directive_optional(lines[idx], source, idx + 1)
            if normalize_profiles is not None:
                normalizers = normalize_profiles
                directive_lines.append(lines[idx].strip())
                idx += 1
                continue

            break

        # Extract SQL
        sql, next_idx = _consume_sql_statement(lines, idx, source, line_no)
        idx = next_idx

        # If no query/statement directive was provided, infer it from SQL.
        if kind is None:
            if _is_likely_query(sql):
                kind, mode, epsilon = "query", "nosort", None
            else:
                kind, mode, epsilon = "statement", "ok", None

        if kind == "copy":
            payload, idx = _consume_copy_payload(lines, idx, source, line_no)
            rows: list[list[Any]] = []
            columns: list[str] = []
            raw_result_lines = payload
        else:
            payload = []
            while idx < len(lines) and lines[idx].strip() and not _is_block_directive_line(lines[idx]):
                # We also check if the line looks like the start of a NEW block (SQL)
                # but that's hard without parsing.
                # In build_transcript we ensure blank lines between blocks.
                payload.append(lines[idx])
                idx += 1

            if mode == "hash":
                rows = []
                columns = []
                raw_result_lines = payload
            elif kind == "statement" and not payload:
                rows = []
                columns = []
                raw_result_lines = []
            elif kind == "statement" and len(payload) == 1 and "\t" not in payload[0]:
                columns = []
                rows = []
                raw_result_lines = payload
            else:
                if payload:
                    header = payload[0]
                    columns = [_decode_text(cell) for cell in header.split("\t")] if header else [""]
                    rows = [_decode_row_tokens(row_line.split("\t")) for row_line in payload[1:]]
                else:
                    columns = []
                    rows = []
                raw_result_lines = payload

        blocks.append(
            ResultBlock(
                index=len(blocks) + 1,
                line_no=line_no,
                kind=kind,
                directive_line="\n".join(directive_lines),
                mode=mode,
                epsilon=epsilon,
                copy_direction=copy_direction,
                sql=sql,
                columns=columns,
                rows=rows,
                raw_result_lines=raw_result_lines,
                normalizers=normalizers,
            )
        )

    return blocks


def _consume_copy_payload(
    lines: list[str],
    start_idx: int,
    source: str,
    directive_line_no: int,
) -> tuple[list[str], int]:
    if start_idx >= len(lines) or lines[start_idx].strip() != "-- @copydata":
        raise ResultParseError(
            f"{source}:{directive_line_no}: COPY transcript requires '-- @copydata' after SQL."
        )

    payload = [lines[start_idx]]
    idx = start_idx + 1

    while idx < len(lines):
        stripped = lines[idx].strip()
        if stripped.startswith("-- @copyfail") or stripped == "-- @copystatus":
            break
        payload.append(lines[idx])
        idx += 1

    if idx < len(lines) and lines[idx].strip().startswith("-- @copyfail"):
        payload.append(lines[idx])
        idx += 1

    if idx < len(lines) and lines[idx].strip() == "-- @copystatus":
        payload.append(lines[idx])
        idx += 1

        while idx < len(lines) and lines[idx].strip() and not _is_block_directive_line(lines[idx]):
            payload.append(lines[idx])
            idx += 1

    return payload, idx


def _parse_directive_line_optional(line: str) -> tuple[str, str, float | None, str | None] | None:
    match = _DIRECTIVE_RE.match(line)
    if match is None:
        return None

    kind = match.group(1).lower()
    arg = (match.group(2) or "").strip().lower()

    if kind == "statement":
        return kind, arg or "ok", None, None

    if kind == "copy":
        if arg not in {"in", "out"}:
            return None
        return kind, "nosort", None, arg

    if not arg:
        return kind, "nosort", None, None
    if arg in _QUERY_MODES and arg != "approx":
        return kind, arg, None, None

    approx = _APPROX_RE.match(arg)
    if approx is not None:
        epsilon_text = approx.group(1)
        try:
            epsilon = float(epsilon_text)
            return kind, "approx", epsilon, None
        except ValueError:
            return None
    return None


def _parse_normalize_directive_optional(
    line: str, source: str, line_no: int
) -> tuple[str, ...] | None:
    match = _NORMALIZE_DIRECTIVE_RE.match(line)
    if match is None:
        return None
    return _parse_normalize_arg(match.group(1), source, line_no)


def _parse_normalize_arg(arg: str, source: str, line_no: int) -> tuple[str, ...]:
    profiles = [part.strip().lower() for part in arg.split(",")]
    if not profiles or any(not profile for profile in profiles):
        raise ResultParseError(
            f"{source}:{line_no}: invalid '@normalize' list; expected comma-separated profile names."
        )
    return tuple(profiles)


def _is_block_directive_line(line: str) -> bool:
    return _parse_directive_line_optional(line) is not None or _NORMALIZE_DIRECTIVE_RE.match(
        line
    ) is not None

_QUERY_START_RE = re.compile(
    r"^\s*(SELECT|WITH|VALUES|SHOW|DESCRIBE|EXPLAIN|CALL)", re.IGNORECASE
)

def _is_likely_query(sql: str) -> bool:
    return _QUERY_START_RE.match(sql) is not None


def build_transcript(query_outputs: Sequence[QueryOutput], *, precision: int = 6) -> str:
    """Serialize executed query outputs into .result transcript format."""
    if not query_outputs:
        return ""

    chunks: list[str] = []
    for output in query_outputs:
        directive = None
        if output.copy_direction is not None:
            directive = f"-- @copy {output.copy_direction}"
        elif not output.is_statement:
            if output.mode != "nosort":
                directive = render_query_directive(output.mode, output.epsilon)

        block_lines = []
        if directive:
            block_lines.append(directive)
        if output.normalizers:
            block_lines.append(render_normalize_directive(output.normalizers))
        block_lines.append(output.sql)

        if output.copy_direction is not None:
            block_lines.append("-- @copydata")
            block_lines.extend(output.copy_data_lines)
            if output.copy_fail_message is not None:
                block_lines.append(f"-- @copyfail {output.copy_fail_message}")
            if output.status and _should_render_copy_status(output.status):
                block_lines.append("-- @copystatus")
                block_lines.append(output.status)
        elif output.is_statement:
            rows = []
            columns = []
            if output.status and output.status != "OK":
                block_lines.append(output.status)
        elif output.mode == "hash":
            block_lines.append(_hash_summary(output, precision=precision))
        else:
            # Query with results
            block_lines.append("\t".join(_encode_text(column) for column in output.columns))
            block_lines.extend(_encode_row(output, row_index, precision=precision) for row_index in range(len(output.rows)))

        chunks.append("\n".join(block_lines))

    return "\n\n".join(chunks) + "\n"


def render_statement_directive(status: str | None) -> str:
    # Kept for compatibility if called elsewhere, but logic moved into build_transcript
    return "-- @statement ok" if status != "ERROR" else "-- @statement error"


def compare_result_file(
    *,
    expected_path: str | Path,
    query_outputs: Sequence[QueryOutput],
    precision: int = 6,
    write_actual: bool = True,
) -> None:
    """Compare actual outputs against expected .result file."""
    expected_file = Path(expected_path)
    actual_text = build_transcript(query_outputs, precision=precision)

    if not expected_file.exists():
        _maybe_write_actual(expected_file, actual_text, write_actual)
        raise ResultMismatch(f"missing expected result file: {expected_file}")

    expected_text = expected_file.read_text(encoding="utf-8")
    expected_blocks = parse_result_text(expected_text, source=str(expected_file))
    actual_blocks = parse_result_text(actual_text, source=f"{expected_file}.actual")

    mismatch_messages: list[str] = []
    if len(expected_blocks) != len(actual_blocks):
        mismatch_messages.append(
            f"block count mismatch: expected {len(expected_blocks)}, actual {len(actual_blocks)}"
        )

    for index, (expected_block, actual_block) in enumerate(
        zip(expected_blocks, actual_blocks, strict=False),
        start=1,
    ):
        message = _compare_one_block(index, expected_block, actual_block)
        if message is not None:
            mismatch_messages.append(message)
    if mismatch_messages:
        _maybe_write_actual(expected_file, actual_text, write_actual)
        # For structured reporting, we take the first mismatch as the primary one
        # but the exception message still contains all diffs for console output.
        first_exc = next((m for m in mismatch_messages if isinstance(m, ResultMismatch)), None)
        msg = "\n\n".join(str(m) for m in mismatch_messages)
        if first_exc:
            raise ResultMismatch(msg, sql=first_exc.sql, line_no=first_exc.line_no, 
                               expected=first_exc.expected, actual=first_exc.actual)
        raise ResultMismatch(msg)


def render_query_directive(mode: str | None, epsilon: float | None) -> str:
    normalized = (mode or "nosort").lower()
    if normalized == "approx":
        if epsilon is None:
            raise ValueError("approx query mode requires epsilon.")
        return f"-- @query approx({epsilon:g})"
    if normalized not in _QUERY_MODES:
        raise ValueError(f"unsupported query mode: {mode}")
    return f"-- @query {normalized}"


def render_normalize_directive(normalizers: Sequence[str]) -> str:
    profiles = [name.strip().lower() for name in normalizers]
    if not profiles or any(not profile for profile in profiles):
        raise ValueError("normalize directive requires at least one profile name.")
    return f"-- @normalize {','.join(profiles)}"


def _should_render_copy_status(status: str) -> bool:
    if status == "ERROR: no error details available":
        return False
    return status.startswith("ERROR:") or status.startswith("COPYFAIL:")


def encode_cell(value: Any, *, precision: int = 6) -> str:
    """Encode one cell with result escaping and reserved token handling."""
    if value is None:
        return "NULL"
    if isinstance(value, str) and value == "":
        return "(empty)"

    normalized = normalize_value(value, precision=precision)
    encoded = _encode_text(normalized)
    if encoded in _RESERVED_TOKENS:
        encoded = "\\" + encoded
    return encoded


def decode_cell(token: str) -> Any:
    """Decode one cell token from result transcript."""
    if token == "NULL":
        return None
    if token == "(empty)":
        return ""
    if token == "\\NULL":
        return "NULL"
    if token == "\\(empty)":
        return "(empty)"
    return _decode_text(token)


def _decode_row_tokens(tokens: list[str]) -> list[Any]:
    return [decode_cell(token) for token in tokens]


def _compare_one_block(index: int, expected: ResultBlock, actual: ResultBlock) -> str | None:
    if expected.kind != actual.kind:
        return _format_block_message(
            index,
            expected.sql,
            f"kind mismatch: expected {expected.kind}, actual {actual.kind}",
            expected.raw_result_lines,
            actual.raw_result_lines,
            line_no=expected.line_no,
        )

    if expected.mode != actual.mode:
        return _format_block_message(
            index,
            expected.sql,
            f"mode mismatch: expected {expected.mode}, actual {actual.mode}",
            expected.raw_result_lines,
            actual.raw_result_lines,
        )

    if expected.copy_direction != actual.copy_direction:
        return _format_block_message(
            index,
            expected.sql,
            (
                "copy direction mismatch: expected "
                f"{expected.copy_direction}, actual {actual.copy_direction}"
            ),
            expected.raw_result_lines,
            actual.raw_result_lines,
            line_no=expected.line_no,
        )

    if expected.sql.strip() != actual.sql.strip():
        return _format_block_message(
            index,
            expected.sql,
            "SQL mismatch between expected and actual transcript blocks",
            [expected.sql],
            [actual.sql],
        )

    profiles = expected.normalizers or actual.normalizers
    expected_lines = apply_normalizers(list(expected.raw_result_lines), profiles)
    actual_lines = apply_normalizers(list(actual.raw_result_lines), profiles)

    if expected.mode == "hash":
        return _compare_hash_block(index, expected, actual, expected_lines, actual_lines)

    if expected.mode == "approx":
        return _compare_approx_block(index, expected, actual, expected_lines, actual_lines)

    if expected.mode == "json":
        expected_json_block = _reparse_block_with_lines(expected, expected_lines)
        actual_json_block = _reparse_block_with_lines(actual, actual_lines)
        return _compare_json_block(index, expected_json_block, actual_json_block)

    if expected_lines != actual_lines:
        return _format_block_message(
            index,
            expected.sql,
            "text mismatch",
            expected_lines,
            actual_lines,
            line_no=expected.line_no
        )
    return None


def _compare_hash_block(
    index: int,
    expected: ResultBlock,
    actual: ResultBlock,
    expected_lines: list[str],
    actual_lines: list[str],
) -> str | None:
    expected_count, expected_hash = _parse_hash_line(expected_lines[0], expected.line_no)
    actual_count, actual_hash = _parse_hash_line(actual_lines[0], actual.line_no)

    if expected_count != actual_count or expected_hash != actual_hash:
        detail = (
            "hash mismatch: "
            f"expected {expected_count} values hashing to {expected_hash}, "
            f"actual {actual_count} values hashing to {actual_hash}"
        )
        return _format_block_message(
            index,
            expected.sql,
            detail,
            expected_lines,
            actual_lines,
        )
    return None


def _compare_approx_block(
    index: int,
    expected: ResultBlock,
    actual: ResultBlock,
    expected_lines: list[str],
    actual_lines: list[str],
) -> str | None:
    epsilon = expected.epsilon if expected.epsilon is not None else actual.epsilon
    if epsilon is None:
        return _format_block_message(
            index,
            expected.sql,
            "approx block missing epsilon",
            expected_lines,
            actual_lines,
        )

    if expected.columns != actual.columns:
        return _format_block_message(
            index,
            expected.sql,
            "column mismatch in approx block",
            expected_lines,
            actual_lines,
        )

    if len(expected.rows) != len(actual.rows):
        return _format_block_message(
            index,
            expected.sql,
            "row count mismatch in approx block",
            expected_lines,
            actual_lines,
        )

    for row_idx, (expected_row, actual_row) in enumerate(
        zip(expected.rows, actual.rows, strict=False),
        start=1,
    ):
        if len(expected_row) != len(actual_row):
            return _format_block_message(
                index,
                expected.sql,
                f"column count mismatch at row {row_idx}",
                expected_lines,
                actual_lines,
            )

        for col_idx, (expected_cell, actual_cell) in enumerate(
            zip(expected_row, actual_row, strict=False),
            start=1,
        ):
            if _cells_close(expected_cell, actual_cell, epsilon):
                continue
            return _format_block_message(
                index,
                expected.sql,
                (
                    f"approx mismatch at row {row_idx}, col {col_idx}: "
                    f"expected {expected_cell!r}, actual {actual_cell!r}, epsilon={epsilon}"
                ),
                expected_lines,
                actual_lines,
            )

    return None


def _compare_json_block(
    index: int,
    expected: ResultBlock,
    actual: ResultBlock,
) -> str | None:
    if expected.columns != actual.columns:
        return _format_block_message(
            index,
            expected.sql,
            "column mismatch in json block",
            expected.raw_result_lines,
            actual.raw_result_lines,
        )

    if len(expected.rows) != len(actual.rows):
        return _format_block_message(
            index,
            expected.sql,
            "row count mismatch in json block",
            expected.raw_result_lines,
            actual.raw_result_lines,
        )

    for row_idx, (expected_row, actual_row) in enumerate(
        zip(expected.rows, actual.rows, strict=False),
        start=1,
    ):
        if len(expected_row) != len(actual_row):
            return _format_block_message(
                index,
                expected.sql,
                f"column count mismatch at row {row_idx}",
                expected.raw_result_lines,
                actual.raw_result_lines,
            )
        for col_idx, (expected_cell, actual_cell) in enumerate(
            zip(expected_row, actual_row, strict=False),
            start=1,
        ):
            try:
                expected_json = _canonicalize_json_value(json.loads(str(expected_cell)))
                actual_json = _canonicalize_json_value(json.loads(str(actual_cell)))
            except json.JSONDecodeError as exc:
                raise ResultParseError(
                    f"line {expected.line_no}: invalid JSON cell in json block: {exc}"
                ) from exc
            if expected_json != actual_json:
                return _format_block_message(
                    index,
                    expected.sql,
                    f"json mismatch at row {row_idx}, col {col_idx}",
                    [json.dumps(expected_json, indent=2, sort_keys=True)],
                    [json.dumps(actual_json, indent=2, sort_keys=True)],
                )

    return None


def _reparse_block_with_lines(block: ResultBlock, raw_result_lines: list[str]) -> ResultBlock:
    if not raw_result_lines:
        return ResultBlock(
            index=block.index,
            line_no=block.line_no,
            kind=block.kind,
            directive_line=block.directive_line,
            mode=block.mode,
            epsilon=block.epsilon,
            copy_direction=block.copy_direction,
            sql=block.sql,
            columns=[],
            rows=[],
            raw_result_lines=[],
            normalizers=block.normalizers,
        )

    header = raw_result_lines[0]
    columns = [_decode_text(cell) for cell in header.split("\t")] if header else [""]
    rows = [_decode_row_tokens(row_line.split("\t")) for row_line in raw_result_lines[1:]]
    return ResultBlock(
        index=block.index,
        line_no=block.line_no,
        kind=block.kind,
        directive_line=block.directive_line,
        mode=block.mode,
        epsilon=block.epsilon,
        copy_direction=block.copy_direction,
        sql=block.sql,
        columns=columns,
        rows=rows,
        raw_result_lines=raw_result_lines,
        normalizers=block.normalizers,
    )


def _cells_close(expected: Any, actual: Any, epsilon: float) -> bool:
    expected_float = _to_float(expected)
    actual_float = _to_float(actual)

    if expected_float is not None and actual_float is not None:
        if math.isnan(expected_float) and math.isnan(actual_float):
            return True
        if math.isinf(expected_float) or math.isinf(actual_float):
            return expected_float == actual_float
        return abs(expected_float - actual_float) <= epsilon

    return expected == actual


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _canonicalize_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _canonicalize_json_value(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [_canonicalize_json_value(item) for item in value]
    return value


def _format_block_message(
    block_index: int,
    sql: str,
    detail: str,
    expected_lines: Sequence[str],
    actual_lines: Sequence[str],
    line_no: int = 0,
) -> ResultMismatch:
    expected_text = "\n".join(expected_lines)
    actual_text = "\n".join(actual_lines)
    diff = "\n".join(
        difflib.unified_diff(
            list(expected_lines),
            list(actual_lines),
            fromfile="expected",
            tofile="actual",
            lineterm="",
        )
    )
    msg = (
        f"block {block_index} mismatch (line {line_no})\n"
        f"SQL: {sql}\n"
        f"detail: {detail}\n"
        f"diff:\n{diff}"
    )
    return ResultMismatch(msg, sql=sql, line_no=line_no, expected=expected_text, actual=actual_text)


def _encode_row(output: QueryOutput, row_index: int, *, precision: int) -> str:
    row_tokens: list[str] = []
    normalized_row = output.rows[row_index]
    raw_row = output.raw_rows[row_index] if row_index < len(output.raw_rows) else ()

    for col_index, normalized_cell in enumerate(normalized_row):
        if col_index < len(raw_row):
            token = encode_cell(raw_row[col_index], precision=precision)
        else:
            token = _encode_text(str(normalized_cell))
        row_tokens.append(token)
    return "\t".join(row_tokens)


def _hash_summary(output: QueryOutput, *, precision: int) -> str:
    values: list[str] = []
    for row_index, normalized_row in enumerate(output.rows):
        raw_row = output.raw_rows[row_index] if row_index < len(output.raw_rows) else ()
        for col_index, normalized_cell in enumerate(normalized_row):
            if col_index < len(raw_row):
                token = encode_cell(raw_row[col_index], precision=precision)
            else:
                token = _encode_text(str(normalized_cell))
            values.append(token)

    digest = hashlib.md5("\n".join(values).encode("utf-8")).hexdigest()
    return f"{len(values)} values hashing to {digest}"


def _maybe_write_actual(path: Path, transcript: str, write_actual: bool) -> None:
    if not write_actual:
        return
    actual_path = path.with_suffix(".result.actual")
    actual_path.write_text(transcript, encoding="utf-8")


def _parse_directive_line(line: str, source: str, line_no: int) -> tuple[str, str, float | None]:
    match = _DIRECTIVE_RE.match(line)
    if match is None:
        raise ResultParseError(
            f"{source}:{line_no}: expected '-- @query ...', '-- @statement ...', or '-- @copy ...' directive line."
        )

    kind = match.group(1).lower()
    arg = (match.group(2) or "").strip().lower()

    if kind == "statement":
        return kind, arg or "ok", None
    if kind == "copy":
        if arg in {"in", "out"}:
            return kind, arg, None
        raise ResultParseError(f"{source}:{line_no}: unsupported copy direction '{arg}'.")

    if not arg:
        return kind, "nosort", None

    if arg in _QUERY_MODES and arg != "approx":
        return kind, arg, None

    approx = _APPROX_RE.match(arg)
    if approx is not None:
        epsilon_text = approx.group(1)
        try:
            epsilon = float(epsilon_text)
        except ValueError as exc:
            raise ResultParseError(
                f"{source}:{line_no}: invalid approx epsilon '{epsilon_text}'."
            ) from exc
        return kind, "approx", epsilon

    raise ResultParseError(f"{source}:{line_no}: unsupported query mode '{arg}'.")


def _consume_sql_statement(
    lines: Sequence[str],
    start_idx: int,
    source: str,
    directive_line_no: int,
) -> tuple[str, int]:
    if start_idx >= len(lines):
        raise ResultParseError(f"{source}:{directive_line_no}: missing SQL after directive.")

    idx = start_idx
    collected: list[str] = []
    state = _ScanState()

    while idx < len(lines):
        line = lines[idx]
        collected.append(line)
        if _scan_line_for_terminator(line, state):
            return "\n".join(collected).strip(), idx + 1
        idx += 1

    raise ResultParseError(f"{source}:{directive_line_no}: SQL statement must end with ';'.")


def _scan_line_for_terminator(line: str, state: _ScanState) -> bool:
    i = 0
    while i < len(line):
        if state.in_block_comment:
            end = line.find("*/", i)
            if end == -1:
                return False
            state.in_block_comment = False
            i = end + 2
            continue

        if state.dollar_quote_tag is not None:
            tag = state.dollar_quote_tag
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
            match = re.match(r"(\$[A-Za-z_][A-Za-z0-9_]*\$|\$\$)", line[i:])
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


def _parse_hash_line(line: str, line_no: int) -> tuple[int, str]:
    match = _HASH_LINE_RE.match(line)
    if match is None:
        raise ResultParseError(f"line {line_no}: invalid hash summary line: {line!r}")
    return int(match.group(1)), match.group(2).lower()


def _encode_text(text: str) -> str:
    return (
        text.replace("\\", "\\\\")
        .replace("\t", "\\t")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )


def _decode_text(text: str) -> str:
    result: list[str] = []
    idx = 0
    while idx < len(text):
        ch = text[idx]
        if ch != "\\":
            result.append(ch)
            idx += 1
            continue

        if idx + 1 >= len(text):
            result.append("\\")
            idx += 1
            continue

        nxt = text[idx + 1]
        if nxt == "t":
            result.append("\t")
        elif nxt == "n":
            result.append("\n")
        elif nxt == "r":
            result.append("\r")
        elif nxt == "\\":
            result.append("\\")
        else:
            result.append(nxt)
        idx += 2

    return "".join(result)


__all__ = [
    "ResultBlock",
    "ResultMismatch",
    "ResultParseError",
    "build_transcript",
    "compare_result_file",
    "decode_cell",
    "encode_cell",
    "parse_result_file",
    "parse_result_text",
    "render_normalize_directive",
    "render_query_directive",
]
