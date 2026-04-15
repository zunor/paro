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
                        query_mode="nosort",
                        normalizers=pending_normalizers,
                    )
                )
            else:
                blocks.append(
                    Block(
                        kind="statement",
                        line_no=idx + 1,
                        sql=sql,
                        statement_expect="ok",
                        normalizers=pending_normalizers,
                    )
                )
            pending_normalizers = ()
            idx = next_idx
            continue

        name, arg = directive
        name = name.lower()
        line_no = idx + 1

        if name == "normalize":
            pending_normalizers = _parse_normalize_arg(arg, source, line_no)
            idx += 1
            continue

        if name == "control":
            action, control_args = _parse_control_arg(arg, source, line_no)
            blocks.append(
                Block(
                    kind="control",
                    line_no=line_no,
                    sql="",
                    control_action=action,
                    control_args=control_args,
                )
            )
            idx += 1
            continue

        if name in {"setup", "teardown"}:
            sql, next_idx = _consume_sql_statement(lines, idx + 1, source, line_no)
            blocks.append(Block(kind=name, line_no=line_no, sql=sql))
            idx = next_idx
            continue

        if name == "statement":
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
                    statement_expect=expect,
                    expected_count=expected_count,
                    error_pattern=error_pattern,
                    normalizers=normalizers,
                )
            )
            pending_normalizers = ()
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
                    query_mode=mode,
                    epsilon=epsilon,
                    normalizers=normalizers,
                )
            )
            pending_normalizers = ()
            idx = next_idx
            continue

        if name == "copy":
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
                    copy_direction=direction,
                    copy_data_lines=copy_data_lines,
                    copy_fail_message=copy_fail_message,
                    normalizers=normalizers,
                )
            )
            pending_normalizers = ()
            idx = next_idx
            continue

        if name in {"skipif", "onlyif"}:
            engine = arg.strip()
            if not engine:
                raise ParseError(f"{source}:{line_no}: '@{name}' requires an engine argument.")

            next_content = _next_content_line(lines, idx + 1)
            if next_content is None or _parse_directive_line(lines[next_content]) is not None:
                blocks.append(Block(kind=name, line_no=line_no, sql="", engine=engine))
                idx += 1
                continue

            sql, next_idx = _consume_sql_statement(lines, idx + 1, source, line_no)
            blocks.append(Block(kind=name, line_no=line_no, sql=sql, engine=engine))
            idx = next_idx
            continue

        raise ParseError(f"{source}:{line_no}: unsupported directive '@{name}'.")

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
