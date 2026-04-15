# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Validate SPDX/copyright headers for tracked and untracked source files."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]

NORMAL_HEADER_LINES = (
    "Copyright 2024-2026 Zunor",
    "SPDX-License-Identifier: Apache-2.0",
)

DERIVED_HEADER_LINES = (
    "Copyright 2024-2026 Zunor",
    "SPDX-License-Identifier: Apache-2.0",
    "",
    "Derived from Databend (https://github.com/datafuselabs/databend),",
    "Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.",
)

EXCLUDED_PARTS = {
    ".git",
    ".venv",
    "__pycache__",
    ".pytest_cache",
    "target",
    "report",
}

EXCLUDED_SUFFIXES = {
    ".gitkeep",
    ".json",
    ".result",
    ".toml",
    ".txt",
}

EXCLUDED_BASENAMES = {
    "Cargo.lock",
    "LICENSE",
    "README.md",
    "requirements.txt",
}

MAKEFILES = {
    PurePosixPath("Makefile"),
    PurePosixPath("benchmark/Makefile"),
    PurePosixPath("regress/Makefile"),
}


@dataclass(frozen=True)
class HeaderExpectation:
    comment_prefix: str
    kind: str


def iter_repo_files() -> list[PurePosixPath]:
    result = subprocess.run(
        [
            "git",
            "-C",
            str(ROOT),
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "-z",
        ],
        check=True,
        capture_output=True,
        text=False,
    )
    entries = []
    for raw in result.stdout.split(b"\x00"):
        if not raw:
            continue
        entries.append(PurePosixPath(raw.decode("utf-8")))
    return entries


def expectation_for(path: PurePosixPath) -> HeaderExpectation | None:
    if path in MAKEFILES:
        return HeaderExpectation(comment_prefix="#", kind="normal")

    if any(part in EXCLUDED_PARTS for part in path.parts):
        return None

    if path.name in EXCLUDED_BASENAMES or path.suffix in EXCLUDED_SUFFIXES:
        return None

    if len(path.parts) >= 2 and path.parts[:2] == ("crates", "parser"):
        if path.suffix == ".rs":
            return HeaderExpectation(comment_prefix="//", kind="derived")
        if (
            len(path.parts) >= 5
            and path.parts[:5] == ("crates", "parser", "tests", "fixtures", "sql")
            and path.suffix == ".sql"
        ):
            return HeaderExpectation(comment_prefix="--", kind="derived")
        return None

    if path.parts and path.parts[0] == "crates" and path.suffix == ".rs":
        return HeaderExpectation(comment_prefix="//", kind="normal")

    if path.parts and path.parts[0] == ".github" and path.suffix in {".yml", ".yaml"}:
        return HeaderExpectation(comment_prefix="#", kind="normal")

    if path.parts and path.parts[0] == "tools" and path.suffix == ".py":
        return HeaderExpectation(comment_prefix="#", kind="normal")

    if len(path.parts) >= 1 and path.parts[0] == "benchmark":
        if path.suffix == ".py":
            return HeaderExpectation(comment_prefix="#", kind="normal")
        if len(path.parts) >= 4 and path.parts[:2] == ("benchmark", "workloads") and path.parent.name == "sql" and path.suffix == ".sql":
            return HeaderExpectation(comment_prefix="--", kind="normal")

    if len(path.parts) >= 1 and path.parts[0] == "regress":
        if path.suffix == ".py":
            return HeaderExpectation(comment_prefix="#", kind="normal")
        if len(path.parts) >= 3 and path.parts[:2] == ("regress", "cases") and path.suffix == ".sql":
            return HeaderExpectation(comment_prefix="--", kind="normal")

    return None


def build_expected_lines(expectation: HeaderExpectation) -> list[str]:
    header_body = NORMAL_HEADER_LINES if expectation.kind == "normal" else DERIVED_HEADER_LINES
    rendered = []
    for line in header_body:
        if line:
            rendered.append(f"{expectation.comment_prefix} {line}")
        else:
            rendered.append(expectation.comment_prefix)
    rendered.append("")
    return rendered


def detect_comment_prefix(line: str) -> str | None:
    for prefix in ("//", "#", "--"):
        if line == prefix or line.startswith(f"{prefix} "):
            return prefix
    return None


def actual_header_kind(lines: list[str]) -> str:
    header_region = "\n".join(lines[:6])
    return "derived" if "Derived from Databend" in header_region else "normal"


def validate_file(path: PurePosixPath, expectation: HeaderExpectation) -> list[str]:
    absolute_path = ROOT / path
    try:
        text = absolute_path.read_text(encoding="utf-8")
    except UnicodeDecodeError as exc:
        return [f"{path}: expected UTF-8 text file, decode failed: {exc}"]

    lines = text.splitlines()
    offset = 1 if lines and lines[0].startswith("#!") else 0
    expected_lines = build_expected_lines(expectation)
    actual_slice = lines[offset : offset + len(expected_lines)]

    if actual_slice == expected_lines:
        return []

    errors: list[str] = []
    expected_kind = expectation.kind
    actual_kind = actual_header_kind(lines[offset:])

    if offset >= len(lines):
        errors.append("missing header")
    else:
        found_prefix = detect_comment_prefix(lines[offset])
        if found_prefix is None:
            errors.append("missing header")
        elif found_prefix != expectation.comment_prefix:
            errors.append(
                f"wrong comment prefix: expected {expectation.comment_prefix!r}, found {found_prefix!r}"
            )

    if actual_kind != expected_kind:
        errors.append(f"wrong header type: expected {expected_kind}, found {actual_kind}")

    expected_separator_index = offset + len(expected_lines) - 1
    if expected_separator_index >= len(lines) or lines[expected_separator_index] != "":
        errors.append("header must be followed by a blank line")

    for index, expected_line in enumerate(expected_lines[:-1]):
        actual_index = offset + index
        actual_line = lines[actual_index] if actual_index < len(lines) else "<missing>"
        if actual_line != expected_line:
            errors.append(
                f"line {actual_index + 1}: expected {expected_line!r}, found {actual_line!r}"
            )
            break

    unique_errors = []
    for error in errors:
        if error not in unique_errors:
            unique_errors.append(error)
    return [f"{path}: expected {expected_kind} header ({expectation.comment_prefix}) - {error}" for error in unique_errors]


def main() -> int:
    checked = 0
    failures: list[str] = []

    for path in iter_repo_files():
        expectation = expectation_for(path)
        if expectation is None:
            continue
        checked += 1
        failures.extend(validate_file(path, expectation))

    if failures:
        print("header check failed:", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        print(f"checked {checked} files; found {len(failures)} issue(s)", file=sys.stderr)
        return 1

    print(f"header check passed for {checked} files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
