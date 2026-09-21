# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""`runner.py gate` command parser and dispatcher."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .common import GateCommandError
from .gate_bisect import run_bisect
from .gate_bless import run_bless
from .gate_calibrate import run_calibrate
from .gate_check import run_check
from ..run_output import RunOutput, RunOutputError


def run(argv: list[str], *, runner_module: object) -> int:
    parser = argparse.ArgumentParser(prog="runner.py gate", description="Run benchmark performance gates")
    configure_parser(parser)
    args = parser.parse_args(argv)
    root_dir = Path(__file__).resolve().parents[2]
    return run_parsed(args, root_dir=root_dir, runner_module=runner_module)


def configure_parser(parser: argparse.ArgumentParser) -> None:
    subparsers = parser.add_subparsers(dest="action", required=True)

    check = subparsers.add_parser("check", help="Evaluate a performance gate")
    _add_common_args(check, include_baseline=True)
    _add_archive_args(check)
    check.add_argument("--execution-mode", choices=("strict", "quorum"), default="quorum")
    check.add_argument("--quorum-retries", type=int, default=3)

    bless = subparsers.add_parser("bless", help="Refresh a gate baseline")
    _add_common_args(bless, include_baseline=True)
    bless.add_argument("--policy-evolution", action="store_true")
    bless.add_argument("--bless-runs", type=int, default=3)

    calibrate = subparsers.add_parser("calibrate", help="Append clean main/nightly observations to archive")
    _add_common_args(calibrate, include_baseline=True)
    _add_archive_args(calibrate)

    bisect = subparsers.add_parser("bisect", help="Compare this checkout against an archived gate result")
    _add_common_args(bisect, include_baseline=False)
    _add_archive_args(bisect)
    bisect.add_argument("--against", required=True, help="Archived git commit SHA or prefix to compare against")


def run_parsed(
    args: argparse.Namespace,
    *,
    root_dir: Path,
    runner_module: object,
) -> int:
    run_output = None
    try:
        report_root = args.report_root if getattr(args, "report_root", None) is not None else root_dir / "report"
        if not report_root.is_absolute():
            report_root = (Path.cwd() / report_root).resolve()
        run_output = RunOutput.create(report_root, run_id=getattr(args, "run_id", None))
        if args.action == "check":
            result = run_check(args, root_dir=root_dir, runner_module=runner_module, run_output=run_output)
            run_output.finalize(status="Completed" if result == 0 else "Failed")
            return result
        if args.action == "bless":
            result = run_bless(args, root_dir=root_dir, runner_module=runner_module, run_output=run_output)
            run_output.finalize(status="Completed" if result == 0 else "Failed")
            return result
        if args.action == "calibrate":
            result = run_calibrate(args, root_dir=root_dir, runner_module=runner_module, run_output=run_output)
            run_output.finalize(status="Completed" if result == 0 else "Failed")
            return result
        if args.action == "bisect":
            result = run_bisect(args, root_dir=root_dir, runner_module=runner_module, run_output=run_output)
            run_output.finalize(status="Completed" if result == 0 else "Failed")
            return result
    except GateCommandError as exc:
        if run_output is not None:
            run_output.finalize(status="Failed")
        print(f"gate error: {exc}", file=sys.stderr)
        return 2
    except RunOutputError as exc:
        if run_output is not None:
            run_output.finalize(status="Incomplete")
        print(f"gate error: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        if run_output is not None:
            run_output.finalize(status="Cancelled")
        raise
    return 2


def _add_common_args(parser: argparse.ArgumentParser, *, include_baseline: bool) -> None:
    parser.add_argument("--gate", required=True)
    if include_baseline:
        parser.add_argument("--baseline", default="auto")
    parser.add_argument("--policy", default="auto")
    parser.add_argument("--pid", default="auto")
    parser.add_argument("--include-source", action="append", default=[])
    parser.add_argument("--skip-source", action="append", default=[])
    parser.add_argument("--report-root", type=Path, default=None)
    parser.add_argument("--run-id", default=None)


def _add_archive_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--archive", default="auto")
    parser.add_argument("--archive-cache", default="auto")
