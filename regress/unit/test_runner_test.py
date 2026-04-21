# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from pathlib import Path

import runner
from harness.executor import ExecutionResult, QueryOutput
from harness.parser import Block


def test_discover_case_files_and_filter(tmp_path: Path) -> None:
    cases = tmp_path / "cases"
    (cases / "dml" / "select").mkdir(parents=True)
    (cases / "ddl").mkdir(parents=True)

    sql_a = cases / "ddl" / "create_table.sql"
    sql_b = cases / "dml" / "select" / "basic_select.sql"
    sql_c = cases / "dml" / "select" / "where_clause.sql"
    for sql_file in (sql_a, sql_b, sql_c):
        sql_file.write_text("SELECT 1;\n", encoding="utf-8")

    discovered = runner.discover_case_files(cases)
    assert discovered == [sql_a, sql_b, sql_c]

    filtered = runner.discover_case_files(cases, filter_pattern="where")
    assert filtered == [sql_c]


def test_resolve_config_precedence(tmp_path: Path) -> None:
    config_file = tmp_path / "config.toml"
    config_file.write_text(
        "\n".join(
            [
                "[connection]",
                'host = "host_from_file"',
                "port = 6000",
                'database = "db_from_file"',
                'user = "user_from_file"',
                'password = "pass_from_file"',
                "",
                "[test]",
                "float_precision = 4",
                "write_actual = true",
                "update = false",
                "jobs = 1",
            ]
        ),
        encoding="utf-8",
    )

    args = runner.parse_args(
        [
            "--config",
            str(config_file),
            "--host",
            "host_from_cli",
            "--update",
            "--jobs",
            "3",
        ]
    )
    env = {
        "PARO_HOST": "host_from_env",
        "PARO_PORT": "7000",
        "PARO_DATABASE": "db_from_env",
        "PARO_USER": "user_from_env",
        "PARO_PASSWORD": "pass_from_env",
        "PARO_WRITE_ACTUAL": "0",
    }

    resolved = runner.resolve_config(args, env=env, root_dir=tmp_path)

    assert resolved.host == "host_from_cli"  # CLI > env > file
    assert resolved.port == 7000  # env > file
    assert resolved.database == "db_from_env"
    assert resolved.user == "user_from_env"
    assert resolved.password == "pass_from_env"
    assert resolved.update is True  # CLI override
    assert resolved.write_actual is False  # env override
    assert resolved.jobs == 3


def test_resolve_config_parses_runtime_profiles_and_resolves_paths(tmp_path: Path) -> None:
    config_file = tmp_path / "config.toml"
    fixture_dir = tmp_path / "fixtures" / "python_udf" / "bin"
    fixture_dir.mkdir(parents=True)
    fixture = fixture_dir / "python_probe_only.py"
    fixture.write_text("#!/usr/bin/env python3\n", encoding="utf-8")
    config_file.write_text(
        "\n".join(
            [
                "[connection]",
                'host = "127.0.0.1"',
                "port = 6432",
                'database = "postgres"',
                'user = "postgres"',
                'password = ""',
                "",
                "[test]",
                "float_precision = 6",
                "write_actual = true",
                "update = false",
                "jobs = 1",
                "",
                "[runtime_profiles.python_worker_crash.env]",
                'PARO_PYTHON_BIN = "fixtures/python_udf/bin/python_probe_only.py"',
                "",
                "[runtime_profiles.auth_matrix.env]",
                'PARO_CREATE_ROUTINE_USERS = "paro,routine_builder"',
                'PARO_CREATE_ELEVATED_ROUTINE_USERS = "paro"',
            ]
        ),
        encoding="utf-8",
    )

    args = runner.parse_args(["--config", str(config_file)])
    resolved = runner.resolve_config(args, env={}, root_dir=tmp_path)

    assert "default" in resolved.runtime_profiles
    assert "python_worker_crash" in resolved.runtime_profiles
    assert (
        resolved.runtime_profiles["python_worker_crash"].env["PARO_PYTHON_BIN"]
        == fixture.resolve().as_posix()
    )
    assert set(resolved.managed_runtime_env) == {
        "PARO_PYTHON_BIN",
        "PARO_CREATE_ROUTINE_USERS",
        "PARO_CREATE_ELEVATED_ROUTINE_USERS",
    }


def test_run_single_case_new_writes_actual(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path
    (root / "cases" / "dml").mkdir(parents=True)
    case_file = root / "cases" / "dml" / "new_case.sql"
    case_file.write_text("SELECT 1;\n", encoding="utf-8")

    config_file = root / "config.toml"
    config_file.write_text(
        "\n".join(
            [
                "[connection]",
                'host = "127.0.0.1"',
                "port = 6432",
                'database = "postgres"',
                'user = "postgres"',
                'password = ""',
                "",
                "[test]",
                "float_precision = 6",
                "write_actual = true",
                "update = false",
                "jobs = 1",
            ]
        ),
        encoding="utf-8",
    )

    args = runner.parse_args(["--config", str(config_file)])
    config = runner.resolve_config(args, env={}, root_dir=root)

    monkeypatch.setattr(runner, "parse_sql_file", lambda _path: [])

    query_output = QueryOutput(
        block_index=1,
        line_no=1,
        sql="SELECT 1;",
        mode="nosort",
        epsilon=None,
        columns=["v"],
        rows=[["1"]],
        raw_rows=[(1,)],
    )
    execution = ExecutionResult(query_outputs=[query_output])
    monkeypatch.setattr(
        runner,
        "execute_blocks",
        lambda conn, blocks, engine, float_precision, control_handler=None: execution,
    )

    outcome, _ = runner.run_single_case(object(), case_file, config)

    assert outcome.status == "NEW"
    rel_path_str = case_file.relative_to(config.cases_dir).as_posix().replace("/", "_")
    expected_actual = config.actuals_dir / (rel_path_str + ".actual")
    assert expected_actual.exists()


def test_prepare_case_blocks_stages_fixture_and_rewrites_sql(tmp_path: Path) -> None:
    root = tmp_path
    (root / "cases" / "python_udf").mkdir(parents=True)
    (root / "fixtures" / "python_udf" / "modules" / "basic_math").mkdir(parents=True)
    fixture_file = root / "fixtures" / "python_udf" / "modules" / "basic_math" / "basic_math.py"
    fixture_file.write_text("def add_one(x): return x + 1\n", encoding="utf-8")

    config_file = root / "config.toml"
    config_file.write_text(
        "\n".join(
            [
                "[connection]",
                'host = "127.0.0.1"',
                "port = 6432",
                'database = "postgres"',
                'user = "postgres"',
                'password = ""',
                "",
                "[test]",
                "float_precision = 6",
                "write_actual = true",
                "update = false",
                "jobs = 1",
            ]
        ),
        encoding="utf-8",
    )

    args = runner.parse_args(["--config", str(config_file)])
    config = runner.resolve_config(args, env={}, root_dir=root)
    runner.prepare_report_dir(config.report_dir)

    case_path = root / "cases" / "python_udf" / "fixture_case.sql"
    blocks = [
        Block(
            kind="query",
            line_no=1,
            sql="FILE '{{fixture:python_udf/modules/basic_math}}/basic_math.py';",
            fixture_refs=("python_udf/modules/basic_math",),
            query_mode="file",
        )
    ]

    prepared = runner._prepare_case_blocks(case_path, blocks, config)

    staged_root = config.staged_fixtures_dir / "python_udf" / "fixture_case" / "python_udf" / "modules" / "basic_math"
    assert staged_root.exists()
    assert staged_root.joinpath("basic_math.py").exists()
    assert staged_root.as_posix() in prepared[0].sql


def test_build_runtime_profile_env_clears_managed_keys(tmp_path: Path, monkeypatch) -> None:
    config_file = tmp_path / "config.toml"
    config_file.write_text(
        "\n".join(
            [
                "[connection]",
                'host = "127.0.0.1"',
                "port = 6432",
                'database = "postgres"',
                'user = "postgres"',
                'password = ""',
                "",
                "[test]",
                "float_precision = 6",
                "write_actual = true",
                "update = false",
                "jobs = 1",
            ]
        ),
        encoding="utf-8",
    )
    args = runner.parse_args(["--config", str(config_file)])
    config = runner.resolve_config(args, env={}, root_dir=tmp_path)
    profile = runner.RuntimeProfile(
        name="auth_matrix",
        env={"PARO_CREATE_ROUTINE_USERS": "paro,routine_builder"},
        unset=("PARO_CREATE_ELEVATED_ROUTINE_USERS",),
    )
    config = runner.replace(
        config,
        managed_runtime_env=(
            "PARO_CREATE_ROUTINE_USERS",
            "PARO_CREATE_ELEVATED_ROUTINE_USERS",
        ),
    )

    monkeypatch.setenv("PARO_CREATE_ROUTINE_USERS", "stale")
    monkeypatch.setenv("PARO_CREATE_ELEVATED_ROUTINE_USERS", "stale")
    built = runner._build_runtime_profile_env(config, profile)

    assert built["PARO_CREATE_ROUTINE_USERS"] == "paro,routine_builder"
    assert "PARO_CREATE_ELEVATED_ROUTINE_USERS" not in built


def test_reconnect_uses_current_connection_defaults_and_overrides(monkeypatch) -> None:
    class FakeInfo:
        host = "127.0.0.1"
        port = 6432
        dbname = "postgres"
        user = "paro"

    class FakeConn:
        def __init__(self) -> None:
            self.info = FakeInfo()
            self.closed = False

        def close(self) -> None:
            self.closed = True

    config = runner.RunnerConfig(
        host="127.0.0.1",
        port=6432,
        database="postgres",
        user="paro",
        password="",
        float_precision=6,
        update=False,
        write_actual=True,
        jobs=1,
        verbose=False,
        filter_pattern=None,
        config_path=Path("/tmp/regress/config.toml"),
        root_dir=Path("/tmp/regress"),
        report_dir=Path("/tmp/regress/report"),
        runtime_profiles={"default": runner.RuntimeProfile(name="default", env={})},
        managed_runtime_env=(),
    )
    opened: list[runner.ConnectionTarget] = []

    def fake_open_connection(
        resolved_config: runner.RunnerConfig,
        target: runner.ConnectionTarget | None = None,
    ) -> object:
        assert resolved_config is config
        assert target is not None
        opened.append(target)
        return object()

    monkeypatch.setattr(runner, "_open_connection", fake_open_connection)

    conn = FakeConn()
    reopened = runner._reconnect(
        conn,
        config,
        options={"user": "routine_builder", "database": "postgres"},
    )

    assert reopened is not None
    assert conn.closed is True
    assert opened == [
        runner.ConnectionTarget(
            host="127.0.0.1",
            port=6432,
            database="postgres",
            user="routine_builder",
            password="",
        )
    ]
