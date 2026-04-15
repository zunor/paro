from __future__ import annotations

from pathlib import Path

import runner
from harness.executor import ExecutionResult, QueryOutput


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
        lambda conn, blocks, engine, float_precision: execution,
    )

    outcome, _ = runner.run_single_case(object(), case_file, config)

    assert outcome.status == "NEW"
    rel_path_str = case_file.relative_to(config.cases_dir).as_posix().replace("/", "_")
    expected_actual = config.actuals_dir / (rel_path_str + ".actual")
    assert expected_actual.exists()
