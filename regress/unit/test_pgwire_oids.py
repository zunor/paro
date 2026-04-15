# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import os
import pathlib
import tomllib
import unittest

import psycopg


def _load_connection_config() -> dict[str, str | int]:
    config_path = pathlib.Path(__file__).resolve().parents[1] / "config.toml"
    if config_path.exists():
        config = tomllib.loads(config_path.read_text(encoding="utf-8"))
        conn_cfg = config.get("connection", {})
    else:
        conn_cfg = {}

    return {
        "host": conn_cfg.get("host", "localhost"),
        "port": conn_cfg.get("port", 6432),
        "dbname": conn_cfg.get("database", "postgres"),
        "user": conn_cfg.get("user", "postgres"),
        "password": conn_cfg.get("password", ""),
    }


class PgWireOidTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cfg = _load_connection_config()
        try:
            cls.conn = psycopg.connect(**cfg)
        except psycopg.OperationalError as exc:
            if os.getenv("PARO_REQUIRE_SERVER", "").lower() in {"1", "true", "yes", "on"}:
                raise
            raise unittest.SkipTest(
                "Paro server is not running; skipping pgwire integration test "
                "(set PARO_REQUIRE_SERVER=1 to require it)"
            ) from exc
        cls.conn.autocommit = True

    @classmethod
    def tearDownClass(cls) -> None:
        cls.conn.close()

    def test_pgwire_oids(self) -> None:
        table = "pgwire_oid_test"
        try:
            with self.conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {table};")
                cur.execute(
                    f"""
                    CREATE TABLE {table} (
                        u UUID,
                        j JSON,
                        jb JSONB,
                        tsz TIMESTAMPTZ,
                        ia ARRAY(INTEGER),
                        va ARRAY(VARCHAR)
                    );
                    """
                )
                cur.execute(
                    f"""
                    INSERT INTO {table} VALUES (
                        '550e8400-e29b-41d4-a716-446655440000',
                        '{{"a":1}}',
                        '{{"a":1}}',
                        '2024-01-01 00:00:00+00',
                        [1, 2, 3],
                        ['a', 'b']
                    );
                    """
                )
                cur.execute(f"SELECT u, j, jb, tsz, ia, va FROM {table};")
                desc = cur.description
                self.assertIsNotNone(desc)
                # UUID, JSON, JSONB, TIMESTAMPTZ
                self.assertEqual(desc[0].type_code, 2950)
                self.assertEqual(desc[1].type_code, 114)
                self.assertEqual(desc[2].type_code, 3802)
                self.assertEqual(desc[3].type_code, 1184)
                # ARRAY(INTEGER), ARRAY(VARCHAR)
                self.assertEqual(desc[4].type_code, 1007)
                self.assertEqual(desc[5].type_code, 1015)
        finally:
            with self.conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {table};")
