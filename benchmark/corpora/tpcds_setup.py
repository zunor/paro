#!/usr/bin/env python3
# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Load TPC-DS CSV data with its generator-guaranteed keys declared to Paro."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import psycopg
from psycopg import sql


# TPC-DS dsdgen guarantees these relation keys. They are optimizer metadata,
# not write-time indexes: the corpus is immutable after loading and Paro's
# `NOT ENFORCED` contract records exactly that division of responsibility.
DECLARED_KEYS: dict[str, tuple[str, ...]] = {
    "call_center": ("cc_call_center_sk",),
    "catalog_page": ("cp_catalog_page_sk",),
    "catalog_returns": ("cr_item_sk", "cr_order_number"),
    "catalog_sales": ("cs_item_sk", "cs_order_number"),
    "customer": ("c_customer_sk",),
    "customer_address": ("ca_address_sk",),
    "customer_demographics": ("cd_demo_sk",),
    "date_dim": ("d_date_sk",),
    "household_demographics": ("hd_demo_sk",),
    "income_band": ("ib_income_band_sk",),
    "inventory": ("inv_date_sk", "inv_item_sk", "inv_warehouse_sk"),
    "item": ("i_item_sk",),
    "promotion": ("p_promo_sk",),
    "reason": ("r_reason_sk",),
    "ship_mode": ("sm_ship_mode_sk",),
    "store": ("s_store_sk",),
    "store_returns": ("sr_item_sk", "sr_ticket_number"),
    "store_sales": ("ss_item_sk", "ss_ticket_number"),
    "time_dim": ("t_time_sk",),
    "warehouse": ("w_warehouse_sk",),
    "web_page": ("wp_web_page_sk",),
    "web_returns": ("wr_item_sk", "wr_order_number"),
    "web_sales": ("ws_item_sk", "ws_order_number"),
    "web_site": ("web_site_sk",),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--csv-dir", type=Path, required=True)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--memory-limit", default="2GiB")
    parser.add_argument(
        "--load-only",
        action="store_true",
        help="resume an interrupted setup after all empty tables were created",
    )
    return parser.parse_args()


def declared_schema_statements(schema_path: Path) -> list[tuple[str, str]]:
    statements: list[tuple[str, str]] = []
    for raw in schema_path.read_text(encoding="utf-8").split(";"):
        statement = raw.strip()
        if not statement:
            continue
        match = re.match(r"(?is)^create\s+table\s+([a-z_][a-z0-9_]*)\s*\(", statement)
        if match is None or not statement.endswith(")"):
            raise ValueError(f"unsupported schema statement: {statement[:80]!r}")
        table = match.group(1).lower()
        columns = DECLARED_KEYS.get(table)
        if columns is None:
            raise ValueError(f"TPC-DS key declaration is missing for {table}")
        key = ", ".join(columns)
        statements.append(
            (table, f"{statement[:-1]}, UNIQUE ({key}) NOT ENFORCED)")
        )
    if set(DECLARED_KEYS) != {table for table, _ in statements}:
        missing = sorted(set(DECLARED_KEYS) - {table for table, _ in statements})
        raise ValueError(f"schema is missing declared TPC-DS tables: {missing}")
    return statements


def load_statements(load_path: Path) -> list[str]:
    statements = []
    for raw in load_path.read_text(encoding="utf-8").split(";"):
        statement = raw.strip()
        if not statement:
            continue
        # dsdgen's DuckDB loader omits SQL's WITH keyword and quotes the
        # format name. Normalize only that dialect surface; paths and CSV
        # options remain source-owned.
        statement = statement.replace(" (FORMAT 'csv'", " WITH (FORMAT csv")
        statements.append(statement)
    return statements


def main() -> int:
    args = parse_args()
    schema = declared_schema_statements(args.csv_dir / "schema.sql")
    loads = load_statements(args.csv_dir / "load.sql")
    with psycopg.connect(args.dsn, autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql.SQL("SET threads = {}").format(sql.Literal(args.threads)))
            cursor.execute(
                sql.SQL("SET memory_limit = {}").format(sql.Literal(args.memory_limit))
            )
            if not args.load_only:
                for table, statement in schema:
                    cursor.execute(statement)
                    print(f"created {table}", flush=True)
            for index, statement in enumerate(loads, start=1):
                cursor.execute(statement)
                print(f"loaded {index}/{len(loads)}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
