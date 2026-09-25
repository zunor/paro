# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""The maintained Q39 fixture must exercise the real independent contract."""

import json
import math
from pathlib import Path
import sys

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
from capture_result_difference import encode_value
from verify_integer_aggregate_relation import verify


def test_q39_input_selection_and_relational_roles():
    fixture = ROOT / "corpora/contracts/q39"
    with duckdb.connect() as connection:
        connection.execute("CREATE TABLE item(i_item_sk INTEGER); INSERT INTO item VALUES (2)")
        connection.execute("CREATE TABLE warehouse(w_warehouse_sk INTEGER); INSERT INTO warehouse VALUES (1)")
        connection.execute("CREATE TABLE date_dim(d_date_sk INTEGER, d_year INTEGER, d_moy INTEGER)")
        connection.execute("INSERT INTO date_dim VALUES (1,2001,1),(2,2001,2),(3,2000,1),(4,2001,3)")
        connection.execute("CREATE TABLE inventory(inv_item_sk INTEGER, inv_warehouse_sk INTEGER, inv_date_sk INTEGER, inv_quantity_on_hand INTEGER)")
        connection.execute("INSERT INTO inventory VALUES (2,1,1,0),(2,1,1,4),(2,1,2,0),(2,1,2,6),(2,1,3,99),(2,1,4,99),(9,1,1,99)")
        rows = connection.execute((fixture / "q39-integer-input-bags.sql").read_text()).fetchall()
    assert rows == [(1, 2, 1, 0), (1, 2, 1, 4), (1, 2, 2, 0), (1, 2, 2, 6)]
    inputs = [[encode_value(value) for value in row] for row in rows]
    result = [[encode_value(value) for value in (1, 2, 1, 2.0, math.sqrt(8)/2, 1, 2, 2, 3.0, math.sqrt(18)/3)]]
    report = verify(
        {"actual": inputs, "expected": inputs},
        {"actual": result, "expected": result, "oracle_status": "bounded fixture"},
        json.loads((fixture / "q39-numeric-relation.json").read_text()),
    )
    assert report["input_rows"] == 4
    assert report["reference_rows"] == 1
    for arm in report["engines"].values():
        assert arm["relational_bag_and_order"] == "pass"
        assert arm["numeric_schedule_enclosure"] == "pass"
