# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "corpora"))
from benchmark_evidence import parse_statement_trace_log
from cold_planning import diagnostic_rows


class DiagnosticRowsTests(unittest.TestCase):
    def test_qualified_pgwire_names_and_typed_values(self):
        columns = ["name", "kind", "last_elapsed_us", "metric_value", "metric_unit", "invocation_count"]
        values = [("memo_exploration", "search", 200, 1, "invocations", 1)]
        self.assertEqual(diagnostic_rows(columns, values),
                         diagnostic_rows(["paro_optimizers." + name for name in columns], values))
        self.assertEqual(diagnostic_rows(columns, values)[0]["last_elapsed_us"], 200)

    def test_schema_and_row_arity_fail_closed(self):
        with self.assertRaises(ValueError):
            diagnostic_rows(["name", "name"], [(1, 2)])
        with self.assertRaises(ValueError):
            diagnostic_rows(["name", "kind", "last_elapsed_us", "metric_value", "metric_unit", "invocation_count"], [(1,)])

    def test_statement_trace_parser_handles_ansi_styled_fields(self):
        line = (
            "\x1b[2mparo::statement_trace\x1b[0m: statement trace event "
            "\x1b[3mprocess_id\x1b[0m\x1b[2m=\x1b[0m123 "
            "\x1b[3msession_id\x1b[0m\x1b[2m=\x1b[0m4 "
            "\x1b[3moperation_id\x1b[0m\x1b[2m=\x1b[0m9 "
            "\x1b[3mtrace_sample_id\x1b[0m\x1b[2m=\x1b[0msample-0 "
            "\x1b[3mschema_version\x1b[0m\x1b[2m=\x1b[0m2 "
            "\x1b[3mstatement_id\x1b[0m\x1b[2m=\x1b[0m9 "
            "\x1b[3mstatement_index\x1b[0m\x1b[2m=\x1b[0m0 "
            "\x1b[3mquery_len\x1b[0m\x1b[2m=\x1b[0m8 "
            "\x1b[3mquery_fingerprint\x1b[0m\x1b[2m=\x1b[0m17 "
            "\x1b[3msequence\x1b[0m\x1b[2m=\x1b[0m0 "
            "\x1b[3mphase\x1b[0m\x1b[2m=\x1b[0mfrontend "
            "\x1b[3mevent\x1b[0m\x1b[2m=\x1b[0mparse_entry "
            "\x1b[3melapsed_us\x1b[0m\x1b[2m=\x1b[0m3 "
            "\x1b[3mduration_us\x1b[0m\x1b[2m=\x1b[0m0 "
            "\x1b[3mhas_duration\x1b[0m\x1b[2m=\x1b[0mfalse "
            "\x1b[3mvalue\x1b[0m\x1b[2m=\x1b[0m0 "
            "\x1b[3mhas_value\x1b[0m\x1b[2m=\x1b[0mfalse"
        )
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "trace.log"
            path.write_text(line + "\n")
            traces = parse_statement_trace_log(path)
        self.assertEqual(len(traces), 1)
        self.assertEqual(traces[0]["statement_id"], 9)
        self.assertEqual(traces[0]["events"][0]["event"], "parse_entry")


if __name__ == "__main__":
    unittest.main()
