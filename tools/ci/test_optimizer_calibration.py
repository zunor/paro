# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Calibration guards reject misleading provenance and non-identifiable fits."""

import copy
import hashlib
import json
from pathlib import Path
import tempfile
import tomllib
import unittest

import generate_optimizer_calibration as generator
from optimizer_calibration_receipt import validate_receipt


class ArtifactTests(unittest.TestCase):
    def setUp(self):
        self.payload = tomllib.loads(generator.SOURCE.read_text())

    def test_current_bootstrap_is_valid(self):
        generator._validate(self.payload)
        self.assertEqual(self.payload["artifact"]["provenance"], "bootstrap")

    def test_nonfinite_fields_cannot_be_generated(self):
        for target, key in (("artifact", "risk_weight"), ("parallelism", "coordination_latency_upper"),
                            ("coefficient", "latency_upper")):
            for value in (float("nan"), float("inf"), float("-inf")):
                payload = copy.deepcopy(self.payload)
                table = payload[target][0] if target == "coefficient" else payload[target]
                table[key] = value
                with self.subTest(target=target, value=value), self.assertRaises(ValueError):
                    generator._validate(payload)

    def test_uncovered_fixture_cannot_claim_window_coverage(self):
        next(row for row in self.payload["coefficient"] if row["id"] == 13)["validation_queries"] = ["ordered_aggregate"]
        with self.assertRaisesRegex(ValueError, "cannot claim a validation fixture"):
            generator._validate(self.payload)

    def test_filter_cannot_claim_read_before_write_materialization(self):
        next(row for row in self.payload["coefficient"] if row["id"] == 20001)["validation_queries"] = ["count_filter"]
        with self.assertRaisesRegex(ValueError, "cannot claim a validation fixture"):
            generator._validate(self.payload)

    def test_inactive_or_uncovered_classes_block_measured_provenance(self):
        self.payload["artifact"]["provenance"] = "measured"
        with self.assertRaisesRegex(ValueError, "fully measured bundle"):
            generator._validate(self.payload)


class ReceiptTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        source_files = []
        for name in ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml", "crates/execution/src/lib.rs"):
            snapshot = "snapshot/" + name
            path = self.root / snapshot
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("captured dirty source: " + name)
            source_files.append({"path": name, "snapshot_file": snapshot,
                                 "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
        identity = {"mode": "snapshot", "git_commit": "a" * 40, "working_tree_sha256": "b" * 64}
        source = {**identity, "manifest": self.write("source.json", {
            "identity": identity, "scope": "recorded-source-subset", "files": source_files,
        })}
        columns = [{"name": "build", "class_id": 1, "feature_unit": "row"},
                   {"name": "probe_output", "class_id": 2, "feature_unit": "row"}]
        self.receipt = {
            "schema_version": 1, "source": source, "binary_sha256": "c" * 64,
            "units": {"time": "ns", "latency": "reference-normalized", "reference_ns": 10,
                      "reference_id": "fixed-reference-dop1"},
            "columns": columns, "coefficients_ns": [10, 20],
            "split": {"train": ["train-build", "train-probe"], "holdout": ["holdout"]},
            "holdout_gate": {"max_relative_error": 0.1},
        }
        self.run = {key: copy.deepcopy(self.receipt[key]) for key in
                    ("schema_version", "source", "binary_sha256", "units", "columns")}
        self.run["timer_scope"] = {"kind": "operator_work", "admitted_workers": 1, "observed_workers": 1,
                                   "profile_mode": "off", "includes": ["consume", "merge", "finish"],
                                   "excludes": ["runtime_init", "publication", "drop", "input_construction"]}
        self.run["reference_measurement"] = {"id": "fixed-reference-dop1", "samples_ns": [9, 10, 11]}
        self.run["holdout_gate"] = copy.deepcopy(self.receipt["holdout_gate"])
        self.run["cells"] = [
            {"id": "train-build", "split_key": "build/scale1/seed1", "status": "Completed",
             "features": [1, 0], "samples_ns": [9, 10, 11]},
            {"id": "train-probe", "split_key": "probe/scale1/seed1", "status": "Completed",
             "features": [0, 1], "samples_ns": [19, 20, 21]},
            {"id": "holdout", "split_key": "both/scale2/seed2", "status": "Completed",
             "features": [1, 2], "samples_ns": [49, 50, 51]},
        ]

    def write(self, name, payload):
        data = json.dumps(payload, sort_keys=True).encode()
        (self.root / name).write_bytes(data)
        return {"file": name, "sha256": hashlib.sha256(data).hexdigest()}

    def seal(self):
        self.receipt["runs"] = [self.write("run.json", self.run)]
        return self.write("receipt.json", self.receipt)

    def validate(self):
        return validate_receipt(self.root, self.seal(), {1, 2}, {1: 1, 2: 2})

    def test_dirty_snapshot_rank_and_holdout_are_recomputed(self):
        result = self.validate()
        self.assertEqual(result, {"rank": 2, "holdout_cells": 1, "max_relative_error": 0.0})

    def test_captured_source_content_hash_is_checked(self):
        (self.root / "snapshot/Cargo.lock").write_text("different dependencies")
        with self.assertRaisesRegex(ValueError, "source file hash mismatch"):
            self.validate()

    def test_actual_run_file_hash_is_checked(self):
        reference = self.seal()
        (self.root / "run.json").write_text("{}")
        with self.assertRaisesRegex(ValueError, "input hash mismatch"):
            validate_receipt(self.root, reference, {1, 2}, {1: 1, 2: 2})

    def test_run_source_cannot_be_borrowed_from_another_binary(self):
        self.run["binary_sha256"] = "d" * 64
        with self.assertRaisesRegex(ValueError, "identity/units/design"):
            self.validate()

    def test_correlated_features_are_not_independent_coefficients(self):
        self.run["cells"][0]["features"] = [1, 2]
        self.run["cells"][1]["features"] = [2, 4]
        with self.assertRaisesRegex(ValueError, "rank deficient"):
            self.validate()

    def test_holdout_must_not_split_repeated_samples_from_one_case(self):
        self.run["cells"][2]["split_key"] = self.run["cells"][0]["split_key"]
        with self.assertRaisesRegex(ValueError, "holdout reuses"):
            self.validate()

    def test_holdout_gate_uses_observed_samples_and_fitted_coefficients(self):
        self.run["cells"][2]["samples_ns"] = [100, 101, 102]
        with self.assertRaisesRegex(ValueError, "holdout error"):
            self.validate()

    def test_units_and_finite_samples_are_required(self):
        self.receipt["columns"][0]["feature_unit"] = "nanoseconds"
        with self.assertRaisesRegex(ValueError, "feature unit"):
            self.validate()
        self.receipt["columns"][0]["feature_unit"] = "row"
        self.run["cells"][0]["samples_ns"] = [float("nan")]
        with self.assertRaisesRegex(ValueError, "sample_ns"):
            self.validate()

    def test_failed_cell_is_not_silently_removed(self):
        self.run["cells"][0]["status"] = "Failed"
        with self.assertRaisesRegex(ValueError, "incomplete measurement cell"):
            self.validate()

    def test_fit_cannot_be_associated_with_different_artifact_coefficients(self):
        with self.assertRaisesRegex(ValueError, "does not match"):
            validate_receipt(self.root, self.seal(), {1, 2}, {1: 2, 2: 2})

    def test_experimental_fit_is_not_bound_to_shipped_bootstrap_coefficients(self):
        result = validate_receipt(self.root, self.seal(), {1, 2})
        self.assertEqual(result["rank"], 2)

    def test_inactive_class_cannot_be_fitted(self):
        with self.assertRaisesRegex(ValueError, "inactive classes"):
            validate_receipt(self.root, self.seal(), {20002})

    def test_parallel_or_diagnostic_timing_cannot_identify_serial_work(self):
        self.run["timer_scope"]["observed_workers"] = 4
        with self.assertRaisesRegex(ValueError, "serial trace-off"):
            self.validate()

    def test_worker_counts_require_integers_not_boolean_or_float_aliases(self):
        for key in ("admitted_workers", "observed_workers"):
            for value in (True, 1.0):
                with self.subTest(key=key, value=value):
                    self.run["timer_scope"][key] = value
                    with self.assertRaisesRegex(ValueError, "serial trace-off"):
                        self.validate()
                    self.run["timer_scope"][key] = 1

    def test_timer_phase_lists_reject_missing_contradictory_or_ambiguous_scope(self):
        valid = copy.deepcopy(self.run["timer_scope"])
        for includes, excludes in (
            ([], valid["excludes"]),
            (valid["includes"], []),
            (["consume", "consume", "merge", "finish"], valid["excludes"]),
            (valid["includes"], valid["excludes"] + ["drop"]),
            (valid["includes"], valid["excludes"] + ["consume"]),
            (valid["includes"], ["input_construction"]),
            (valid["includes"] + ["unrecorded_setup"], valid["excludes"]),
            (valid["includes"] + ["input_construction"], valid["excludes"][:-1]),
            ("consume", valid["excludes"]),
            (valid["includes"], "input_construction"),
            (["consume", ["merge"], "finish"], valid["excludes"]),
        ):
            with self.subTest(includes=includes, excludes=excludes):
                self.run["timer_scope"] = {**valid, "includes": includes, "excludes": excludes}
                with self.assertRaisesRegex(ValueError, "timer phase|timer scope"):
                    self.validate()

    def test_complete_timer_phase_classification_allows_explicitly_included_overhead(self):
        self.run["timer_scope"]["includes"] += ["runtime_init", "publication", "drop"]
        self.run["timer_scope"]["excludes"] = ["input_construction"]
        self.assertEqual(self.validate()["rank"], 2)

    def test_fit_runs_must_use_the_same_timer_phase_classification(self):
        first = copy.deepcopy(self.run)
        first["cells"] = self.run["cells"][:2]
        second = copy.deepcopy(self.run)
        second["cells"] = self.run["cells"][2:]
        second["timer_scope"]["includes"].append("drop")
        second["timer_scope"]["excludes"].remove("drop")
        self.receipt["runs"] = [self.write("first.json", first), self.write("second.json", second)]
        with self.assertRaisesRegex(ValueError, "classification differs"):
            validate_receipt(self.root, self.write("receipt.json", self.receipt), {1, 2}, {1: 1, 2: 2})

    def test_reference_scale_requires_actual_retained_measurement(self):
        self.run["reference_measurement"]["samples_ns"] = [19, 20, 21]
        with self.assertRaisesRegex(ValueError, "reference_ns"):
            self.validate()


if __name__ == "__main__":
    unittest.main()
