# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

import sys
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "corpora"))
from benchmark_evidence import ImmutableDataSeed, content_digest, tree_digest, isolated_paro_server
from tpcds_compare import verify_measurement_inputs


class SnapshotTests(unittest.TestCase):
    def test_absolute_catalog_roots_are_rejected_before_launch(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            catalog = root / "instance" / "meta" / "catalog.json"
            catalog.parent.mkdir(parents=True)
            for storage in [str(root / "databases" / "db-1"), "../outside", "databases/../outside"]:
                catalog.write_text(json.dumps({"format_version": 1, "databases": [{"storage_dir": storage}]}))
                with self.assertRaisesRegex(ValueError, "not root-relative"):
                    ImmutableDataSeed.capture(root)
            catalog.write_text(json.dumps({"format_version": 1, "databases": [{"storage_dir": "./databases/db-1"}]}))
            seed = ImmutableDataSeed.capture(root)
            with seed.snapshot() as snapshot:
                self.assertEqual(tree_digest(snapshot.path), seed.sha256)

    def test_process_local_writes_do_not_change_the_seed_or_the_next_process(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            seed_path = root / "seed"
            seed_path.mkdir()
            (seed_path / "owner").write_text("original")
            seed = ImmutableDataSeed.capture(seed_path)
            private_paths = []
            for _ in range(3):
                with seed.snapshot() as snapshot:
                    private_paths.append(snapshot.path)
                    self.assertNotEqual(snapshot.path, seed.path)
                    self.assertEqual(tree_digest(snapshot.path), seed.sha256)
                    self.assertEqual(snapshot.identity()["initial_sha256"], seed.sha256)
                    (snapshot.path / "owner").write_text("server changed the checkpoint")
                    (snapshot.path / "wal").write_bytes(b"new process state")
                self.assertFalse(snapshot.path.exists())
                seed.verify_unchanged()
            self.assertEqual(len(set(private_paths)), 3)

    def test_exceptions_cleanup_only_the_private_copy(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary)
            (path / "state").write_text("input")
            seed = ImmutableDataSeed.capture(path)
            with self.assertRaisesRegex(RuntimeError, "query failed"):
                with seed.snapshot() as snapshot:
                    (snapshot.path / "state").write_text("partial")
                    raise RuntimeError("query failed")
            self.assertFalse(snapshot.path.exists())
            self.assertEqual((path / "state").read_text(), "input")

    def test_input_drift_and_symlinks_fail_closed(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary)
            (path / "state").write_text("before")
            seed = ImmutableDataSeed.capture(path)
            with self.assertRaisesRegex(RuntimeError, "immutable benchmark seed changed"):
                with seed.snapshot():
                    (path / "state").write_text("after")
            with self.assertRaisesRegex(RuntimeError, "snapshot differs"):
                with seed.snapshot():
                    self.fail("changed seed must not admit another process")
            (path / "link").symlink_to(path / "state")
            with self.assertRaisesRegex(ValueError, "link or special file"):
                ImmutableDataSeed.capture(path)

    def test_server_factory_never_launches_on_its_seed(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "seed"
            path.mkdir()
            (path / "state").write_text("input")
            seed = ImmutableDataSeed.capture(path)
            with patch("benchmark_evidence.ManagedParoServer") as server_type:
                for _ in range(2):
                    with isolated_paro_server(path / "parod", seed, "127.0.0.1:6440",
                                              path.parent / "log", max_memory="2GB", threads=4):
                        arguments, keywords = server_type.call_args
                        self.assertNotEqual(arguments[1], seed.path)
                        self.assertEqual(tree_digest(arguments[1]), seed.sha256)
                        self.assertEqual(keywords["input_snapshot"]["seed_sha256"], seed.sha256)
                self.assertEqual(server_type.call_count, 2)
                with self.assertRaisesRegex(ValueError, "logs must not write"):
                    with isolated_paro_server(path / "parod", seed, "127.0.0.1:6440",
                                              path / "log", max_memory="2GB", threads=4):
                        self.fail("cannot admit a writer inside the seed")
                self.assertEqual(server_type.call_count, 2)

    def test_qualification_rechecks_every_declared_input(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            files = [root / name for name in ("binary", "harness", "duckdb")]
            for file in files:
                file.write_text(file.name)
            args = SimpleNamespace()
            for field in ("query_dir", "dataset_source_dir", "server_data_dir"):
                directory = root / field
                directory.mkdir()
                (directory / "input.sql").write_text(field)
                setattr(args, field, directory)
            args.duckdb_database = files[2]
            source = {"commit": "fixed", "dirty": False}
            report = {
                "source": source,
                "build_attestation": {"source": source, "binary_sha256": content_digest(files[0])},
                "harness": {"files": [{"path": str(files[1]), "sha256": content_digest(files[1])}]},
                "query_corpus_sha256": tree_digest(args.query_dir, (".sql",)),
                "dataset": {"source_sha256": tree_digest(args.dataset_source_dir),
                            "paro_data_sha256": tree_digest(args.server_data_dir),
                            "duckdb_sha256": content_digest(args.duckdb_database)},
            }
            with patch("tpcds_compare.repository_identity", return_value=source):
                verify_measurement_inputs(root, files[0], args, report)
                for file in files + [getattr(args, field) / "input.sql" for field in
                                     ("query_dir", "dataset_source_dir", "server_data_dir")]:
                    original = file.read_bytes()
                    file.write_bytes(b"changed")
                    with self.assertRaisesRegex(RuntimeError, "measurement inputs changed"):
                        verify_measurement_inputs(root, files[0], args, report)
                    file.write_bytes(original)
            with patch("tpcds_compare.repository_identity", return_value={"commit": "different"}):
                with self.assertRaisesRegex(RuntimeError, "measurement inputs changed: source"):
                    verify_measurement_inputs(root, files[0], args, report)


if __name__ == "__main__":
    unittest.main()
