# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

import copy
import hashlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "corpora"))
from job_setup import RELEASE_PREFIX, corpus_inputs, download, normalize_paro_ddl, paro_ddl_inputs, verify_downloads, write_json
from job_compare import answer_rows, measurement_lock, query_paths, validate_import_receipt, validate_result
from tpcds_result_contract import ResultContractError, canonicalize_rows, duckdb_schema, paro_schema
from tpcds_compare import DuckDBProcess


class JobCorpusTests(unittest.TestCase):
    def test_import_receipt_binds_content_across_relocated_seed_and_different_binary(self):
        tables = {"small": {"rows": 1, "csv_sha256": "csv", "csv_bytes": 17},
                  "large": {"rows": 20, "csv_sha256": "csv2", "csv_bytes": 30}}
        manifest = {"export": {"tables": tables}}
        ddl = {"paro_ddl_sha256": "ddl", "tables": ["small", "large"],
               "normalization_count": 2}
        receipt = {"status": "completed", "dataset_manifest_sha256": "manifest",
                   "seed": {"path": "/historical/location", "sha256": "seed"},
                   "binary_sha256": "historical-importer-binary",
                   "expected_input_tables": copy.deepcopy(tables),
                   "verified_row_counts": {"small": 1, "large": 20},
                   "paro_ddl": copy.deepcopy(ddl)}
        validate_import_receipt(receipt, manifest, "manifest", "seed", ddl)
        bad = []
        for field, value in (("status", "running"), ("dataset_manifest_sha256", "other")):
            changed = copy.deepcopy(receipt)
            changed[field] = value
            bad.append(changed)
        changed = copy.deepcopy(receipt)
        changed["seed"]["sha256"] = "same-counts-different-content"
        bad.append(changed)
        for field in ("expected_input_tables", "verified_row_counts"):
            changed = copy.deepcopy(receipt)
            del changed[field]["large"]
            bad.append(changed)
        for field, value in (("rows", True), ("csv_sha256", "other"), ("csv_bytes", 18)):
            changed = copy.deepcopy(receipt)
            changed["expected_input_tables"]["small"][field] = value
            bad.append(changed)
        changed = copy.deepcopy(receipt)
        changed["verified_row_counts"]["small"] = True
        bad.append(changed)
        for field, value in (("paro_ddl_sha256", "other"), ("tables", ["large", "small"]),
                             ("normalization_count", 3)):
            changed = copy.deepcopy(receipt)
            changed["paro_ddl"][field] = value
            bad.append(changed)
        for changed in bad:
            with self.subTest(receipt=changed), self.assertRaises(ValueError):
                validate_import_receipt(changed, manifest, "manifest", "seed", ddl)

    def test_worker_timeout_closes_before_late_response_can_be_reused(self):
        worker = object.__new__(DuckDBProcess)
        worker._parent = Mock()
        worker._parent.poll.return_value = False
        worker.close = Mock()
        with self.assertRaises(TimeoutError):
            worker.execute("SELECT expensive", timeout_seconds=0.1)
        worker.close.assert_called_once()
        worker._parent.recv.assert_not_called()

    def test_invalid_worker_timeout_does_not_dispatch_a_query(self):
        worker = object.__new__(DuckDBProcess)
        worker._parent = Mock()
        with self.assertRaises(ValueError):
            worker.execute("SELECT 1", timeout_seconds=0)
        worker._parent.send.assert_not_called()

    def fixture(self, root):
        init = root / "benchmark/imdb_plan_cost/init"
        init.mkdir(parents=True)
        tables = [f"table_{letter}" for letter in "abcdefghijklmnopqrstu"]
        (init / "schema.sql").write_text("\n".join(f"CREATE TABLE {table} (id integer NOT NULL, note varchar);" for table in tables))
        (init / "load.sql").write_text("\n".join(f"INSERT INTO {table} SELECT * FROM '{RELEASE_PREFIX}job_{table}.parquet';" for table in tables))
        return tables

    def test_schema_and_load_must_cover_same_pinned_official_tables(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            tables = self.fixture(root)
            schema, urls = corpus_inputs(root)
            self.assertEqual([table for table, _ in schema], tables)
            self.assertEqual(set(urls), set(tables))
            path = root / "benchmark/imdb_plan_cost/init/load.sql"
            path.write_text(path.read_text().replace(RELEASE_PREFIX, "https://unrelated.invalid/"))
            with self.assertRaisesRegex(ValueError, "unrecognized JOB source"):
                corpus_inputs(root)

    def test_paro_alias_normalization_preserves_official_widths_nullability_and_other_types(self):
        # Plain column patterns from DuckDB imdb_plan_cost/init/schema.sql:
        # aka_name, movie_info, person_info, and title.
        official = """CREATE TABLE aka_name (
    id integer NOT NULL,
    person_id integer NOT NULL,
    name character varying(218) NOT NULL,
    imdb_index character varying(12),
    md5sum character varying(32),
    info character varying(8000) NOT NULL,
    note character varying(1),
    series_years character varying(49),
    person_info text NOT NULL
)"""
        transformed, count = normalize_paro_ddl(official)
        self.assertEqual(count, 6)
        self.assertEqual(transformed, official.replace("character varying", "varchar"))
        self.assertEqual(normalize_paro_ddl(transformed), (transformed, 0))
        untouched = """CREATE TABLE patterns (
    id integer NOT NULL,
    fixed character(12),
    unbounded text,
    existing varchar(218),
    note text DEFAULT 'character varying(12)',
    other text /* character varying(12) */
)"""
        self.assertEqual(normalize_paro_ddl(untouched), (untouched, 0))

    def test_paro_ddl_receipt_binds_transformation_without_rewriting_official_schema(self):
        with tempfile.TemporaryDirectory() as temporary:
            checkout = Path(temporary)
            tables = self.fixture(checkout)
            path = checkout / "benchmark/imdb_plan_cost/init/schema.sql"
            path.write_text(path.read_text().replace("note varchar", "note character varying(218)"))
            original_bytes = path.read_bytes()
            original, _ = corpus_inputs(checkout)
            statements, receipt = paro_ddl_inputs(checkout)
            self.assertEqual([table for table, _ in statements], tables)
            self.assertEqual(receipt["tables"], tables)
            self.assertEqual(receipt["normalization_count"], 21)
            self.assertIn("character varying(width) -> varchar(width)", receipt["normalization"])
            self.assertEqual(receipt["source_schema_sha256"], hashlib.sha256(original_bytes).hexdigest())
            serialized = ";\n\n".join(statement for _, statement in statements) + ";\n"
            self.assertEqual(receipt["paro_ddl_sha256"], hashlib.sha256(serialized.encode()).hexdigest())
            self.assertEqual(path.read_bytes(), original_bytes)
            self.assertEqual(corpus_inputs(checkout)[0], original)
            for (_, official), (_, paro) in zip(original, statements):
                self.assertEqual(paro, official.replace("character varying", "varchar"))
            # Width changes have a different transformed identity as well.
            path.write_text(path.read_text().replace("varying(218)", "varying(219)"))
            self.assertNotEqual(paro_ddl_inputs(checkout)[1]["paro_ddl_sha256"], receipt["paro_ddl_sha256"])

    def test_download_records_hashes_and_detects_drift_on_resume(self):
        with tempfile.TemporaryDirectory() as temporary:
            checkout = Path(temporary) / "source"
            tables = self.fixture(checkout)
            root = Path(temporary) / "data"
            body = b"fake parquet payload"
            assets = [{"name": f"job_{table}.parquet", "id": idx,
                       "size": len(body), "browser_download_url": f"{RELEASE_PREFIX}job_{table}.parquet"}
                      for idx, table in enumerate(tables)]
            release = {"assets": assets, "html_url": "official release", "id": 1, "tag_name": "v1.0"}
            responses = lambda request, **kwargs: io.BytesIO(json.dumps(release).encode() if "api.github.com" in request.full_url else body)
            with patch("job_setup.input_identity", return_value={"revision": "fixed"}), patch("job_setup.urllib.request.urlopen", side_effect=responses):
                download(checkout, root)
                manifest = json.loads((root / "dataset.json").read_text())
                self.assertEqual(len(manifest["assets"]), 21)
                self.assertEqual(manifest["assets"][tables[0]]["sha256"], hashlib.sha256(body).hexdigest())
                verify_downloads(root, manifest)
                # A later API response may begin advertising the same digest.
                for asset in assets:
                    asset["digest"] = "sha256:" + hashlib.sha256(body).hexdigest()
                download(checkout, root)
                verify_downloads(root, json.loads((root / "dataset.json").read_text()))
                (root / "parquet" / assets[0]["name"]).write_bytes(b"changed")
                with self.assertRaisesRegex(ValueError, "changed existing input"):
                    download(checkout, root)

    def test_resume_rejects_remote_drift_without_mutating_retained_prefix(self):
        with tempfile.TemporaryDirectory() as temporary:
            checkout = Path(temporary) / "source"
            tables = self.fixture(checkout)
            root = Path(temporary) / "data"
            (root / "parquet").mkdir(parents=True)
            body = b"retained parquet payload"
            digest = hashlib.sha256(body).hexdigest()
            assets = [{"name": f"job_{table}.parquet", "id": idx, "size": len(body),
                       "browser_download_url": f"{RELEASE_PREFIX}job_{table}.parquet"}
                      for idx, table in enumerate(tables)]
            release = {"assets": assets, "html_url": "official release", "id": 1, "tag_name": "v1.0"}
            table = tables[0]
            retained = root / "parquet" / assets[0]["name"]
            retained.write_bytes(body)
            manifest_path = root / "dataset.json"
            manifest_path.write_text(json.dumps({
                "schema_version": 1, "corpus": "JOB", "source": {"revision": "fixed"},
                "release": {"url": "official release", "id": 1, "tag": "v1.0"},
                "assets": {table: {"name": assets[0]["name"], "url": assets[0]["browser_download_url"],
                                   "release_asset_id": 0, "size": len(body), "sha256": digest}},
            }))
            before = manifest_path.read_bytes()
            for category, field, value in (
                ("release", "id", 2), ("release", "tag_name", "v1.1"),
                ("release", "html_url", "different release"),
                ("asset", "id", 100), ("asset", "size", len(body) + 1),
                ("asset", "browser_download_url", "https://unrelated.invalid/replaced.parquet"),
                ("asset", "digest", "sha256:" + "0" * 64),
                ("asset", "name", "removed_or_renamed.parquet"),
            ):
                changed = copy.deepcopy(release)
                target = changed if category == "release" else changed["assets"][0]
                target[field] = value
                with self.subTest(category=category, field=field), \
                     patch("job_setup.input_identity", return_value={"revision": "fixed"}), \
                     patch("job_setup.urllib.request.urlopen", return_value=io.BytesIO(json.dumps(changed).encode())) as request:
                    with self.assertRaisesRegex(ValueError, "drift.*new data root"):
                        download(checkout, root)
                    request.assert_called_once()  # Only the API, never a new table.
                    self.assertEqual(manifest_path.read_bytes(), before)
                    self.assertEqual(retained.read_bytes(), body)
                    self.assertEqual(list((root / "parquet").iterdir()), [retained])

    def test_interrupted_download_removes_its_zero_byte_partial(self):
        class Interrupted(io.BytesIO):
            def read(self, *_):
                raise KeyboardInterrupt()
        with tempfile.TemporaryDirectory() as temporary:
            checkout = Path(temporary) / "source"
            tables = self.fixture(checkout)
            root = Path(temporary) / "data"
            assets = [{"name": f"job_{table}.parquet", "id": idx, "size": 1,
                       "browser_download_url": f"{RELEASE_PREFIX}job_{table}.parquet"}
                      for idx, table in enumerate(tables)]
            release = {"assets": assets, "html_url": "release", "id": 1, "tag_name": "v1.0"}
            responses = lambda request, **kwargs: io.BytesIO(json.dumps(release).encode()) if "api.github.com" in request.full_url else Interrupted()
            with patch("job_setup.input_identity", return_value={}), patch("job_setup.urllib.request.urlopen", side_effect=responses):
                with self.assertRaises(KeyboardInterrupt):
                    download(checkout, root)
            self.assertEqual(list((root / "parquet").iterdir()), [])

    def test_validated_download_recovers_partial_or_published_file_without_refetching(self):
        for boundary in ("before_rename", "after_rename", "after_manifest_commit"):
            with self.subTest(boundary=boundary), tempfile.TemporaryDirectory() as temporary:
                checkout = Path(temporary) / "source"
                tables = self.fixture(checkout)
                root = Path(temporary) / "data"
                body = b"fully validated parquet"
                digest = hashlib.sha256(body).hexdigest()
                assets = [{"name": f"job_{table}.parquet", "id": idx, "size": len(body),
                           "digest": "sha256:" + digest,
                           "browser_download_url": f"{RELEASE_PREFIX}job_{table}.parquet"}
                          for idx, table in enumerate(tables)]
                release = {"assets": assets, "html_url": "release", "id": 1, "tag_name": "v1.0"}
                fetched = []

                def response(request, **_):
                    if "api.github.com" in request.full_url:
                        return io.BytesIO(json.dumps(release).encode())
                    fetched.append(request.full_url)
                    return io.BytesIO(body)

                original_replace = Path.replace

                def interrupt_rename(path, target):
                    if boundary == "before_rename" and path.name.endswith(".part"):
                        raise KeyboardInterrupt("validated checkpoint precedes rename")
                    return original_replace(path, target)

                def interrupt_manifest(path, value):
                    if tables[0] in value["assets"] and "pending_download" not in value:
                        if boundary == "after_manifest_commit":
                            write_json(path, value)
                        raise OSError("injected manifest checkpoint interruption")
                    write_json(path, value)

                with patch("job_setup.input_identity", return_value={}), \
                     patch("job_setup.urllib.request.urlopen", side_effect=response):
                    with patch.object(Path, "replace", interrupt_rename), \
                         patch("job_setup.write_json", side_effect=interrupt_manifest):
                        with self.assertRaises((KeyboardInterrupt, OSError)):
                            download(checkout, root)
                    manifest = json.loads((root / "dataset.json").read_text())
                    if boundary == "after_manifest_commit":
                        self.assertNotIn("pending_download", manifest)
                        self.assertIn(tables[0], manifest["assets"])
                    else:
                        self.assertEqual(manifest["pending_download"]["state"], "validated")
                        with self.assertRaisesRegex(ValueError, "uncommitted pending"):
                            verify_downloads(root, manifest)
                        partial = root / "parquet" / manifest["pending_download"]["partial"]
                        published = root / "parquet" / assets[0]["name"]
                        self.assertEqual(partial.exists(), boundary == "before_rename")
                        self.assertEqual(published.exists(), boundary == "after_rename")
                    download(checkout, root)
                manifest = json.loads((root / "dataset.json").read_text())
                self.assertNotIn("pending_download", manifest)
                verify_downloads(root, manifest)
                self.assertEqual(fetched.count(assets[0]["browser_download_url"]), 1)
                self.assertEqual(len(fetched), 21)
                self.assertEqual(len(list((root / "parquet").iterdir())), 21)

    def test_pending_resume_rejects_remote_drift_and_changed_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            checkout = Path(temporary) / "source"
            tables = self.fixture(checkout)
            root = Path(temporary) / "data"
            body = b"fully validated parquet"
            digest = hashlib.sha256(body).hexdigest()
            assets = [{"name": f"job_{table}.parquet", "id": idx, "size": len(body),
                       "digest": "sha256:" + digest,
                       "browser_download_url": f"{RELEASE_PREFIX}job_{table}.parquet"}
                      for idx, table in enumerate(tables)]
            release = {"assets": assets, "html_url": "release", "id": 1, "tag_name": "v1.0"}
            original_replace = Path.replace

            def interrupt_rename(path, target):
                if path.name.endswith(".part"):
                    raise KeyboardInterrupt()
                return original_replace(path, target)

            response = lambda request, **kwargs: io.BytesIO(
                json.dumps(release).encode() if "api.github.com" in request.full_url else body)
            with patch("job_setup.input_identity", return_value={}), \
                 patch("job_setup.urllib.request.urlopen", side_effect=response), \
                 patch.object(Path, "replace", interrupt_rename):
                with self.assertRaises(KeyboardInterrupt):
                    download(checkout, root)
            manifest_path = root / "dataset.json"
            before = manifest_path.read_bytes()
            manifest = json.loads(before)
            partial = root / "parquet" / manifest["pending_download"]["partial"]
            for field, value in (("id", 100), ("size", len(body) + 1),
                                 ("digest", "sha256:" + "0" * 64),
                                 ("browser_download_url", "https://unrelated.invalid/replaced.parquet"),
                                 ("name", "removed_or_renamed.parquet")):
                changed = copy.deepcopy(release)
                changed["assets"][0][field] = value
                with self.subTest(field=field), patch("job_setup.input_identity", return_value={}), \
                     patch("job_setup.urllib.request.urlopen", return_value=io.BytesIO(json.dumps(changed).encode())) as request:
                    with self.assertRaisesRegex(ValueError, "drift.*new data root"):
                        download(checkout, root)
                    request.assert_called_once()
                    self.assertEqual(manifest_path.read_bytes(), before)
                    self.assertEqual(partial.read_bytes(), body)
            partial.write_bytes(b"changed")
            with patch("job_setup.input_identity", return_value={}), \
                 patch("job_setup.urllib.request.urlopen", side_effect=response):
                with self.assertRaisesRegex(ValueError, "changed validated JOB partial"):
                    download(checkout, root)
            self.assertEqual(partial.read_bytes(), b"changed")
            self.assertEqual(json.loads(manifest_path.read_text())["pending_download"], manifest["pending_download"])

    def test_unvalidated_journal_cannot_adopt_an_existing_published_file(self):
        with tempfile.TemporaryDirectory() as temporary:
            checkout = Path(temporary) / "source"
            tables = self.fixture(checkout)
            root = Path(temporary) / "data"
            body = b"unvalidated parquet"
            assets = [{"name": f"job_{table}.parquet", "id": idx, "size": len(body),
                       "browser_download_url": f"{RELEASE_PREFIX}job_{table}.parquet"}
                      for idx, table in enumerate(tables)]
            release = {"assets": assets, "html_url": "release", "id": 1, "tag_name": "v1.0"}
            response = lambda request, **kwargs: io.BytesIO(
                json.dumps(release).encode() if "api.github.com" in request.full_url else body)

            def fail_validated_checkpoint(path, value):
                if value.get("pending_download", {}).get("state") == "validated":
                    raise OSError("failed validated checkpoint")
                write_json(path, value)

            with patch("job_setup.input_identity", return_value={}), \
                 patch("job_setup.urllib.request.urlopen", side_effect=response), \
                 patch("job_setup.write_json", side_effect=fail_validated_checkpoint):
                with self.assertRaisesRegex(OSError, "failed validated checkpoint"):
                    download(checkout, root)
            manifest = json.loads((root / "dataset.json").read_text())
            self.assertEqual(manifest["pending_download"]["state"], "downloading")
            published = root / "parquet" / assets[0]["name"]
            published.write_bytes(body)
            with patch("job_setup.input_identity", return_value={}), \
                 patch("job_setup.urllib.request.urlopen", side_effect=response):
                with self.assertRaisesRegex(ValueError, "unowned or changed existing input"):
                    download(checkout, root)
            self.assertEqual(published.read_bytes(), body)

    def test_complete_partial_without_validated_checkpoint_is_redownloaded(self):
        with tempfile.TemporaryDirectory() as temporary:
            checkout = Path(temporary) / "source"
            tables = self.fixture(checkout)
            root = Path(temporary) / "data"
            body = b"complete but not durably validated"
            assets = [{"name": f"job_{table}.parquet", "id": idx, "size": len(body),
                       "browser_download_url": f"{RELEASE_PREFIX}job_{table}.parquet"}
                      for idx, table in enumerate(tables)]
            release = {"assets": assets, "html_url": "release", "id": 1, "tag_name": "v1.0"}
            fetched = []

            def response(request, **_):
                if "api.github.com" in request.full_url:
                    return io.BytesIO(json.dumps(release).encode())
                fetched.append(request.full_url)
                return io.BytesIO(body)

            def fail_validated_checkpoint(path, value):
                if value.get("pending_download", {}).get("state") == "validated":
                    raise OSError("failed validated checkpoint")
                write_json(path, value)

            with patch("job_setup.input_identity", return_value={}), \
                 patch("job_setup.urllib.request.urlopen", side_effect=response):
                with patch("job_setup.write_json", side_effect=fail_validated_checkpoint):
                    with self.assertRaises(OSError):
                        download(checkout, root)
                download(checkout, root)
            verify_downloads(root, json.loads((root / "dataset.json").read_text()))
            self.assertEqual(fetched.count(assets[0]["browser_download_url"]), 2)
            self.assertEqual(len(list((root / "parquet").iterdir())), 21)

    def test_exact_query_selection_and_measurement_lock_preserve_owner(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            queries = root / "benchmark/imdb_plan_cost/queries"
            queries.mkdir(parents=True)
            for name in ["01a", "01b", "10a"]:
                (queries / f"{name}.sql").write_text("SELECT 1")
            self.assertEqual([p.stem for p in query_paths(root, "01a,10a")], ["01a", "10a"])
            for selection in ["1", "01", "01a,01a", "../01a"]:
                with self.assertRaises(ValueError):
                    query_paths(root, selection)
            lock = root / "lock"
            with measurement_lock(lock):
                with self.assertRaises(FileExistsError):
                    with measurement_lock(lock):
                        self.fail("occupied lock must not be acquired")
                self.assertTrue((lock / "owner").exists())
            self.assertFalse(lock.exists())

    def test_authoritative_answers_keep_large_integers_nulls_and_bag_counts(self):
        schema = duckdb_schema([("v", "BIGINT"), ("s", "VARCHAR")])
        wire_schema = paro_schema([
            SimpleNamespace(name="v", type_code=20), SimpleNamespace(name="s", type_code=1043)])
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "answer.csv"
            path.write_text('v|s\n9007199254740993|NULL\n9007199254740993|"quoted|pipe"\n')
            self.assertEqual(answer_rows(path, schema), canonicalize_rows(
                [(9007199254740993, None), (9007199254740993, "quoted|pipe")], schema))
        query = "SELECT 1::BIGINT AS v, 'x'::VARCHAR AS s"
        expected = canonicalize_rows([(1, "x")], schema)
        validate_result(query, [(1, "x")], wire_schema, schema, expected)
        validate_result(query, [(1, "x")], schema, schema, expected, engine="duckdb")
        with self.assertRaises(ResultContractError):
            validate_result(query, [(1, "x")], duckdb_schema([("v", "INTEGER"), ("s", "VARCHAR")]),
                            schema, expected, engine="duckdb")
        with self.assertRaises(ResultContractError):
            validate_result(query, [(1, "x"), (1, "x")], schema, schema, expected, engine="duckdb")
        with self.assertRaises(ResultContractError):
            validate_result(query, [(1, "x"), (1, "x")], wire_schema, schema, expected)


if __name__ == "__main__":
    unittest.main()
