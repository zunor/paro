#!/usr/bin/env python3
# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Download DuckDB's JOB inputs, export a local oracle/CSV, or load Paro.

Each phase is explicit: download never imports a database or starts a server.
Generated data belongs outside source repositories. Run from benchmark with
PYTHONPATH=.:corpora and the declared .venv interpreter.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
import urllib.request
import uuid
from pathlib import Path

from benchmark_evidence import content_digest, repository_identity, tree_digest

RELEASE_API = "https://api.github.com/repos/duckdb/duckdb-data/releases/tags/v1.0"
RELEASE_PREFIX = "https://github.com/duckdb/duckdb-data/releases/download/v1.0/"


def sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def write_json(path: Path, value: dict) -> None:
    # The pending journal must survive process interruption before publishing a
    # downloaded file. Sync both the file and the atomic rename's directory.
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                     prefix=path.name + ".", suffix=".tmp", delete=False) as output:
        temporary = Path(output.name)
        try:
            output.write(json.dumps(value, indent=2) + "\n")
            output.flush()
            os.fsync(output.fileno())
        except BaseException:
            temporary.unlink()
            raise
    try:
        temporary.replace(path)
        sync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def corpus_inputs(checkout: Path) -> tuple[list[tuple[str, str]], dict[str, str]]:
    init = checkout / "benchmark/imdb_plan_cost/init"
    statements = []
    for raw in (init / "schema.sql").read_text(encoding="utf-8").split(";"):
        statement = raw.strip()
        if not statement:
            continue
        match = re.fullmatch(r"(?is)CREATE\s+TABLE\s+([a-z_]+)\s*\(.*\)", statement)
        if not match:
            raise ValueError(f"unsupported JOB DDL: {statement[:80]}")
        statements.append((match[1], statement))
    loads = {}
    for raw in (init / "load.sql").read_text(encoding="utf-8").split(";"):
        if not raw.strip():
            continue
        match = re.fullmatch(r"(?is)\s*INSERT INTO ([a-z_]+) SELECT \* FROM '([^']+)'\s*", raw)
        if not match:
            raise ValueError(f"unsupported JOB load: {raw[:80]}")
        table, url = match.groups()
        if url != f"{RELEASE_PREFIX}job_{table}.parquet" or table in loads:
            raise ValueError(f"unrecognized JOB source for {table}: {url}")
        loads[table] = url
    if len(statements) != 21 or len(loads) != 21 or {t for t, _ in statements} != set(loads):
        raise ValueError("JOB requires the same complete 21-table schema and input set")
    return statements, loads


def normalize_paro_ddl(statement: str) -> tuple[str, int]:
    """Normalize only JOB's plain CHARACTER VARYING(width) column alias.

    This is a compatibility adapter for the pinned corpus, not a SQL rewriter.
    Match a complete unquoted column declaration so strings, comments, defaults,
    other types, widths, and nullability cannot be accidentally rewritten.
    """
    table = re.fullmatch(r"(?is)(CREATE\s+TABLE\s+[a-z_]+\s*\()(.*)(\))", statement)
    if table is None:
        raise ValueError("unsupported JOB CREATE TABLE for Paro normalization")
    alias = re.compile(r"(?is)(\s*[a-z_][a-z_0-9]*\s+)character\s+varying"
                       r"(\s*\(\s*[0-9]+\s*\)(?:\s+NOT\s+NULL)?\s*)")
    columns = []
    replacements = 0
    for column in table[2].split(","):
        match = alias.fullmatch(column)
        if match:
            column = match[1] + "varchar" + match[2]
            replacements += 1
        columns.append(column)
    return table[1] + ",".join(columns) + table[3], replacements


def paro_ddl_inputs(checkout: Path) -> tuple[list[tuple[str, str]], dict]:
    """Return the exact Paro CREATE statements and their import receipt fields."""
    official, _ = corpus_inputs(checkout)
    statements = []
    replacements = 0
    for table, statement in official:
        normalized, count = normalize_paro_ddl(statement)
        statements.append((table, normalized))
        replacements += count
    serialized = ";\n\n".join(statement for _, statement in statements) + ";\n"
    receipt = {
        "source_schema_sha256": content_digest(checkout / "benchmark/imdb_plan_cost/init/schema.sql"),
        "paro_ddl_sha256": hashlib.sha256(serialized.encode("utf-8")).hexdigest(),
        "normalization": "Paro CREATE TABLE plain column type alias character varying(width) -> varchar(width); "
                         "preserve width, nullability, and all remaining DDL",
        "normalization_count": replacements,
        "ddl_serialization": "UTF-8 statements in official table order, joined with ;\\n\\n and trailing ;\\n",
        "tables": [table for table, _ in statements],
    }
    return statements, receipt


def input_identity(checkout: Path) -> dict:
    return {
        "duckdb_source": repository_identity(checkout),
        "schema_sha256": content_digest(checkout / "benchmark/imdb_plan_cost/init/schema.sql"),
        "load_sha256": content_digest(checkout / "benchmark/imdb_plan_cost/init/load.sql"),
        "queries_sha256": tree_digest(checkout / "benchmark/imdb_plan_cost/queries", (".sql",)),
        "answers_sha256": tree_digest(checkout / "benchmark/imdb/answers", (".csv",)),
    }


def read_manifest(root: Path) -> dict:
    manifest = json.loads((root / "dataset.json").read_text(encoding="utf-8"))
    if manifest.get("corpus") != "JOB" or manifest.get("schema_version") != 1:
        raise ValueError("unsupported JOB dataset manifest")
    return manifest


def verify_downloads(root: Path, manifest: dict) -> None:
    if "pending_download" in manifest:
        raise ValueError("JOB download has an uncommitted pending asset; resume download first")
    assets = manifest.get("assets", {})
    if len(assets) != 21:
        raise ValueError("JOB download is incomplete")
    for table, record in assets.items():
        if record["name"] != f"job_{table}.parquet" or record["url"] != f"{RELEASE_PREFIX}job_{table}.parquet":
            raise ValueError(f"unexpected JOB asset identity: {table}")
        path = root / "parquet" / record["name"]
        if path.stat().st_size != record["size"] or content_digest(path) != record["sha256"]:
            raise ValueError(f"JOB input drift: {path}")


def verify_release_record(table: str, record: dict, loads: dict, released: dict, *, validated: bool) -> None:
    name = f"job_{table}.parquet"
    asset = released.get(name)
    digest = record.get("sha256")
    if (table not in loads or record.get("name") != name or record.get("url") != loads[table]
            or asset is None or asset["browser_download_url"] != record["url"]
            or asset["id"] != record.get("release_asset_id") or asset["size"] != record.get("size")
            or (validated and (not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest)))
            or (validated and asset.get("digest") and asset["digest"] != f"sha256:{digest}")):
        raise ValueError(f"JOB release asset drift for {table}; select a new data root")


def verify_pending(pending: dict, manifest: dict, loads: dict, released: dict) -> None:
    if not isinstance(pending, dict) or pending.get("state") not in {"downloading", "validated"}:
        raise ValueError("invalid JOB pending-download journal")
    table = pending.get("table")
    if not isinstance(table, str) or table not in loads:
        raise ValueError("invalid JOB pending-download table")
    partial = pending.get("partial")
    if not isinstance(partial, str) or not re.fullmatch(rf"job_{table}\.parquet\.[0-9a-f]{{32}}\.part", partial):
        raise ValueError("invalid JOB owned partial filename")
    record = pending.get("asset")
    if not isinstance(record, dict):
        raise ValueError("invalid JOB pending-download asset")
    verify_release_record(table, record, loads, released, validated=pending["state"] == "validated")
    old = manifest["assets"].get(table)
    if old and any(record.get(key) != old[key] for key in record):
        raise ValueError(f"JOB pending asset differs from retained input: {table}")


def path_exists(path: Path) -> bool:
    return path.exists() or path.is_symlink()


def matches_record(path: Path, record: dict) -> bool:
    return (not path.is_symlink() and path.is_file() and path.stat().st_size == record["size"]
            and content_digest(path) == record["sha256"])


def recover_pending(root: Path, manifest: dict) -> None:
    pending = manifest.get("pending_download")
    if pending is None:
        return
    record = pending["asset"]
    partial = root / "parquet" / pending["partial"]
    path = root / "parquet" / record["name"]
    if pending["state"] == "downloading":
        # Only a completed, hashed checkpoint can own a published filename.
        if path_exists(path):
            raise ValueError(f"unowned or changed existing input: {path}")
        if path_exists(partial):
            if partial.is_symlink() or not partial.is_file():
                raise ValueError(f"invalid JOB owned partial: {partial}")
            partial.unlink()
            sync_directory(partial.parent)
        del manifest["pending_download"]
        write_json(root / "dataset.json", manifest)
        return
    if path_exists(path):
        if path_exists(partial) or not matches_record(path, record):
            raise ValueError(f"unowned or changed pending input: {path}")
    else:
        if not matches_record(partial, record):
            raise ValueError(f"missing or changed validated JOB partial: {partial}")
        partial.replace(path)
        sync_directory(path.parent)
    manifest["assets"][pending["table"]] = record
    del manifest["pending_download"]
    write_json(root / "dataset.json", manifest)


def download(checkout: Path, root: Path) -> None:
    _, loads = corpus_inputs(checkout)
    identity = input_identity(checkout)
    if root.exists() and any(root.iterdir()) and not (root / "dataset.json").is_file():
        raise ValueError("nonempty data root lacks this tool's ownership manifest")
    root.mkdir(parents=True, exist_ok=True)
    manifest = read_manifest(root) if (root / "dataset.json").exists() else {
        "schema_version": 1, "corpus": "JOB", "source": identity, "assets": {},
    }
    if manifest["source"] != identity:
        raise ValueError("JOB corpus source changed; select a new data root")
    request = urllib.request.Request(RELEASE_API, headers={"User-Agent": "Paro-JOB-collector"})
    with urllib.request.urlopen(request, timeout=60) as response:
        release = json.load(response)
    released = {a["name"]: a for a in release["assets"]}
    release_identity = {"url": release["html_url"], "id": release["id"], "tag": release["tag_name"]}
    if (("release" in manifest and manifest["release"] != release_identity)
            or ((manifest["assets"] or "pending_download" in manifest) and "release" not in manifest)):
        raise ValueError("JOB release identity drift; select a new data root")
    # Verify the entire retained prefix before writing a refreshed manifest or
    # downloading the suffix. A mutable release tag must not mix old/new tables.
    for table, old in manifest["assets"].items():
        verify_release_record(table, old, loads, released, validated=True)
    if "pending_download" in manifest:
        verify_pending(manifest["pending_download"], manifest, loads, released)
    manifest["release"] = release_identity
    write_json(root / "dataset.json", manifest)
    (root / "parquet").mkdir(exist_ok=True)
    sync_directory(root)
    recover_pending(root, manifest)
    for table, url in loads.items():
        name = f"job_{table}.parquet"
        asset = released[name]
        if asset["browser_download_url"] != url:
            raise ValueError(f"release/source URL mismatch: {name}")
        path = root / "parquet" / name
        if path_exists(path):
            old = manifest["assets"].get(table)
            if not old or not matches_record(path, old):
                raise ValueError(f"unowned or changed existing input: {path}")
            continue
        partial = path.with_name(name + "." + uuid.uuid4().hex + ".part")
        record = {"name": name, "url": url, "release_asset_id": asset["id"], "size": asset["size"]}
        manifest["pending_download"] = {"table": table, "state": "downloading",
                                        "partial": partial.name, "asset": record}
        write_json(root / "dataset.json", manifest)
        digest = hashlib.sha256()
        size = 0
        created_partial = False
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "Paro-JOB-collector"})
            with urllib.request.urlopen(request, timeout=120) as response:
                with partial.open("xb") as output:
                    created_partial = True
                    while block := response.read(1024 * 1024):
                        output.write(block)
                        digest.update(block)
                        size += len(block)
                    output.flush()
                    os.fsync(output.fileno())
            if size != asset["size"]:
                raise ValueError(f"release size mismatch for {name}: {size} != {asset['size']}")
            advertised = asset.get("digest")
            if advertised and advertised != f"sha256:{digest.hexdigest()}":
                raise ValueError(f"release digest mismatch for {name}")
            old = manifest["assets"].get(table)
            if old and old["sha256"] != digest.hexdigest():
                raise ValueError(f"JOB retained input digest drift for {name}")
            sync_directory(partial.parent)
        except BaseException:
            # Only remove the partial created by this invocation.
            if created_partial and partial.exists():
                partial.unlink()
                sync_directory(partial.parent)
            raise
        record["sha256"] = digest.hexdigest()
        manifest["pending_download"]["state"] = "validated"
        write_json(root / "dataset.json", manifest)
        recover_pending(root, manifest)
        print(f"downloaded {table}: {size} bytes {digest.hexdigest()}", flush=True)


def literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def export(checkout: Path, root: Path, threads: int, memory: str) -> None:
    import duckdb

    manifest = read_manifest(root)
    if manifest["source"] != input_identity(checkout):
        raise ValueError("JOB corpus source differs from downloaded inputs")
    verify_downloads(root, manifest)
    database = root / "job.duckdb"
    csv_root = root / "csv"
    if database.exists() or csv_root.exists():
        raise ValueError("oracle/CSV outputs already exist; use a new owned export directory")
    csv_root.mkdir()
    statements, _ = corpus_inputs(checkout)
    exported = {}
    with duckdb.connect(str(database)) as connection:
        connection.execute(f"SET threads={threads}")
        connection.execute("SET memory_limit=?", [memory])
        for table, statement in statements:
            connection.execute(statement)
            parquet = root / "parquet" / f"job_{table}.parquet"
            connection.execute(f"INSERT INTO {table} SELECT * FROM read_parquet({literal(str(parquet))})")
            path = csv_root / f"{table}.csv"
            connection.execute(f"COPY {table} TO {literal(str(path))} (FORMAT CSV, HEADER false, NULL '\\N', FORCE_QUOTE *)")
            count = connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            exported[table] = {"rows": count, "csv_sha256": content_digest(path), "csv_bytes": path.stat().st_size}
            print(f"exported {table}: {count} rows", flush=True)
        connection.execute("ANALYZE")
        connection.execute("CHECKPOINT")
    manifest["export"] = {"metadata_track": "none", "duckdb_version": duckdb.__version__,
                           "database_sha256": content_digest(database), "tables": exported,
                           "csv_options": {"format": "csv", "header": False, "null": "\\N", "force_quote": "all_non_null"}}
    write_json(root / "dataset.json", manifest)


def load(checkout: Path, root: Path, dsn: str, threads: int, memory: str) -> None:
    import psycopg
    from psycopg import sql

    manifest = read_manifest(root)
    if manifest["source"] != input_identity(checkout) or "export" not in manifest:
        raise ValueError("JOB export/source manifest is missing or mismatched")
    statements, ddl_receipt = paro_ddl_inputs(checkout)
    print("Paro DDL receipt: " + json.dumps(ddl_receipt, sort_keys=True), flush=True)
    with psycopg.connect(dsn, autocommit=True) as connection:
        connection.execute(sql.SQL("SET threads={}").format(sql.Literal(threads)))
        connection.execute(sql.SQL("SET memory_limit={}").format(sql.Literal(memory)))
        for table, statement in statements:
            path = root / "csv" / f"{table}.csv"
            record = manifest["export"]["tables"][table]
            if content_digest(path) != record["csv_sha256"]:
                raise ValueError(f"JOB CSV drift: {path}")
            connection.execute(statement)
            connection.execute(sql.SQL("COPY {} FROM {} WITH (FORMAT csv, HEADER false, NULL {})").format(
                sql.Identifier(table), sql.Literal(str(path)), sql.Literal("\\N")))
            count = connection.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(table))).fetchone()[0]
            if count != record["rows"]:
                raise AssertionError(f"JOB row count mismatch: {table}: {count} != {record['rows']}")
            # Paro maintains storage column statistics during ingestion. Its
            # SQL ANALYZE TABLE statement is not implemented in this checkout.
            print(f"loaded/verified {table}: {count} rows", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb-checkout", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--memory-limit", default="2GB")
    phases = parser.add_subparsers(dest="phase", required=True)
    phases.add_parser("download", help="network and file writes only; no DB import")
    phases.add_parser("export", help="create DuckDB oracle and Paro-compatible CSV")
    phases.add_parser("load", help="load/verify an explicitly owned running Paro server").add_argument("--dsn", required=True)
    args = parser.parse_args()
    if args.threads < 1:
        parser.error("threads must be positive")
    root = args.data_root.resolve()
    checkout = args.duckdb_checkout.resolve()
    for source in [checkout, Path(__file__).resolve().parents[2]]:
        if root.is_relative_to(source):
            parser.error("generated JOB data must be outside source repositories")
    if args.phase == "download":
        download(checkout, root)
    elif args.phase == "export":
        export(checkout, root, args.threads, args.memory_limit)
    else:
        load(checkout, root, args.dsn, args.threads, args.memory_limit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
