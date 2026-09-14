from pathlib import Path
from types import SimpleNamespace
import argparse
import json
import subprocess
import sys

sys.path.insert(0, "/private/tmp/paro-page-read-20260915/benchmark/corpora")
from benchmark_evidence import ImmutableDataSeed, content_digest, isolated_paro_server
from tpcds_compare import open_paro_connection, timed_run_paro
from tpcds_result_contract import canonicalize_rows, multiset_digest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    output = Path(args.output)
    root = Path("/private/tmp/paro-page-read-20260915")
    query = (root / "benchmark/evidence/first-statement/q11/20260914/memo-contract-v1/11.sql").read_text()
    seed = ImmutableDataSeed.capture(Path("/private/tmp/paro-memo-seed-NvaAEm/data"))
    connection_args = SimpleNamespace(
        listen="127.0.0.1:16663",
        database="postgres",
        user="paro",
        threads=4,
        memory_limit="2GB",
        statement_timeout_seconds=300,
    )
    binary = root / "target/release/parod"
    log_path = output.with_suffix(".parod.log")
    sample_path = output.with_suffix(".sample")
    with isolated_paro_server(
        binary,
        seed,
        connection_args.listen,
        log_path,
        max_memory=connection_args.memory_limit,
        threads=connection_args.threads,
        statement_trace=False,
        cache_evidence=True,
        optimizer_environment={"PARO_QUALITY_POLICY_HANDOFF": "1"},
    ) as server:
        with open_paro_connection(connection_args) as connection:
            sampler = subprocess.Popen(
                ["sample", str(server.identity()["pid"]), "1", "1", "-file", str(sample_path)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            rows, schema, elapsed_ms = timed_run_paro(connection, query, True)
            sampler.wait(timeout=15)
    print(
        json.dumps(
            {
                "diagnostic_only": True,
                "binary_sha256": content_digest(binary),
                "client_ms_with_sampler": elapsed_ms,
                "rows": len(rows),
                "typed_digest": multiset_digest(canonicalize_rows(rows, schema)),
                "sampler_exit": sampler.returncode,
                "sample_path": str(sample_path),
            }
        )
    )


if __name__ == "__main__":
    main()
