#!/bin/zsh
set -eu
ulimit -n 65536
run_root=/private/tmp/paro-aggregate-ab.r2t5h9
source_root=$run_root/control-src
python=/Users/linjunhong/workspace/paro/benchmark/.venv/bin/python
export PYTHONPATH=benchmark
export CARGO_TARGET_DIR=/Users/linjunhong/workspace/paro/target
export PARO_COMPILE_WORK_EVIDENCE=1
export PARO_DIAGNOSTIC_SEARCH_STOP_MS=30000
cd "$source_root"
cp target/release/parod "$run_root/v2-probe-parod"

for batch in probe-1 control-1 control-2 probe-2; do
    case "$batch" in
        probe-1) arm=probe; revision=5b83c533; seed=2026092421; queries=(4 11 74) ;;
        control-1) arm=control; revision=e37ae93f; seed=2026092421; queries=(4 11 74) ;;
        control-2) arm=control; revision=e37ae93f; seed=2026092422; queries=(74 11 4) ;;
        probe-2) arm=probe; revision=5b83c533; seed=2026092422; queries=(74 11 4) ;;
    esac
    test -z "$(git status --porcelain)"
    if [[ "$batch" == control-1 || "$batch" == probe-2 ]]; then
        git switch --detach "$revision"
        cargo clean --release -p paro-optimizer -p paro-execution -p paro-server
        "$python" - <<'PY' > "$run_root/v2-$batch-prebuild.json"
from pathlib import Path
from corpora.benchmark_evidence import build_benchmark_server
import json
_, attestation = build_benchmark_server(Path.cwd(), 4)
print(json.dumps(attestation, indent=2))
PY
        if [[ "$batch" == control-1 ]]; then
            cp "$run_root/v2-$batch-prebuild.json" "$run_root/v2-control-build.json"
            cp target/release/parod "$run_root/v2-control-parod"
        fi
    fi
    "$python" - "$run_root/v2-$arm-build.json" "$revision" <<'PY'
from pathlib import Path
from corpora.benchmark_evidence import repository_identity, content_digest
import json, sys
expected = json.loads(Path(sys.argv[1]).read_text())
actual = repository_identity(Path.cwd())
assert actual == expected['source'] and actual['commit'].startswith(sys.argv[2])
assert content_digest(Path('target/release/parod')) == expected['binary_sha256']
other = Path(sys.argv[1]).with_name('v2-control-build.json')
if other.exists():
    probe = json.loads(other.with_name('v2-probe-build.json').read_text())
    assert json.loads(other.read_text())['binary_sha256'] != probe['binary_sha256']
print('preflight source and saved binary match:', actual['commit'][:8])
PY
    ps -axo pid,pcpu,pmem,etime,comm | sort -nr -k2 | head -16 > "$run_root/v2-$batch-host-before.txt"
    for query in "${queries[@]}"; do
        printf -v query_id '%02d' "$query"
        "$python" benchmark/corpora/tpcds_compare.py \
            --server-data-dir /private/tmp/paro-migration-relative.u1PLBV \
            --duckdb-database /Users/linjunhong/workspace/tpcds-sf1/tpcds-sf1.duckdb \
            --dataset-source-dir /Users/linjunhong/workspace/tpcds-sf1/csv \
            --query-dir /Users/linjunhong/workspace/duckdb/extension/tpcds/dsdgen/queries \
            --report "$run_root/v2-$batch-q$query_id.json" --start "$query" --end "$query" \
            --listen 127.0.0.1:16433 --process-blocks 3 --diagnostic-process-blocks 1 \
            --measurement-rounds-per-process 3 --warmups-per-process 1 \
            --bootstrap-samples 10000 --random-seed "$seed" --threads 4 --memory-limit 2GB \
            --optimizer-search-policy quality --optimizer-verify off \
            --metadata-track generator-declared --paro-result-format binary \
            --build-jobs 4 --statement-timeout-seconds 60 > "$run_root/v2-$batch-q$query_id.log" 2>&1
        "$python" - "$run_root/v2-$batch-q$query_id-run" "$run_root/v2-$arm-build.json" <<'PY'
import json, sys
from pathlib import Path
from corpora.benchmark_evidence import content_digest
from harness.receipt_contract import validate_campaign_summary, validate_benchmark_payload, validate_compile_document
root = Path(sys.argv[1])
expected = json.loads(Path(sys.argv[2]).read_text())
inputs = json.loads((root/'inputs.json').read_text())
assert inputs['source'] == expected['source']
assert inputs['build_attestation']['binary_sha256'] == expected['binary_sha256']
assert content_digest(Path('target/release/parod')) == expected['binary_sha256']
validate_campaign_summary(json.loads((root/'campaign.json').read_text()), json.loads((root/'manifest.json').read_text()))
for path in root.glob('sources/*/attempts/*/result.json'):
    payload = json.loads(path.read_text())
    normal = '-normal' in str(path)
    validate_benchmark_payload(payload, require_receipts=normal)
    if normal:
        receipts = payload['workloads'][0]['queries'][0]['compile_receipts']
        cold = [r['compile'] for r in receipts if r['compile']['cache_hit'] is False]
        assert len(cold) == 3
        assert all(c['receipt']['compile_work']['compiler_elapsed_us'] > 0 for c in cold)
for path in root.glob('sources/*/attempts/*/captures/*.json'):
    validate_compile_document(json.loads(path.read_text()))
assert sum(p.stat().st_size for p in root.rglob('*') if p.is_file()) < 1024*1024
print('validated', root.name, expected['binary_sha256'])
PY
    done
    ps -axo pid,pcpu,pmem,etime,comm | sort -nr -k2 | head -16 > "$run_root/v2-$batch-host-after.txt"
done
