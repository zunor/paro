#!/usr/bin/env python3
"""Archive only this experiment's owned immutable artifacts; retain slow blocks."""
import gzip
import hashlib
import json
from pathlib import Path
from analyze import analyze

root = Path(__file__).resolve().parent
out = root / 'raw'
out.mkdir(exist_ok=True)
manifest = []
for path in sorted(Path('/private/tmp').glob('paro-partition-*')):
    if not path.is_file() or path.suffix not in ('.json', '.jsonl', '.log'):
        continue
    data = path.read_bytes()
    zipped = gzip.compress(data, mtime=0)
    target = out / (path.name + '.gz')
    if target.exists() and target.read_bytes() != zipped:
        raise RuntimeError(f'changed artifact: {target}')
    target.write_bytes(zipped)
    manifest.append(dict(file=target.name, raw_bytes=len(data),
                         raw_sha256=hashlib.sha256(data).hexdigest(),
                         gzip_sha256=hashlib.sha256(zipped).hexdigest()))
(out / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
results = {}
for name in ('off0', 'on0', 'off1', 'v2-off0', 'v2-on0', 'v2-off1', 'probe-off0', 'probe-on0', 'probe-off1'):
    report = out / f'paro-partition-{name}.json.gz'
    ledger = out / f'paro-partition-{name}.ledger.jsonl.gz'
    if report.exists():
        results[name] = analyze(report, ledger if ledger.exists() else None)
(root / 'summary.json').write_text(json.dumps(results, indent=2) + '\n')
