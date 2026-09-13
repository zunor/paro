#!/usr/bin/env python3
"""Archive owned P1 reports/logs and their raw-byte hashes, without altering them."""
import gzip
import hashlib
import json
from pathlib import Path

root = Path(__file__).resolve().parent
out = root / 'raw'
out.mkdir(exist_ok=True)
items = []
for arm in ('off0', 'on0', 'off1', 'w1', 'w2', 'w4', 'w8', 'winf',
            'p2probe', 'p2control', 'phaseoff0', 'phaseon0', 'phaseoff1'):
    for path in sorted(Path('/private/tmp').glob(f'paro-p1-{arm}*')):
        if not path.is_file() or path.suffix not in ('.json', '.jsonl', '.log'):
            continue
        data = path.read_bytes()
        compressed = gzip.compress(data, mtime=0)
        target = out / (path.name + '.gz')
        if target.exists() and target.read_bytes() != compressed:
            raise RuntimeError(f'refusing to overwrite changed artifact {target}')
        target.write_bytes(compressed)
        items.append(dict(file=target.name, raw_sha256=hashlib.sha256(data).hexdigest(),
                          gzip_sha256=hashlib.sha256(compressed).hexdigest(), raw_bytes=len(data)))
# Commands and failures are preserved separately from performance artifacts.
for pattern in ('paro-p1-*-tests*.log', 'paro-p2-*-tests.log'):
    for path in sorted(Path('/private/tmp').glob(pattern)):
        data = path.read_bytes()
        compressed = gzip.compress(data, mtime=0)
        target = out / (path.name + '.gz')
        if target.exists() and target.read_bytes() != compressed:
            raise RuntimeError(f'refusing to overwrite changed artifact {target}')
        target.write_bytes(compressed)
        items.append(dict(file=target.name, raw_sha256=hashlib.sha256(data).hexdigest(),
                          gzip_sha256=hashlib.sha256(compressed).hexdigest(), raw_bytes=len(data)))
(out / 'manifest.json').write_text(json.dumps(items, indent=2) + '\n')
