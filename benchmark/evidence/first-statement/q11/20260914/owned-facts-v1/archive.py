#!/usr/bin/env python3
"""Archive this task's diagnostic/pilot/test artifacts, including negative runs."""
import gzip
import hashlib
import json
from pathlib import Path
from analyze import main

root = Path(__file__).resolve().parent
out = root / 'raw'
out.mkdir(exist_ok=True)
manifest = []
paths = sorted(p for phase in ('t0', 't1', 't2')
               for p in Path('/private/tmp').glob(f'paro-owned-facts-{phase}-*')
               if p.is_file() and p.suffix in ('.json', '.gz', '.jsonl', '.log'))
for path in paths:
    original = path.read_bytes()
    raw = gzip.decompress(original) if path.suffix == '.gz' else original
    target = out / (path.name if path.suffix == '.gz' else path.name + '.gz')
    compressed = gzip.compress(raw, mtime=0)
    if target.exists() and target.read_bytes() != compressed:
        raise RuntimeError(f'changed artifact: {target}')
    target.write_bytes(compressed)
    manifest.append(dict(file=target.name, raw_bytes=len(raw),
                         raw_sha256=hashlib.sha256(raw).hexdigest(),
                         gzip_sha256=hashlib.sha256(compressed).hexdigest()))
(out / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
main()
