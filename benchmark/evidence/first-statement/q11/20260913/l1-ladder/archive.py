"""Preserve completed and pre-observation failed L1 attempts with hashes."""
import gzip
import hashlib
import json
from pathlib import Path

destination = Path(__file__).resolve().parent / 'raw'
destination.mkdir(exist_ok=True)
sources = []
for path in sorted(Path('/private/tmp').glob('paro-l1-*')):
    if path.is_dir():
        sources.extend((child, path.name + '/' + child.name) for child in sorted(path.iterdir())
                       if child.is_file() and child.suffix in ('.json', '.sql', '.log'))
    elif path.suffix in ('.json', '.log'):
        sources.append((path, path.name))
records = []
for source, relative in sources:
    data = source.read_bytes()
    packed = gzip.compress(data, mtime=0)
    target = destination / (relative + '.gz')
    target.parent.mkdir(exist_ok=True, parents=True)
    target.write_bytes(packed)
    records.append({'source': str(source), 'archive': str(target.relative_to(destination)),
                    'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest(),
                    'gzip_sha256': hashlib.sha256(packed).hexdigest()})
(destination / 'manifest.json').write_text(json.dumps(records, indent=2) + '\n')
print(f'Archived {len(records)} files')
