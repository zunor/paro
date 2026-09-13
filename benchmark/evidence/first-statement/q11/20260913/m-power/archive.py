"""Archive all owned M-POWER observations, including slow samples and logs."""
import gzip
import hashlib
import json
from pathlib import Path

root = Path('/private/tmp')
destination = Path(__file__).resolve().parent / 'raw'
destination.mkdir(exist_ok=True)
records = []
for source in sorted(root.glob('paro-mpower-*')):
    if not source.is_file() or source.suffix not in ('.json', '.log'):
        continue
    data = source.read_bytes()
    packed = gzip.compress(data, mtime=0)
    target = destination / (source.name + '.gz')
    target.write_bytes(packed)
    records.append({'source': str(source), 'archive': target.name,
                    'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest(),
                    'gzip_sha256': hashlib.sha256(packed).hexdigest()})
(destination / 'manifest.json').write_text(json.dumps(records, indent=2) + '\n')
print(f'Archived {len(records)} files')
