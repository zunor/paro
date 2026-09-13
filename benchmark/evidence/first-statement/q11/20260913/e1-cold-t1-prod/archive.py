"""Compress explicit report prefixes and retain original/archive identities."""
import gzip
import hashlib
import json
import sys
from pathlib import Path

destination = Path(__file__).parent / 'raw'
destination.mkdir(exist_ok=True)
manifest_path = destination / 'manifest.json'
manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
for prefix in sys.argv[1:]:
    path = Path(prefix)
    sources = [path] if path.is_file() else sorted(path.parent.glob(path.name + '*'))
    if not sources:
        raise SystemExit(f'No artifacts: {prefix}')
    for source in sources:
        if not source.is_file():
            continue
        payload = source.read_bytes()
        archive = destination / (source.name + '.gz')
        compressed = gzip.compress(payload, compresslevel=6, mtime=0)
        if archive.exists() and archive.read_bytes() != compressed:
            raise SystemExit(f'Refusing to replace different archived evidence: {archive}')
        archive.write_bytes(compressed)
        manifest[source.name] = {
            'source_path':str(source), 'bytes':len(payload),
            'sha256':hashlib.sha256(payload).hexdigest(),
            'archive':archive.name, 'archive_bytes':len(compressed),
            'archive_sha256':hashlib.sha256(compressed).hexdigest(),
        }
manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + '\n')
