"""Archive an owned hygiene attempt before the runner resets its report dir."""
import gzip
import hashlib
import json
from pathlib import Path
import sys

label = sys.argv[1]
assert label in ('fd256', 'fd16384', 'unit')
destination = Path(__file__).resolve().parent / 'raw' / label
destination.mkdir(parents=True, exist_ok=True)
sources = ([Path('/private/tmp/paro-hygiene-optimizer.log'),
            Path('/private/tmp/paro-hygiene-mark-isolated.log')]
           if label == 'unit' else list(Path('/private/tmp').glob('paro-hygiene-*.log')))
sources += list(Path('/private/tmp').glob('paro-ubatch-*.log'))
if label != 'unit':
    sources += [p for p in Path('/private/tmp/paro-tcwc-baseline-ZjwFtb/regress/report').iterdir()
                if p.is_file()]
records = []
for source in sorted(sources):
    data = source.read_bytes()
    packed = gzip.compress(data, mtime=0)
    target = destination / (source.name + '.gz')
    target.write_bytes(packed)
    records.append({'source': str(source), 'archive': target.name,
                    'sha256': hashlib.sha256(data).hexdigest(),
                    'gzip_sha256': hashlib.sha256(packed).hexdigest()})
(destination / 'manifest.json').write_text(json.dumps(records, indent=2) + '\n')
print(label, len(records))
