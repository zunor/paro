"""Archive this experiment's owned outputs without touching inputs or user files."""
import gzip
import hashlib
import json
from pathlib import Path

destination = Path(__file__).resolve().parent / 'raw'
destination.mkdir(exist_ok=True)
records = []
reports = []
for source in sorted(Path('/private/tmp').glob('paro-e3-*')):
    if not source.is_file() or source.suffix not in ('.json', '.log'):
        continue
    data = source.read_bytes()
    target = destination / (source.name + '.gz')
    packed = gzip.compress(data, mtime=0)
    target.write_bytes(packed)
    records.append({'source': str(source), 'archive': target.name,
                    'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest(),
                    'gzip_sha256': hashlib.sha256(packed).hexdigest()})
    if source.suffix == '.json':
        report = json.loads(data)
        if report.get('corpus') == 'TPC-DS':
            assert report['source']['dirty'] is False
            q = report['queries'][0]
            assert q['status'] == 'passed' and q['rows'] == 90
            for block in q['process_blocks']:
                evidence = block['cold_miss_evidence']
                assert evidence['status'] == 'verified'
                assert evidence['cache_hit'] is False and evidence['occurrence'] == 0
                assert block['statement_trace']['enabled'] is False
                assert block['statement_trace']['verified_empty'] is True
            reports.append({'archive': target.name, 'source': report['source'],
                            'binary_sha256': report['build_attestation']['binary_sha256'],
                            'pre_touch_diagnostic_only': report['pre_touch'] is not None,
                            'launch_window_compromised_campaign': source.name in {
                                f'paro-e3-{arm}.json' for arm in
                                ['control-a', 'stream-a', 'stream-b', 'control-b']}})
(destination / 'manifest.json').write_text(json.dumps({'files': records, 'reports': reports}, indent=2)+'\n')
print(f'Archived {len(records)} files, {len(reports)} fresh reports; all target typed90/first-miss checks passed')
