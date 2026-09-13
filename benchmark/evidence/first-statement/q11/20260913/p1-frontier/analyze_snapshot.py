#!/usr/bin/env python3
"""Offline sensitivity, NOT a certificate authorizing candidate removal."""
import argparse
import collections
import gzip
import json
from pathlib import Path


def coordinates(candidate, project, envelope):
    c = candidate
    memory = (c['non_revocable'], c['minimum'], c['preferred'], c['peak'],
              c['spill'], c['external_slots'])
    # An optimistic local feasibility projection only. Parent retained state,
    # revocation and actual availability can distinguish these candidates.
    if (project and c['completion'] == 'Guaranteed' and
            max(memory[:4]) <= envelope and c['spill'] == c['external_slots'] == 0):
        memory = (0,) * 6
    return (c['expected'], c['risk'], c['work'], c['span'], *memory)


def retained(candidates, project, envelope):
    keep = []
    for i, c in enumerate(candidates):
        gate = (c['source_response_class'], c['tasks'], c['output_tasks'],
                c['external_workers'], c['completion'])
        xs = coordinates(c, project, envelope)
        for j, other in enumerate(candidates):
            if j == i:
                continue
            other_gate = (other['source_response_class'], other['tasks'], other['output_tasks'],
                          other['external_workers'], other['completion'])
            ys = coordinates(other, project, envelope)
            if gate == other_gate and all(a <= b for a, b in zip(ys, xs)):
                if ys != xs or j < i:
                    break
        else:
            keep.append(c['candidate'])
    return keep


def analyze(snapshot, envelope):
    if snapshot['omitted_candidates']:
        raise ValueError('incomplete snapshot')
    by_group = collections.defaultdict(lambda: collections.Counter())
    for f in snapshot['frontiers']:
        if 'objective: Latency' not in f['goal']:
            raise ValueError('non-Latency goal requires its own comparator')
        c = f['candidates']
        row = by_group[f['group']]
        row['goals'] += 1
        row['live'] += len(c)
        row['exact_coordinate_kept'] += len(retained(c, False, envelope))
        row['optimistic_projection_kept'] += len(retained(c, True, envelope))
        row['source_response_classes'] += len({x['source_response_class'] for x in c})
        row['high_water_sum'] += f['high_water']
    totals = sum(by_group.values(), collections.Counter())
    return dict(published_archive=snapshot['published_archive'], capture_us=snapshot['capture_us'],
                totals=totals, groups=by_group, proven_envelope_removals=None,
                warning='local slack projection is not equivalent under all parent/admission contexts; no production proof')


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('snapshot')
    p.add_argument('--envelope', type=int, default=2 * 1024 ** 3)
    p.add_argument('--output', type=Path)
    args = p.parse_args()
    opener = gzip.open if args.snapshot.endswith('.gz') else open
    with opener(args.snapshot, 'rt') as source:
        snapshots = [json.loads(line) for line in source]
    # Explicit selection by archived count only for report analysis, never a
    # production strategy. Print all largest-search occurrences, not fastest.
    largest = max(s['published_archive'] for s in snapshots)
    result = json.dumps([analyze(s, args.envelope) for s in snapshots
                         if s['published_archive'] == largest], indent=2)
    if args.output:
        args.output.write_text(result + '\n')
    else:
        print(result)
