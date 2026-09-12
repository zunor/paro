"""Exclusive sample attribution inside the optimizer, not nested timer sums."""
import collections
import json
import re
import sys
from pathlib import Path

nodes = []
stack = []
for line in Path(sys.argv[1]).read_text().splitlines():
    if line.startswith('Total number in stack'):
        break
    match = re.match(r'^([ +!:|]*)(\d+) (.+?)  \(in ', line)
    if not match:
        continue
    depth, count, name = len(match[1]), int(match[2]), match[3]
    while stack and nodes[stack[-1]]['depth'] >= depth:
        stack.pop()
    parent = stack[-1] if stack else None
    node = dict(depth=depth, count=count, name=name, parent=parent, children=0)
    if parent is not None:
        nodes[parent]['children'] += count
    nodes.append(node)
    stack.append(len(nodes)-1)

exclusive = collections.Counter()
assert all(node['children'] <= node['count'] for node in nodes), 'invalid sample-tree attribution'
branches = collections.Counter()
categories = collections.Counter()
total = 0
for index, node in enumerate(nodes):
    own = node['count'] - node['children']
    if own <= 0:
        continue
    lineage = [node]
    parent = node['parent']
    while parent is not None:
        lineage.append(nodes[parent])
        parent = nodes[parent]['parent']
    lineage.reverse()
    optimizer = next((i for i,n in enumerate(lineage) if 'Optimizer::optimize::' in n['name']), None)
    if optimizer is None:
        continue
    total += own
    exclusive[node['name']] += own
    labels = [n['name'] for n in lineage[optimizer:]]
    if any('explore_transformations_with_interleave' in n for n in labels):
        position = next(i for i,n in enumerate(labels) if 'explore_transformations_with_interleave' in n)
        branch = labels[position+1] if len(labels)>position+1 else labels[position]
    else:
        branch = labels[-1]
    branches[branch] += own
    # Disjoint categories: the first matching semantic branch wins.
    category = next((label for needle,label in [
        ('apply_binding','rule_apply'),('statistics','statistics'),
        ('compose_candidate_cost_with_sources','cost_composition'),
        ('intern_child_combination_event','combination_identity'),
        ('constrain_composed_cost_to_grant','grant_constraint'),
        ('resolve_task_supply','task_supply'),
        ('drain_physical_interleave','other_physical_interleave')]
        if any(needle in n for n in labels)), 'other_optimizer')
    categories[category] += own
print(json.dumps({'optimizer_exclusive_samples':total,'categories':categories,
                  'branches':branches.most_common(20),'top_exclusive':exclusive.most_common(20)},indent=2))
