# Mandatory completion is not optional implementation completion

The minimal direct-rowset RF fixture had one unchanged join and two scans.
Before this repair it finished with only 3 physical expressions, 6 cost
compositions and `Complete`: the optional RF and left-build alternatives had
never been enumerated. The complete bounded test observation is retained in
`c0/rf-selection-triage.log` in the private archive (not a performance sample).

`reset_cost_epoch` correctly invalidated tasks and cleared cost frontiers at
the mandatory/optional boundary. Resident physical task state nevertheless
retained its ReadSet and recipe cursor. `optimize_group` enumerated local
implementations only if logical membership had changed. Because no logical
rewrite was necessary, it merely repriced the mandatory recipes and then
mistook that prefix for completion. The existing implementation-seen key
already included the mandatory/optional phase, but the call that could reach
that key was bypassed.

The resident task now records the implementation phase actually consumed.
A phase change invalidates fast reuse, reopens a reused task and requests
local implementation enumeration using the existing phase-aware seen set.
Unchanged requests in the same phase still reuse the resident cursor.
No budget, cost coefficient, objective, frontier, rule, grant or stopping
policy is changed; previously omitted legal physical work is restored.

The new no-transformation engine oracle offers mandatory cost 100 and optional
cost 1 with one logical expression. Both physical implementations must exist,
the latter must win, obligations must be empty, and a repeated unchanged
request must neither enumerate nor publish again.

Full optimizer tests: **1344 passed / 0 failed**, log
`c0/implementation-phase-full-tests-r2.log`. All eleven previously unresolved
assertions pass unchanged: seven RF/build-side cases, singleton aggregate,
calibration selection, and both certified-bound pruning cases. The earlier
four fixture adjudications remain documented separately. This is not a
blanket blessing of historical failures or a full workspace/SQL pass.

The SQL facet lifecycle errors and Q39 numerical oracle contract remain
separate C2 blockers. Restoring optional enumeration may change plans and
work counts; no performance equivalence is claimed.
