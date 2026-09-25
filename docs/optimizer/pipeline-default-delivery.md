# Pipeline default: implementation and validation

The user authorized switching the default **without deleting production
Cascades**. `pipeline` is now the single typed default for SQL sessions, RESET
and contexts without a session override. `quality`, `budgeted` and `regional`
remain explicit choices. A failed pipeline plan never silently selects them.

## Production changes

- Statement layers attach after relation selection. Self-reading mutations
  use the existing executable input-spool barrier, cost and write contracts;
  exceeding its non-spillable envelope remains an error. COPY formats own
  their versioned semantic binding, not Debug text or a file handle.
- Local search-provider selection preserves TopK windows, scoring/residual
  contracts and catalog capability dependencies. Graph, search, range join,
  DML and external-routine payloads have typed structural identity encoding.
  The structural identity domain advances to v6; historical v5 fingerprints
  must not be compared as if the encoder were unchanged.
- Direct physical selection may lower DOP when no implementation fits. It
  does not rerun logical search, fabricate another memory grant, or switch
  policies. The number of attempted operating points is observable.
- ORDER BY resolves a complete unqualified output name before the input
  namespace. Compound expressions, quoting, ambiguity and WHERE retain their
  distinct contracts. This fixes the Q58-shaped binding failure generically.
- Window append-only aggregation keeps a fixed-capacity payload workspace
  instead of allocating input vectors for every delta. Chunk owns active/spare
  buffers, including validity and variable-width reset. Update/finalize order
  and the independent recomputation fallback are unchanged.
- RunOutput capacity failure also terminates an already published campaign
  summary, retaining the original failure and completed attempts.

## Validation boundaries

The default-switch workspace run passed 7,019 tests (85 ignored). Subsequent
identity and window regression tests require the final run recorded below;
the earlier result does not certify code added afterwards.

Release SQL regress was run from empty owned storage with pipeline active for
setup, writes and queries: **164 passed, 21 failed**. The maintained whole-block
audit found plan snapshots, two run-scoped IMPORTS path echoes, one Memo-only
observability query and the changed pg_settings default/description. It found
no other non-EXPLAIN result differences. Raw snapshots remain failed, and no
expected result was updated. An earlier run uncovered unsupported external
operator identity; that implementation gap was fixed, not blessed away.

The latest local audit is
`/private/tmp/paro-pipeline-default.7dx8h6/regress-audit.json`; final compact
evidence and source identity will be recorded after validation.

This implements default selection, not the full PipelineReady release gate.
Q39 certification, the historical spill reproducer, bounded same-execution
operator profiling, the cold-path ledger and RF/corpus ablations are still
separate unfinished items in the convergence plan. The entire plan is not
complete, and no performance/parity claim follows from the default change.

## Exploratory measurement registration

EvidenceId: `pipeline-default-window-workspace-v1`.

After builds/tests stop, use the maintained TPC-DS collector on Q04/Q11/Q74,
Q51 and Q58. Two fresh process blocks per cell, one warmup, one measurement
round, 1,000 bootstrap resamples and one separate diagnostic block. Four
threads, decimal 2GB, binary results, normal verifier off/trace off,
`PARO_COMPILE_WORK_EVIDENCE=1`; compare the explicit pipeline policy with
DuckDB 1.5.5. The generator-declared metadata track is non-qualifying for
parity. This is screening, not a pre-powered non-inferiority experiment.

Reuse the relocatable seed `/private/tmp/paro-migration-relative.u1PLBV` and
the selected local DuckDB TPC-DS SQL source. The collector records actual
source/binary/SQL/seed identities. Required DuckDB native module SHA256:
`85fad85339c7e345eabb66a33a25d8503c3ee8881f9d298fa8499a97617dbf68`.
The package declaration is `benchmark/requirements.txt`; no dependencies are
upgraded. Preserve every valid slow sample and any error; a result failure
blocks performance certification. Do not subtract diagnostic time from C1.
Store ordinary bounded RunOutput evidence only, at most 64 MiB, outside the
source tree during collection. Historical samples are context, not a causal
control. No same-process policy A/B or execution-layer speedup is certified by
this limited run.
