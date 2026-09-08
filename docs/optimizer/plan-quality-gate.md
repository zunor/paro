# Known-cardinality plan-quality gate

The SQL regression runner compares text and result bags. The independent
quality collector compares estimated cardinalities against deliberately small
SQL fixture oracles. It does not accept an updated `.result` file as evidence
that a worse estimate is desirable.

From the repository root:

```sh
ulimit -n 8192
make -C benchmark quality-collect QUALITY_REPORT=report/quality-candidate.json
make -C benchmark quality QUALITY_REPORT=report/quality-candidate.json QUALITY_BASELINE=report/quality-baseline.json
```

`quality-collect` builds the source revision, starts a **fresh temporary owned
database** on port 6442, loads `benchmark/corpora/plan_quality/setup.sql`, then
captures EXPLAIN JSON and executes each SELECT. The server is stopped and the
temporary database removed on exit. It never changes the TPC-DS seed or a user
database. Use `QUALITY_LISTEN` for another unused local address.

Every case in `cases.toml` names a semantic boundary. Most are result boundaries;
the vector scan and aggregate-subsumption inner stages use explicit, uniquely
matched plan selectors plus separate SQL cardinality oracles. An absent or
ambiguous selector is an error, not a dropped observation. This avoids falsely
equating runtime pipeline IDs with physical EXPLAIN node IDs. These internal
counts are **independent SQL oracle counts**, not claimed to be runtime counters
from the selected physical node; the selector/oracle correspondence is part of
the reviewed fixture contract. The ordinary query result count is checked too.

The report records fixture and SQL identities, collector hashes, settings,
build/binary identity and complete plans. Source or binary changes during a run
invalidate it. Comparisons require identical corpus/collector/settings, but
permit different compiler source revisions. Baselines must be complete raw
reports: a supplied percentile summary cannot bypass missing cases. The gate
rejects any individual boundary's q-error increase, even when the global tail
improves. Non-finite input cardinalities and duplicate observations are invalid.
Zero-versus-positive cardinalities have infinite q-error, encoded as `"inf"` in
strict JSON.

The nullable-unique singleton case additionally gates the number of aggregate
nodes. Its current extra partial-merge phase is recorded, not blessed as a
permanent target: removing it is reported as an improvement, adding a phase is
a regression independently of q-error. There is no automatic bless command.
Changing fixtures, selectors or oracles requires an explicitly reviewed new
baseline contract.

This is neither a complete semantic test suite nor a timing comparator. In
particular, the partitioned-CTE winning case and native NULL-safe singleton
proof remain separate implementation work. The cold-planning gate continues to
measure fresh-process latency, peak RSS, search completeness and search counts.
