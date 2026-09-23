# Selected properties and physical-subproblem ownership

EvidenceId: `optimizer-property-ownership-20260923-v1`.
Registration: [property-ownership-registration.md](../property-ownership-registration.md).
Control: `437c6539`. Final measured probe: `11462c1d` (both clean).
Machine-readable results: [summary.json](summary.json),
[validation.json](validation.json). This is an engineering pilot, not a
powered performance, warm non-inferiority, or cross-engine parity certificate.

## Outcome

All three ordered interventions are implemented. The substantial measured
benefit is Q74: normal compiler median **72.55 -> 40.22 ms (-44.6%)** with the
same selected/admitted identities, resource contracts, logical/physical counts,
and 1,045 cost syntheses. Q04 has a small directional compiler reduction.
Q11 does **not** demonstrate a repeatable reduction or meet the 10 ms target.
No budget, deadline, cost objective, execution operator or search policy was
changed to obtain these results.

Each arm has six independent fresh processes per query, collected in the
registered C/P/C/P order, with tracing off and the bounded compile-work receipt
observer enabled on both arms. Detail captures are separate processes. All
typed full-result, order and cache-miss checks passed. Raw slow samples remain
in the accepted normal cells; they are not removed as outliers.

| Query | Compiler median control -> probe (ms) | Compiler P90 control -> probe (ms) | C1 median control -> probe (ms) | Warm median control -> probe (ms) |
| --- | --- | --- | --- | --- |
| Q04 | 20.287 -> 19.346 | 20.822 -> 19.660 | 258.920 -> 268.268 | 153.584 -> 164.099 |
| Q11 | 13.444 -> 13.557 | 13.589 -> 23.004 | 159.354 -> 166.556 | 96.680 -> 93.040 |
| Q74 | 72.550 -> 40.223 | 77.232 -> 44.252 | 187.299 -> 152.479 | 64.092 -> 67.157 |

P90 uses the maintained harness percentile function; six samples do not
certify a tail-latency SLO. All arms stop at `QualityPolicySatisfied` with
`search_complete=false`, not `ProofComplete`. This campaign does not claim
performance parity or complete-search acceleration.

## The three contracts

### 1. Local, dependency-driven quality properties

`SelectedQualityProperties` belongs to one planner/provider. Its node entries
are keyed by exact immutable `CandidateId`, with a live fact `PatternRead`,
child revision vector and monotone property revision. Iterative postorder
refresh rejects incomplete/cyclic choices. It reuses unchanged local results;
a changed leaf reopens its ancestors without rebuilding an unrelated sibling.
It stores no frontier membership or candidate price.

CTE predicate demand is a separate property keyed by the selected producer
definition/input and **all selected incoming consumer edges**, including
immediate parent revisions. A producer/root fingerprint alone is insufficient.
Absent or unsupported evidence remains unavailable, not a proof of coverage.

One provider evaluation replaces preflight followed by a second frozen-tree
derivation. The bundle registry owns applicability and local fact readiness.
Only locally ready claims demand the full exact-root choice manifest; readiness
itself cannot certify anything. Region/choice validation and the final
executable verifier still gate handoff. The old frozen-tree derivation remains
test-only as an independent oracle, not as a second production path.

In each final Q74 diagnostic, 140 evaluations produce **423 local node builds
and 4,644 reuses**, plus **72 CTE-demand builds and 68 reuses**. Combined
exclusive QualityEvidence + QualityDomain work falls from **34.23/39.55 ms**
to **11.34/11.56 ms** in the two separate diagnostic pairs. These are
diagnostic attribution values, not numbers subtracted from normal C1.

The implementation still inspects the selected DAG to discover dependencies;
it does not claim constant-time whole-query certification. The remaining
roughly 11 ms of Q74 quality evidence is a real residual, not hidden work.

### 2. One query-statistics construction boundary

`settle_query_properties` owns storage/bound-column propagation followed by
one final relation-fact gathering pass. The old preliminary gather in
Gather -> Propagate -> Gather annotated a tree whose column map was immediately
replaced. It is removed rather than cached. CTE/delim publication order and
statistics provenance remain authoritative at the final gather.

Independent tests run both constructions and compare every node's operator
kind, statistics and output layout, plus serialized column evidence, across
the existing projection, limit, CTE/delim, join and empty-grouping cases.
The change does not remove necessary semantic normalization or claim that
every remaining owned boundary has disappeared. No independent latency win
is attributed to this stage from the combined final pilot.

### 3. One owner per exact physical subproblem

The corrected post-stage-2 Q74 diagnostic put Subproblem + Dependencies at
about 5.4 ms. That supported a scoped ownership consolidation, not a new
physical search algorithm or another price cache.

`PhysicalSubproblem` now owns resident task state, published dependency
snapshot, exact dirty recipes, completion notification, phase recost flag and
recipe sequence for `(GroupId, OptimizationGoal)`. Six independently managed
maps/sets no longer have to remain synchronized. The task registry still owns
lifecycle/proofs; reverse indexes only route notifications. The resident's
immutable snapshot is the context it consumed, not another mutable owner.

Completion-only wakes remain distinct from cost invalidation. Group redirects
merge pending work and dependencies but discard stale resident completion.
Mandatory-to-optional coverage and empty-delta semantics remain covered by
real engine tests. Final synthesis/readset/subproblem counts are unchanged;
there is no independent performance claim for this ownership refactor.

## Search and selection checks

The following values match across every normal sample and both arms:

| Query | Groups / logical / physical | Cost syntheses | ReadSet rebuilds | Physical requests / evaluations / reuses |
| --- | --- | --- | --- | --- |
| Q04 | 69 / 77 / 121 | 782 | 418 | 284 / 264 / 75 |
| Q11 | 53 / 61 / 105 | 401 | 301 | 200 / 187 / 68 |
| Q74 | 95 / 136 / 217 | 1045 | 486 | 375 / 307 / 294 |

ReadSet/subproblem counts come from the paired diagnostic captures. Artifact,
structure, dependency, actual admission fingerprint, grant class, resource
contract and stop state are compared through typed receipts. Each associated
Detail capture has the same artifact identity. Identity agreement is a
consistency check, not a standalone semantic equivalence proof; independent
oracles, final verification and complete typed SQL results provide the other
checks. Detail has declared omissions, so it is not a complete event history.

## Negative results and investigations retained

- `exploratory/stage2-q04-run`: the cold collector refused a record beyond its
  registered capacity. The campaign is `Incomplete`, not a timing pass. The
  pre-registered amendment uses the existing TPC-DS collector; no buffer limit
  was raised to suppress this result.
- The first unified provider eagerly constructed full manifests for blocked
  candidates. This changed quality work readiness/priority, not just its CPU
  cost: Q74 expanded to **549 / 1082 / 1458** groups/logical/physical and
  **10,912** syntheses. Stage-1-only isolation reproduced it. The separate
  joint-region boundary correction did not fix that regression. All three
  negative captures remain under `exploratory/`.
- The remedy is policy-owned demand: evaluate local properties first and
  request the full proof only after its applicable facts are ready. The
  independent certificate still verifies exact choices. The demand-proof
  capture restores **95 / 136 / 217** and **1,045**, followed by the final
  controlled matrix. No query-specific scheduling preference was added.
- Q04 batch 1 warm median rose about 19.4%, crossing the registered 10%
  investigation threshold. Exact plan, admission and resource receipts are
  identical. DuckDB warm did not rise in that batch, so competitor drift does
  **not** explain it. Across all registered blocks Q04 warm is +6.85%, Q74
  +4.78%, Q11 -3.76%; none is a formal non-inferiority certificate. The
  first-batch negative signal remains in the archive.
- Q11 probe batch 2 contains compiler samples **23.004, 36.987, 14.210 ms**;
  its Paro and DuckDB C1 distributions both slowed. This does not prove an
  environmental cause or authorize dropping samples. The combined Q11 result
  is inconclusive, not the faster first batch selected as the headline.
- The first SQL run used a task-owned report directory and got two fixture
  IMPORTS echo-path differences (183/185). The error file is retained byte-for-
  byte as JSON `raw_utf8` with its original SHA. The unchanged compare-only suite
  passed **185/185** with its standard report path. Both outcomes are retained,
  and the preexisting user report was restored afterward.

## Reproduction and evidence ownership

Sources are committed; one selected checkout/shared Cargo target was built
sequentially without concurrent measurement/build/test work. No new worktree,
shared dataset mutation or dependency/toolchain upgrade was needed.

Use `benchmark/corpora/tpcds_compare.py`, once per query in each C/P/C/P batch:

```text
PYTHONPATH=benchmark PARO_COMPILE_WORK_EVIDENCE=1 \
benchmark/.venv/bin/python benchmark/corpora/tpcds_compare.py \
  --server-data-dir <relocatable-SF1-seed> --duckdb-database <SF1.duckdb> \
  --dataset-source-dir <SF1-csv> --query-dir <registered-tpcds-query-dir> \
  --report <unique-run-output>.json --start <04|11|74> --end <same-query> \
  --listen 127.0.0.1:16433 --process-blocks 3 --diagnostic-process-blocks 1 \
  --measurement-rounds-per-process 1 --warmups-per-process 1 \
  --bootstrap-samples 10000 --random-seed <2026092303|2026092304> \
  --threads 4 --memory-limit 2GB --optimizer-search-policy quality \
  --optimizer-verify off --metadata-track generator-declared \
  --paro-result-format binary --build-jobs 4
```

Exact input, compiler, harness, DuckDB **1.5.5** native package, SQL, seed and
binary identities live in each RunOutput's `inputs.json`. The final arms use
one identical binary hash per arm across both batches. The metadata track is
generator-declared and cannot certify cross-engine parity. Normal compile
receipts are opt-in bounded observations whose cost is included, not claimed
to be zero. Compile/cache/admission/first-statement timer boundaries are intact.

Consumers validate `campaign.json` against `manifest.json`, resolve the
explicit `accepted_attempt_id`, validate its typed payload, and validate the
referenced capture and SHA. Only normal receipts with `compilation=Executed`,
`cache_hit=false` and `status=Verified` supply compiler samples. Cached warm
receipts are not new compilations. No event-array position or latest attempt
is used to guess ownership. `summary.json` is a derived convenience view;
the sealed producer outputs remain authoritative.

`matrix/` contains the 12 final bounded RunOutputs; `exploratory/` retains the
seven preceding bounded runs, including the capacity refusal. They fit the
registered 20 MiB budget. No binary, server log or duplicate raw event stream
is committed. Checksums cover the archived producer files, not themselves.

## Validation and remaining scope

- Workspace: **6,933 passed, 0 failed, 85 ignored**; optimizer: **1,394 passed**.
- Workspace check and strict all-target Clippy passed.
- Benchmark: **207 passed**; regress unit: **103 passed, 1 skipped**.
- Fresh-instance SQL regress with verifier on and FD=65,536: **185 passed**.
- Changed Rust formatting, memory/vector guards and calibration check passed.
- Repository-wide header audit still reports **169 preexisting findings** in
  untouched files; this is not a claim that all of `make static` is green.
- The unused quality-preflight setting and its one expected settings row were
  removed explicitly. No plan/result baselines were regenerated or blessed.

Next performance work should use the new attribution: Q74 still has selected
DAG evidence work after the large reduction; Q11/Q04 have relatively little
quality work and need their remaining construction/finalization costs measured
separately. This delivery does not promise another cache, parallel search,
complete-search proof, or a 10 ms result from the remaining work.
