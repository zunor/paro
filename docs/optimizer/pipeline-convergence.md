# Staged planner convergence

## Decision and evidence boundary

Continue one committed staged plan with bounded region search. Do not rebuild
Memo scheduling under another name or inflate a global search budget. The
three-query pilot demonstrates cheaper compilation, not migration completion.

The current Q04 discrepancy is a hypothesis with two independently testable
parts: incoherent CTE/Filter statistics and a different consumer join tree.
The full compound grouping key plus a disjoint UNION discriminator is unique;
`customer_id` alone is **not** thereby proven unique. Functional dependencies
must come from enforced constraints or relational algebra, not the benchmark's
observed data, an estimated NDV, or the other planner's chosen row count.
Similarly, subtracting cold/warm medians does not attribute a 47 ms cold path.
An associated statement timeline must establish that attribution separately.

## Ordered implementation

1. Share semantic key/finite-domain transfer through CTE definition identities,
   projections, constant restrictions and disjoint UNION ALL branches. Keep
   these proofs separate from estimated NDV and sampled value ranges. Recursive
   CTE anchors do not prove a fixed-point domain. Recompute residual Filter
   estimates from their current child instead of inheriting a stale DP count.
   Check the correct laws: ordinary Filter does not increase rows; grouped
   non-grouping-set aggregation does not increase rows (scalar aggregation is
   an exception); UNION ALL adds branch estimates; a unique build key bounds
   an inner join by its probe input, not always by the smaller input.
2. Extend the existing joint-region physical-response owner to ordinary inner
   join regions. Keep full-support residual activation, semantic boundaries,
   exact child choices, bounded work and an explicit fallback domain. Do not
   maintain a second independent cardinality formula for these transitions.
3. Compare committed single-stage and joint partial/final plans using typed
   regional policy, not query-name checks, forced build sides, or experiment
   environment switches. Results and resource feasibility must match before
   interpreting work/latency differences. Do not tune costs to one winner.
4. Run maintained correctness/breadth collectors, preserving every failure.
   TPC-DS 99/TPC-H 22 are migration screens, not substitutes for the repository's
   full corpus gate (JOB, CEB, TPC-DS, TPC-H, LDBC). Q39 numerical certification
   must bind the actual build/plan; Q58 binding and Q51 execution failures must
   not be hidden by a planner fallback or relaxed result contract.
5. Complete explicit write, indexed search, graph and low-resource capability
   tests before default promotion. An unsupported path must fail honestly;
   silently falling back to the old optimizer would hide missing coverage.
6. Only after capability/correctness gates close, run a registered clean paired
   performance campaign. Proposed promotion thresholds: all results pass their
   declared contract; geometric-mean warm pipeline/quality one-sided 95% upper
   bound <=1.02 and every query <=1.20; compiler p50 <=10 ms and p99 <=30 ms on
   the declared analytic corpus/envelope. Register sample size/power from a
   separate pilot before collecting certification data. Explained regressions
   remain failures unless the product contract is explicitly changed beforehand.
   These are promotion gates, not DuckDB parity or exhaustive-optimality claims.

After default promotion and consumer migration, remove the old production
search engine, keeping small independent exhaustive test oracles. No permanent
dual optimizer, production benchmark exception, or debug-only safety checks.

## Separate execution workstream

Normal receipts plus separate EXPLAIN ANALYZE must distinguish first scan/decode,
metadata, admission/lowering and scheduling. Preserve the same execution image
and actual resource envelope in a cold/warm comparison. Scan/aggregate runtime
work and a future breaker-local adaptive build choice are not prerequisites
for inventing a unique-key proof or declaring this optimizer migration done.
Prioritize from measured same-statement attribution rather than residual
differences between medians. Performance interference invalidates certification,
not the correctness reproducer.
