# Q11 first-statement evidence — 2026-09-09

This directory archives the D0/D1/D6 measurement and regression evidence used
to start and review the native-rule/context-reuse refactor. Diagnostic files
are retained separately from the normal C1 cohort; they are not mixed into the
performance gate.

## Source and measurement identity

- Repository commit: `b7436889b17adc0287198cc3eaab5356615adedd`
- Working-tree source hash recorded by the formal report:
  `6d2158fa70ce58459e197a7b4db9d0d3800582d411f329e2e1f2e1f2e1b6e4bdd4f7d5`
- Formal binary SHA-256:
  `2b35c090b0f693d69f892b1eb7ff6ddb74fd3b13b85f5611fca3b23f6aa8892a`
- Formal design: five fresh process blocks, three measurement rounds per
  block, 30 W samples per engine, four threads, 2 GiB memory, binary protocol.
- Normal C1 measurement had tracing disabled; the diagnostic trace was kept in
  a separate sample and excluded from C1.

The latest native-rule/refactor diagnostic build has the same source commit,
dirty working-tree identity
`7cce140d6b59115e4f6a6a397c4100e69d6cd46b4aeed46feacec56ef3e6e07e`, binary
SHA-256
`395784600ce0927d78ac89bf0c7023faf9b0f7f0c9251a78769d45daa6146663`, and
Paro seed SHA-256
`d0bc0cd601e982011b1f3c6c28a1a2c6655d0dd23f2770ecfdfe114a5a35a543`.
The formal and latest reports are both dirty-working-tree evidence, not
post-commit release attestations.

## Formal Q11 result

- Paro cold C1 median: `923.122625 ms`
- DuckDB cold C1 median: `136.748000 ms`
- Paro/DuckDB cold ratio: `6.532112`
- Warm W ratio: `0.960113`, 95% CI `[0.890299, 1.032416]`
- Diagnostic optimizer elapsed: `763345 us`
- Diagnostic fetch/drain: `138167 us`
- Diagnostic sample count: one; it is evidence for decomposition, not a
  stable lower-bound claim.
- Gate status: evidence valid and regression compared; parity milestone not
  passed and cost model not admitted.

## Latest trace-off C1 refactor check

`q11-native-shell-final-v1.json` is a separate two-block check of the custom
90-row Q11 shape in `/private/tmp/paro-tpcds-q11-query/11.sql`. It is not a
replacement for the formal five-block Q11 anchor above.

- Normal trace-off C1: Paro `836.173396 ms`, DuckDB `110.834834 ms`.
- Fresh-block ratio: `7.549352`, 95% CI `[7.336191, 7.768707]`.
- W ratio: `0.920821`, 95% CI `[0.908179, 0.934772]`.
- Four samples per engine, two fresh process blocks, binary result protocol,
  four execution threads, 2 GiB, complete 90-row result and verified cache
  miss. This is still far from the C1 parity target and does not pass M1–M3.

The three independent diagnostic blocks were excluded from C1. Their target
events were: `parse_operation` `688.948–690.822 ms`, optimizer
`686.895–688.454 ms`, and portal/fetch `136–137 ms`. The same traces reported
`physical_subproblem_requests=15384`, `reuses=11889`, `evaluations=3495`;
`PredicateTransfer` had 631 applicable / 253 published applications and
`JoinRegionEnumeration` had 600 / 120.

`q11-e8-v1.json` is the execution-profile artifact: 35 EXPLAIN plan
occurrences and 73 EXPLAIN ANALYZE runtime profiles. The plan and runtime
coordinates are deliberately marked `diagnostic_only_unaligned`; no per-node
q-error or G-Stats/G-Cost admission is inferred.

`q11-cold-planning-final-v1.json` is the existing diagnostic EXPLAIN collector
run for the same 90-row shape. It confirms `memo_exploration=663.541 ms` and
the largest aggregated rule elapsed values are `PredicateTransfer=114.237
ms`, `JoinRegionEnumeration=102.067 ms`, and
`AggregateDimensionDeferral=38.159 ms`. These are EXPLAIN diagnostic timings,
not C1 timings; they are used only to prioritize the native-rule work.

## Regression and FD diagnosis

The final high-FD run used:

```text
ulimit -n 16384
PARO_WRITE_ACTUAL=0 make -C regress check
```

Final result: `184 passed, 0 failed, 0 skipped, 0 new` in `47.49s`.

The first high-FD run completed `168 passed, 16 failed`. All 16 failures were
expected-plan drift from newly printed `Column IDs`/`logical_node_id`, not
semantic regressions. The exact expected outputs were refreshed, and the
per-bind logical IDs are now normalized before comparison. During the run the
server FD count stayed bounded (observed peak about 366, with no monotonic
growth) and no `Too many open files` occurred. This establishes that the prior
failure is a test-capacity/FD-envelope issue under the default limit; it does
not, by itself, prove that every resource-lifecycle path is leak-free.

## Archived files

- `formal-v1.json` and `formal-v1-summary.json`: formal gate report.
- `q11-native-shell-final-v1.json`: latest normal trace-off C1 and separate
  diagnostic cohort after the first native-rule/context-reuse changes.
- `q11-e8-v1.json`: plan/runtime coordinate inventory and execution summary;
  intentionally not aligned into per-node q-error.
- `q11-cold-planning-final-v1.json`: diagnostic optimizer component and
  per-rule elapsed report for the custom shape.
- `d0-preregistration-v1.md`: frozen C1/W envelope, sample plan, statistics,
  stop rules and milestone definitions.
- `binary-q11.log`: binary-protocol smoke output.
- `q11-diagnostic000.parod.log`, `q11-oracle.parod.log`, the native-shell
  block/diagnostic logs, the E8 server log, and the five `formal-v1` block
  logs: diagnostic and fresh-block server logs.
- `pilot-v4.json` and its Q11 logs: preceding pilot evidence.
- `sql-regress-report.txt`, `sql-regress-error.txt`, `sql-regress.log`: final
  high-FD SQL regress output.

The `.log` copies are retained for local inspection; corresponding tracked
text copies are listed in `SHA256SUMS.txt` so the evidence remains visible
despite the repository-wide `*.log` ignore rule.
