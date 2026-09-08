# Q11 cold-planning profile

This snapshot was collected from a fresh statement against the checked-in
TPC-DS SF1 Paro data with the release server built using `alloc-metrics`.
The query returned 99 rows and completed in about 2.11s on the local run.
Metrics are diagnostic only; no search budget was changed.

| component | elapsed | allocated |
| --- | ---: | ---: |
| semantic normalization | 2.3ms | 3.45MiB |
| query IR construction | 1.1ms | 1.65MiB |
| Memo exploration | 1,864.7ms | 4.59GiB |
| physical extraction | 1.3ms | 2.30MiB |
| winner verification | 0.07ms | 61KiB |

The search produced 1,022 groups, 1,613 logical expressions and 2,738
physical expressions.  It was intentionally budget-limited (the summary
reported `search_complete = 0`), so this run is not a correctness or plan
quality baseline by itself.  Settlement had 13,665 local hits and 2,304
misses; this is the primary cold-path optimization target after the session
arena transfer.

The largest rule allocation counters were CTE partitioned materialization
(398MiB), predicate transfer (421MiB), join-region enumeration (435MiB), CTE
inline (230MiB), CTE filter pushdown (260MiB), and aggregate join subsumption
(130MiB).  These numbers are rule-scoped allocations, not a license to lower
their budgets.  Any follow-up must preserve group/expression counts, search
obligations, and the `settlement_local_hit_count`/`miss_count` relationship.

The profile was the motivation for three structural changes in this revision:

* typed diagnostic values distinguish bytes, counts and invocations;
* settlement computes pass-through layouts from borrowed fact layouts and
  avoids cloning scalar-free operators on cache hits;
* staged arena slots are absorbed into one planner-session arena rather than
  allocating a new arena for every transformed alternative.
