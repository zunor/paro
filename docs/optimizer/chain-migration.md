# Chain migration to the current optimizer

## Scope and decisions

The historical reference is `d30970380f3a6a8e991bd4973fb59f2fce724486`;
the migration starts at `0532c68e` on `re-op`. This is not a cherry-pick of
experimental flags or a claim that historical timings apply to current code.

* Materialized CTE necessary-domain normalization is now a common pre-Memo
  pass. Iterative traversal includes nested producer definitions. Explicit
  definition-column mappings replace positional assumptions; unsupported or
  unfiltered consumers prevent an unsafe finite producer restriction.
* Predicate movement reuses the native domain-transfer contract. Consumer and
  aggregate residuals remain enforced. Repeated normalization is idempotent.
* CTE predicate coverage is derived from exact selected candidate edges and
  producer consumption. Normalization/rule provenance is not a certificate.
* SQL exposes a typed, cache-keyed `optimizer_search_policy`: `quality`
  (default) or `budgeted`. Explicit embedding budgets take precedence. The
  old handoff environment switch is removed, not retained as an alias.
* Pattern matching checks cancellation/deadline even for cached or repeated
  visits that charge zero work. Statement cancellation also owns a monotonic
  timeout clock; retry does not reset it. Async timeout tasks use arm-time
  deadlines rather than starting a fresh delay when first polled.
* Detail rendering retains a bounded valid prefix instead of dropping all
  Detail when the encoded byte limit is reached. Omission counts remain exact.

Historical deferred physical-portfolio extraction is **not copied**. The
current producer must establish typed physical identity and verify every
published grant variant. Delaying this behind an old binder closure would
change admission/identity/error boundaries, not merely remove duplicate work.
All current grant alternatives remain available; this migration does not claim
to have delivered that separate optimization. Execution-image lowering already
has its own deferred lifecycle and must not be confused with physical identity.

## Evidence and collector repairs

Real collection exposed pre-existing integration defects: descriptive engine
labels were sent to the typed result validator; failed collection substituted
one receipt for several registered samples; cold receipts were missing from
the registered sample count; receipts were embedded repeatedly; cold-miss
analysis read an obsolete flat field; and finalization could attempt Completed
after sealing a cell Incomplete. These contracts are corrected, not bypassed.

Each normal observation retains one exact typed association. Cold/warm block
coordinates reference that owner. Campaign inputs are retained separately,
diagnostic captures have one owner, and failed registered samples are explicitly
Uncovered. JSON is compact and bounded before publication. A v3 association
contains compilation, admission and execution payloads and exceeds the old
2 KiB allowance; the preregistered allowance is now 4 KiB per association.
Receipt counts are not multiplied to buy capacity; the 64 MiB campaign ceiling
is unchanged. This is an explicit schema-capacity correction.

New managed instances use a relative data root. Snapshot preflight rejects a
catalog whose database paths escape that root, including absolute source paths.
It does not rewrite binary storage metadata or claim to validate every possible
hostile storage-file encoding. Two real clone/write/reopen checks verified that
the newly generated seed is unchanged.

## Validation status

The first completed exploratory matrix used two fresh process blocks per cell,
4 threads, 2 GB, binary results, optimizer verification enabled, DuckDB 1.5.5,
and a newly generated relative-path SF1 seed. Q04/Q11/Q74 completed in both
policies with full result/type/multiplicity/order checks. It is not a parity
certification or a clean-source causal comparison with historical chain.

That matrix predates the monotonic timeout fix and cold-miss summary-reader
fix. Preserve its original records; do not relabel its summary as verified.
Its exact nested receipts retain cold cache misses and measured compiler work.
Quality-mode compiler samples were approximately 75 ms (Q11), 723 ms (Q04),
and 80 ms (Q74); budgeted search reached its roughly 30-second search limit.
Both policies remain SearchIncomplete. Historical approximately 12 ms is not
restored, and no ProofComplete or first-statement parity claim is made.

The original regression control has 18 failures. The migration initially adds
only an internal CTE display-ID change and the deliberately changed settings
inventory; those two expectations are reviewed individually. Other expected
files are not regenerated. Final checks and fresh post-fix results are recorded
in the delivery report, separately from these exploratory observations.
