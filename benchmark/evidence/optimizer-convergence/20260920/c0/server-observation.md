# Reuse the existing process-start observation contract

The clean main baseline does not contain the T0-b server outlet present in the
isolated experiment lineage. The first integration SQL diagnostic correctly
fails before the target query with all `paro_diagnostic/*` keys missing.
That failed run is retained as `q05-partial-grants-r1.json`; it is not a query
result and not a performance sample.

The context snapshot, startup initialization and read-only settings rows are
selectively ported from `d3097038`. No optimizer flag parsing, search policy or
execution algorithm is imported. The frozen external harness keeps its strict
expected/observed comparison; it is not bypassed or taught to tolerate a
missing outlet. This is preservation of prior measurement capability while
the unified Matrix remains pending, not a second profiler.

The context self-test checks unique declarations and immutable snapshot
identity. SQL verification must additionally pass the existing harness's
server-observed equality check before producing target evidence.
