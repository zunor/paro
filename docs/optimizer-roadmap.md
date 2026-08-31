# Optimizer roadmap

This file records optimizer work that is intentionally excluded from the
current physical contract. Each item names the correctness proof and regression
gate required before implementation; neither is a compatibility obligation.

## Scan-order physical alternatives

Model statistics-guided segment order as a provided physical property and a
costed scan implementation. The implementation may reorder every segment
visible to the statement snapshot, but must never prune a segment or stop the
scan early: the upper TopN remains the semantic owner of LIMIT/OFFSET. Its
proof must cover concurrent rowset publication, deleted versions, segments
without usable statistics, and stable ordering among equal or unknown bounds.

Acceptance requires the bulk-delete visibility regression to retain the
visible maximum id (`19999`) and a multi-segment TopN test in which the newest
visible value is outside the statistically preferred first segment.

## Runtime filters across shared CTEs

Represent a cross-CTE runtime filter as an `AuxiliaryPlanRegion` jointly owned
by the build, the materialized producer, and every consuming CTE reference.
The region must choose one consistent producer strategy and must either attach
the filter to every semantically eligible reference or decline the optional
facet. It may not be synthesized by following a single `CTERef` to a scan.

Acceptance requires multiple references with different local predicates,
recursive/nested CTE boundaries, and outer-join preserved-side cases. The
physical verifier must reject an artifact whose complete producer/consumer
ownership set is absent.
