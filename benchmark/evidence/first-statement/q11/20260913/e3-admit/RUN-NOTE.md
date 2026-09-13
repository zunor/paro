# Contaminated launch window and fixed repeat

The independent counter agent confirmed its previously launched optimizer test
compile (cargo12152/rustc12153) remained running after the main agent announced
the serial performance window. It was interrupted and confirmed exited130 before
the stream-b/control-b reports. Exact overlap with control-a's individual timed
SELECTs cannot be reconstructed; the process launch window was not isolated.
Control-a processes12161/12168 were ready at04:54:47.911/04:54:50.156 UTC.

Do not infer interference magnitude from slow values or delete them. Retain the
entire original control-a/stream-a/stream-b/control-b campaign as a launch-window
compromised pilot (including valid unaffected samples), with no clean causal gate
claim from it. Before seeing replacement data, repeat the complete same fixed
control2→stream2→stream2→control2 sequence once, suffix r-. All agent compilation
is explicitly paused, process inventory checked before start. No further repeat
based on timing, no fast-sample selection. Scalar and pre-touch schedules remain
as registered. This is a protocol failure correction, not optional stopping.

## Second-occurrence side-channel coverage follow-up

The initial repeated pre-touch queries executed and validated twice, but their
second cache side-channel lookup reported Uncovered: the existing first-miss
helper deliberately rejects two matching occurrence rows. Q11's distinct first
occurrence remains verified in every sample; no target sample is invalidated.
Add an explicit occurrence1 lookup for the second pre-touch only, leaving the
first-target ambiguity rejection unchanged and tested. Then collect two control
and two stream pre-touch blocks with existing E1 scalars enabled, on the separate
counter-instrumented committed binary. Record second execution fills/image/cache
and both timings. Do not pool these scalar observations with earlier trace-off
timing or invent zero fills for the uncovered earlier second lookup. This fixed
coverage follow-up is registered before its measurements.
