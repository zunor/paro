# Strict Clippy cleanup

This is a build-gate cleanup, not an optimizer performance change. No lint
allow-list or assertion is weakened. Production invocation inputs are retained:
binding namespace, native input sizes, exact subproblem/recipe, selected quality
request, settlement identity, and selected TopN input evidence are grouped at
their existing call boundaries. The quality DAG visitor keeps one accumulator.
The large owned proposal variant owns a boxed plan; rule ordering and all
candidate admission remain unchanged. Test-only helpers are test-only; wrappers
with no callers are removed.

`cargo clippy --workspace --all-targets --locked -- -D warnings` passed after
the pre-existing blockers were removed (including the subsequently exposed
optimizer lints). Logs retain intermediate failed builds as well as the final
run. The repository's existing lint configuration is unchanged; no new
suppression is added. Full clean-source validation is recorded in the C2 gate
manifest, not inferred from this lint pass.
