# Optimizer cost calibration inputs

The shipped `optimizer_cost_model.toml` remains **bootstrap**. Its numerical
coefficients are not fitted, and its coverage queries are not independent
operator measurements. `[fitting]` separates inactive work classes from active
classes whose required execution fixtures are still uncovered. Neither category
may be presented as a fully measured bundle. The unused blocking-merge parameter
also remains explicitly inactive. Activating a class requires reviewing its
production work consumer and this eligibility contract together.

`tools/ci/optimizer_calibration_receipt.py` validates experimental fit inputs.
It reuses measurements collected elsewhere; it introduces no timer or fitter.
A dirty candidate may use snapshot source attestation without changing the
shipped model or its provenance:

```sh
python3 tools/ci/optimizer_calibration_receipt.py \
  --receipt benchmark/runs/<run-id>/fit-receipt.json \
  --sha256 <receipt-sha256> --classes 1,2,16,17
```

This reports experimental input validity, `artifact_binding: Uncovered`, and
`source_coverage: RecordedSubset`.
The current global `measured` provenance is intentionally blocked by inactive
classes: this input validator is not a production promotion path. Future
promotion requires a per-class or mixed-provenance artifact contract, separate
parallelism receipts and checks for all published resource/risk coefficients;
alternatively every class must first gain a real production consumer. The
generator's latent receipt check only binds latency expectations and is not
sufficient for that promotion. Validation does not bless a performance baseline,
certify parity, or prove that a binary was built from a given source tree.

## Receipt version 1

All file references are repository-relative. Each reference is
`{"file": "...", "sha256": "<lowercase SHA-256>"}`; validators read and hash the
referenced contents. Raw exploration belongs in ignored `benchmark/runs/`.
Any future measured artifact needs accessible, durable, compact fit inputs;
an unavailable local run is not verified evidence.

A receipt has:

- `schema_version: 1`, `source`, and `binary_sha256`.
- `units: {time: "ns", latency: "reference-normalized", reference_id,
  reference_ns}`. The reference is a declared fixed DOP=1 fixture. Its retained
  measured samples must have median `reference_ns`. Coefficients in
  `coefficients_ns` are converted to artifact latency units by dividing by this
  reference. This scale does not silently convert existing bootstrap costs.
- `columns: [{name, class_id, feature_unit}, ...]` and `coefficients_ns` in the
  same order. Feature units are model rows/pairs/groups/slots/lookups, 32-byte
  blocks, or modeled comparison proxies; `N log N` is not an actual comparison
  counter. A nuisance intercept has `class_id: null, feature_unit: "invocation"`.
- `runs: [<file references>]`, `split: {train: [cell IDs], holdout: [cell IDs]}`,
  and `holdout_gate: {max_relative_error}`. Every retained cell belongs to one
  split. Fixture/scale/seed split keys must be disjoint; splitting repeated
  samples from one case is not a holdout.

Each run repeats `schema_version`, `source`, `binary_sha256`, `units`, `columns`
and `holdout_gate`, and contains `cells: [{id, split_key, status: "Completed",
features: [...], samples_ns: [...]}]`. It records a `timer_scope` with
`kind: "operator_work"`, `admitted_workers: 1`, `observed_workers: 1`,
`profile_mode: "off"`, and nonempty, unique `includes` and `excludes` lists.
These lists must classify exactly `runtime_init`, `consume`, `merge`, `finish`,
`publication`, `drop`, and `input_construction`, with no overlap or omitted
phase. `input_construction` must be excluded. All runs in one fit must use the
same classification; list order does not matter. For example, a consume/merge/
finish timer includes those three phases and excludes runtime initialization,
publication, drop, and input construction explicitly. At least one run contains
`reference_measurement: {id: <reference_id>, samples_ns: [...]}`. Parallel
span/scaling measurements need a separate
contract; these serial inputs cannot identify worker efficiency.

`source` uses `mode: "git"` or `mode: "snapshot"`, a full `git_commit`, and a
`manifest` file reference. Snapshot mode also records `working_tree_sha256`.
The manifest declares `scope: "recorded-source-subset"`, contains `identity`
(source fields except `manifest`) and
`files: [{path, sha256, snapshot_file?}]`. Git mode checks each commit blob;
snapshot mode checks the captured file contents, including untracked files.
Cargo.toml, Cargo.lock and rust-toolchain.toml are mandatory. The validator
checks the recorded subset contents and run association, but cannot infer
omitted source files or reconstruct a binary. `working_tree_sha256` is matched
to the manifest and runs as an attested identity; this validator does not
recompute an existing collector's working-tree digest algorithm. Full build
reproducibility remains uncovered even when all recorded hashes match.

Validation rejects non-finite/negative features and coefficients, nonpositive
samples, failed cells, identity/hash mismatches, inactive classes, rank-deficient
training matrices and holdout errors above the recorded gate. Rank is recomputed
after column normalization with tolerance `1e-10`. Predictions and errors are
recomputed from coefficients and median observed samples; a reported `rank` or
`pass` flag is never trusted. Full rank alone does not establish good conditioning,
causal attribution, uncertainty coverage or out-of-corpus model quality.

The existing SQL/Divan collectors do not yet emit this complete fit-input schema.
Follow-up collector work must preserve typed RF feature metadata, add the missing
execution fixtures and validate actual thread settings. Do not parse benchmark
IDs or cumulative EXPLAIN timelines into independent operator measurements.
