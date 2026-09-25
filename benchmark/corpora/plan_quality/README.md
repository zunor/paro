# Known-cardinality plan quality

`benchmark/corpora/plan_quality.py` checks deliberately small SQL fixtures
against independent cardinality oracles. Discover the current CLI and
`make -C benchmark help` before using `quality-collect` (builds/starts an owned
fresh server) or `quality` (compares reports). Put output in an explicit ignored
run directory. SQL snapshot updates are not quality baselines.

`cases.toml` defines result boundaries and, where needed, uniquely matched
internal plan selectors with separate SQL cardinality oracles. Missing or
ambiguous selectors fail; pipeline IDs are not physical EXPLAIN node IDs.
Those internal counts are independent oracle counts, not measured runtime
counters from a physical operator.

Comparisons require complete raw reports, identical corpus/collector/settings
and explicit source/build identity. The gate rejects each boundary's q-error
increase even if the global tail improves; zero-versus-positive is infinity.
The singleton fixture also checks aggregate count separately from q-error.
Fixture/selector/oracle changes need review, not automatic bless. This is
neither the entire correctness suite nor a timing benchmark.
