# Q11 first-statement evidence — 2026-09-10

This directory records the post-`360a1428` validation and the D6 diagnostic
campaign. Normal C1 samples and trace-on diagnostic samples remain separate;
diagnostic timings are not added to the C1 cohort.

## Source and measurement identity

- D6 diagnostic source: commit `d39ea555c83becd1a720854ba83b3ad4af70d335`,
  clean working tree (`e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`).
- D6 release binary SHA-256:
  `3b9ecd969be57e149ba4c1faff7d894a766dc8091b766906588b3cfb899d84be`.
- D3-A check source: commit `360a1428c9c3c98e42240729dab9a9d2dbd37cba`,
  clean working tree with the same empty-status hash.
- D3-A release binary SHA-256:
  `f0cbe0e108ed429b9d648599d4369eb3da4bd18406f986dfd92e988e86e40a05`.
- Both Q11 runs used the binary protocol, four execution threads, 2 GiB
  memory, private per-process input copies, verified complete typed results,
  and verified post-timer statement-cache misses. Normal C1 had tracing off;
  diagnostic traces were collected in independent processes.

## D6 diagnostic result

Three independent fresh diagnostic blocks were collected from the same
`d39ea555` binary. The target Q11 operation reported:

- optimizer: `836.958–840.526 ms` (about 84% of the client C1);
- admission/lowering: `0.441–0.476 ms`;
- pipeline initialization: `0.044–0.051 ms`;
- first page: `329.185–340.154 ms`;
- fetch drain: `329.976–340.963 ms`;
- client C1: `1175.088–1183.122 ms`.

The three blocks are sufficient to reject a one-off timing explanation for the
optimizer share, but not to claim a stable execution lower bound. The trace
does not identify a Q11-only scan/decode/RF fast path; D6 therefore remains a
measurement-led execution track, not a query-specific optimization.

## D3-A ReadSet check

`q11-d3a-readset-v1.json` is a two-block normal trace-off check after the
single-group `ReadSet` construction fast path. It preserved semantic and cold
miss validation, but did not produce a detectable C1 improvement:

- Paro C1 median: `1192.661812 ms`;
- DuckDB C1 median: `112.804937 ms`;
- Paro/DuckDB ratio: `10.573470`, 95% CI `[10.510648, 10.636667]`;
- W ratio: `2.727515`.

The change is retained as a unit-cost reduction with no performance claim.

## SQL regression and FD diagnosis

The first rerun at a soft limit of 16384 reproduced `Too many open files` in
the full 184-case flow. A second run raised only the process soft limit to
60000, used a fresh data directory, and sampled the server FD count every
250 ms. It completed:

`184 passed, 0 failed, 0 skipped, 0 new` in `68.27s`.

The server FD samples were `15` initially, `122` at peak, and `122` at the
last sample. There was no monotonic growth. Together with the earlier bounded
high-FD campaign, this classifies the reproduced failure as a test-capacity /
FD-envelope problem; it does not prove every resource-lifecycle path leak-free.
Both the failing 16384 run and the passing 60000 run are retained in the
temporary logs referenced by the task discussion; the passing run, report,
server log, and FD samples are archived here.

## Archived files

- `q11-d6-cold-samples-v1.json` and three diagnostic server logs: D6 phase
  attribution, excluded from C1.
- `q11-d3a-readset-v1.json` and its diagnostic log: D3-A C1/diagnostic check.
- `sql-regress-run-20260910.txt`: complete high-limit runner output and server
  log; `sql-regress-report-20260910.txt`, `sql-regress-error-20260910.txt`,
  and `sql-regress-log-20260910.txt`: suite artifacts.
- `sql-regress-fd-samples-20260910.txt`: raw FD samples.
- `SHA256SUMS.txt`: hashes for every archived artifact.

Neither D6 nor D3-A changes the C1 gate: Q11 is still far from DuckDB and M1–M3
remain unpassed.

## D2 native-cost bridge check — 2026-09-10

The `7c96a433` build removes the last shallow `OwnedLogicalPlan` compatibility
view from closed native staging. Operator implementation admission, local cost,
and cost facts now consume the native operator plus child `NodeState` facts;
the owned settlement path remains unchanged for real scan/search leaves and
source-lineage-dependent capabilities. Native cost facts intentionally leave
source-lineage fields unknown until a boundary adapter proves them, so this
batch does not enable runtime-filter lineage or other source-sensitive optional
paths from a native shell.

The release binary and source were clean and matched the report. A five-block
normal trace-off C1 comparison with the binary protocol, four execution
threads, 2 GiB, private per-process data copies, and complete result
verification produced:

- Paro C1 median `1162.751666 ms`; DuckDB `108.201250 ms`;
- fresh-block ratio `10.818597`, 95% CI `[10.685760, 10.987346]`;
- normal trace-off W ratio `2.728626`;
- diagnostic optimizer `840.093 ms`, lower/admit `0.499 ms`, pipeline init
  `0.049 ms`, first page `329.111 ms`, and fetch drain `329.893 ms`.

The C1 result is a valid fresh-process measurement and shows a lower absolute
Paro time than the preceding 90-row check, but it is still far from parity and
does not pass M1–M3. The diagnostic cohort remains excluded from C1. The
report and block/diagnostic logs are archived as `q11-d2-native-cost-v1.*` in
this directory.

## SQL regression rerun — default memory, high FD limit — 2026-09-10

The earlier `Too many open files` reproduction was rerun from a fresh data
directory with the default server memory limit (`1073741824` bytes) and only
the process soft `nofile` limit raised to `60000`. The complete suite passed:

`184 passed, 0 failed, 0 skipped, 0 new` in `65.35s`.

The FD sampler tracked the server across runner-controlled restarts. New
server PIDs began around `13–18` descriptors; the longest observed lifetimes
grew from `14→123`, `15→253`, and `17→405` descriptors while data-heavy cases
were active. This is bounded per observed run and is consistent with the
tablet/index workload exhausting a low process capacity envelope; it is not a
monotonic-leak proof. The default-memory semantic rerun therefore closes the
configuration confounder, while resource-lifecycle leak analysis remains a
separate follow-up if a lower limit still reproduces the failure.

The `-v2` artifacts are the exact current rerun: report, empty error file,
runner log, and raw FD samples. The server was shut down after the run.
