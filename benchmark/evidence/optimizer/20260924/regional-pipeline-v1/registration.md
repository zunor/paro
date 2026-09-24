# regional-program-v4 exploratory pilot

Retain v1-v3, including failed v2. v3 showed that normalizing domains before
aggregate-region recognition removed the structural input that the recognizer
needs. v4 establishes aggregate alternatives first, then normalizes both before
join enumeration. No search budget or execution changes; eager grant coverage.

Same-source regional vs quality, Q11/Q04/Q74, two fresh processes per arm/query,
two ABBA warm rounds, independent diagnostic cohort; 4 threads/2GB, verifier
off, binary pgwire, trace-off normal, compile-work observer on, 30s optional
deadline, 60s statement timeout. Same immutable inputs and DuckDB 1.5.5 identity
as v1-v3, attested anew by the maintained harness. Stop on result error; inspect
>10% regressions before considering production. Maximum 2 MiB per RunOutput.
Exploratory, not a certification cohort; ambient host workload is not isolated.
