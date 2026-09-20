# Transaction settings transcript adjudication

The savepoint test now explicitly declares a 1GB session limit and verifier-on
before enumerating `pg_settings`. Previously its expected output silently
assumed a 1GiB startup limit and verifier-off, neither of which is the integration
runner's contract (2GB server, verifier-on). This does not change server defaults
or filter out resource information. DISCARD ALL still resets session state.

All 28 process-start observations from
`context/src/diagnostic_environment.rs::OBSERVED_ENVIRONMENT` are now represented
by name, observed value and read-only description in expected output. The
verifier/resource fields are not hidden, and the existing setting inventory is
otherwise unchanged. The normal regression server has these environment
variables unset; a diagnostic server with different settings is not this fixture.

The original expected/actual mismatch remains in
`c2-v3-final-regress-report/actuals/txn_transaction_settings_savepoint.sql.actual`.
The new test passed through the real server with the a5881e4d release binary,
FD 65536, four threads, 2GB startup envelope and verifier-on:
`c2-v4-settings-test.log`. It covers SET LOCAL, nested SAVEPOINT release/rollback,
session persistence and DISCARD, not only a static text comparison.
