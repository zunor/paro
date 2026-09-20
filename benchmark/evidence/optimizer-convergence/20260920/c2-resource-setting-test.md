# RESET memory envelope contract

`system/memory` now captures the connection's initial `memory_limit` before
changing it. RESET must restore that exact value. The old expected 1 GB was
not a server-independent contract: the correctness server explicitly declares
2 GB. This does not hide a setting or remove its assertion; the intermediate
explicit 1 GB setting is still compared exactly.

The revised SQL passed on the attested release binary with a 2 GB server,
four threads and optimizer verification on (`c2-v3-memory-2gb.log`). The
expected final boolean is justified by RESET semantics, not by copied output.
`transaction_settings_savepoint` remains independently open: its complete
settings snapshot must account for verifier and read-only diagnostic fields.
No settings rows are filtered out by this change.
