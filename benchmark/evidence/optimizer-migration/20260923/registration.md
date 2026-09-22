# Final migration pilot registration

Source: clean re-op 9d8674ff (release Rust runtime SHA256
f6e66ce4f888904a49b2882c4c900beab546432a8a46f4f26837395ed06cba98).
Use the same immutable relative SF1 seed, DuckDB 1.5.5 package, SQL, 4 threads,
2 GB, binary protocol and exact result contract as registration.md.

After the monotonic cancellation and typed receipt reader fixes, collect Q11,
Q04 and Q74 with quality policy and verifier on, then Q11 with verifier off.
Each cell has two fresh blocks, one warmup, one ABBA warm round and one separate
diagnostic block. Compilation receipt collection is enabled; normal trace is
off. Statement timeout is 30 seconds. All failures and slow samples remain.
No source changes, builds or other performance campaigns during collection.

This is exploratory migration validation, not a powered parity/causal gate.
The verifier-off arm is declared separately; never pool it with verifier-on
or attribute historical/current differences solely to that setting. Budgeted
v9 results predate the timeout fix and are not reused as final-binary timings.
Preserve v1-v8 collector failures and original v9 negative evidence unchanged.
