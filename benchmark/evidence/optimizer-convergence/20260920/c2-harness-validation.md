# Harness validation

The SQL regress runner accepts `--optimizer-verify on|off` and reapplies the
explicit setting on every new connection, including reconnect/restart. Omitted
means retain the server default. Correctness runs use `on`. A connection test
checks three independent connections; failure to set the value closes the
connection and propagates the error.

An existing registry-inventory test omitted `explain_logical_ids`, which was
already registered in the baseline normalizer. Its exact expected inventory
now includes that name; the normalizer implementation and all `.result` files
are unchanged. This is fixture drift, not blessing a SQL difference.

Benchmark and regress unit tests must run in separate Python processes:
both historically use a top-level `harness` package. A combined invocation
imports the wrong executor and fails collection; that failed invocation is
retained as runner evidence, not counted as a product failure or a pass.
