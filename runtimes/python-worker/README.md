# Python Worker

`runtimes/python-worker/` contains the process-oriented Python worker used by Paro's
external routine runtime. This package is intentionally internal: it owns the
control loop, request decoding, result encoding, and module loading contract that
the Rust host speaks over the external runtime protocol.

## Scope

This package is responsible for:

- decoding control-plane submit/cancel messages
- loading staged Python modules and locating handlers
- decoding Paro column batches into worker-side `Column` views
- executing handlers and re-encoding outputs
- serializing structured Python exceptions back to the host

It is not the user-facing SDK. User-authored handler helpers belong in
[`python/paro_udf/README.md`](../../python/paro_udf/README.md).

## Supported Python Versions

The worker currently targets `Python >= 3.11`.

## Running Tests

Run from the repository root:

```bash
PYTHONPATH=python/paro_udf/src:runtimes/python-worker/src \
python3 -m unittest discover -s runtimes/python-worker/tests -p 'test_*.py' -v
```

The tests run from source and do not require a wheel build.

## Local Development Notes

- Keep imports source-relative; the unit tests intentionally exercise the worker without an editable install.
- The worker shares `paro_udf` column and adapter code with the SDK, so `PYTHONPATH` should include both `python/paro_udf/src` and `runtimes/python-worker/src`.
- Protocol conformance fixtures that the host and worker share live under `runtimes/worker-common/`.
