# Paro Python UDF SDK

`python/paro_udf/` is the user-facing Python SDK for Paro routines. It exposes the
column abstraction, decorators, Arrow/NumPy-oriented helpers, and a lightweight
testing harness for authoring batch-style handlers locally.

## Scope

This package is responsible for:

- `Column` and batch-oriented value access
- Arrow PyCapsule and NumPy fast-path interop
- decorators such as `@batch_udf`
- local testing helpers used by worker and SDK unit tests

The worker process implementation itself lives in
[`runtimes/python-worker/README.md`](../../runtimes/python-worker/README.md).

## Supported Python Versions

The SDK currently targets `Python >= 3.11`.

Optional extras from `pyproject.toml`:

- `numpy`
- `arrow`

## Running Tests

Run from the repository root:

```bash
PYTHONPATH=python/paro_udf/src:runtimes/python-worker/src \
python3 -m unittest discover -s python/paro_udf/tests -p 'test_*.py' -v
```

## Editable Installs

If you want an editable install for local experimentation:

```bash
python3 -m pip install -e python/paro_udf
```

For the repository test flow, source-based execution via `PYTHONPATH` remains the
default because it matches how CI exercises the package.
