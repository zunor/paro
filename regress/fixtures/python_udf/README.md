# Python UDF Fixtures

`regress/fixtures/python_udf/` holds staged inputs for Python UDF SQL cases.

Layout:

- `modules/`: importable Python modules/packages staged into per-run fixture roots
- `data/`: test-only data inputs referenced by SQL cases
- `bin/`: local helper executables used by runner-managed runtime profiles (for example misconfigured or worker-crash shims)

The runner keeps the fixture contract deliberately narrow:

1. Cases declare fixture roots with `-- @fixture <relative-path>`.
2. SQL can reference the staged location with `{{fixture:<relative-path>}}`.
3. Fixture staging only copies files into the per-run report area; it does not bypass
   artifact resolution, worker bootstrap, or any runtime validation step.
