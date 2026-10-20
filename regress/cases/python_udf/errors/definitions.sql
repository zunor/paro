-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @control connect user=paro
CREATE FUNCTION py_bad_signature(a INTEGER) RETURNS INTEGER LANGUAGE python HANDLER 'main'
AS $$
def main(a):
    return a
$$;

CREATE FUNCTION py_async(a INTEGER) RETURNS INTEGER LANGUAGE python HANDLER 'main'
AS $$
async def main(ctx, a):
    return a
$$;

CREATE FUNCTION py_dynamic_import(a INTEGER) RETURNS INTEGER LANGUAGE python
IMPORTS ('/tmp/should-not-be-loaded.py') AS $$return a$$;

CREATE FUNCTION py_unsupported_profile(a INTEGER) RETURNS INTEGER LANGUAGE python
CAPABILITY PROFILE sandbox_wasm AS $$return a$$;
