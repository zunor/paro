# Worker Common

`worker-common/` contains shared semantic material for external workers across
languages.

It is intentionally not a Rust runtime library that the Python worker links
against directly.

## What lives here

This directory is the right place for assets such as:

1. protocol conformance cases
2. recovery and lifecycle fixtures
3. ABI descriptor samples and golden payloads
4. cross-language expectations for worker behavior

## What does not live here

This directory should not become:

1. a second host runtime implementation
2. a Python-worker-only helper package
3. a catch-all bucket for arbitrary test files

Keeping `worker-common/` narrow makes the contract easier to reuse from future
workers without forcing them through a shared implementation language.
