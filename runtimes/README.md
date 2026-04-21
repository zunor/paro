# Runtimes

`runtimes/` contains the language runtime implementations and the shared
protocol/conformance assets used by Paro external routines.

This directory intentionally lives alongside `crates/` and `python/` instead
of under `python/`.

That separation is deliberate:

1. `python/` is for user-facing Python packages and developer SDKs such as
   `python/paro_udf`.
2. `runtimes/` is for execution backends, protocol schemas, worker fixtures,
   and cross-language conformance material.
3. The long-term design already reserves room for non-Python workers, sandboxed
   workers, and remote workers. Nesting everything under `python/` would make
   the repository layout look more Python-specific than the architecture really
   is.

In short: keeping `runtimes/` at the top level is the more elegant long-term
shape for this codebase.

## Directory layout

### `python-worker/`

The reference Python worker runtime.

It owns the Python-side execution loop that talks to the host runtime:

1. maps input/output arenas or receives pre-mapped buffers
2. decodes column batches from the Paro external ABI
3. loads Python handler modules
4. executes handlers with `paro_udf`
5. encodes results back into the output ABI
6. reports completion, cancellation, and structured failures

This is runtime code, not a general-purpose user SDK. User-authored handlers
should depend on `python/paro_udf`, not import internals from this worker.

### `protocol/`

Shared protocol source-of-truth for host ↔ worker communication.

This directory exists because not every protocol artifact naturally belongs in a
Rust crate:

1. the hot-path control header is defined in Rust as a fixed-layout
   `#[repr(C)]` structure
2. rich sideband metadata is defined as language-neutral schemas
3. generated bindings can be emitted for different workers without turning the
   protocol itself into a Python- or Rust-only concept

Today the main contents are FlatBuffers schemas for sideband payloads such as:

1. artifact metadata
2. data-plane descriptors
3. structured error payloads

### `worker-common/`

Shared semantic material for workers across languages.

This directory is intentionally not a Rust runtime library that Python workers
link against directly. Its role is narrower and cleaner:

1. protocol/conformance fixtures
2. recovery and lifecycle test cases
3. ABI descriptor examples
4. golden payloads and reference expectations

That keeps the worker contract reusable without forcing all workers through the
same implementation language.

## Relationship to other top-level directories

### `python/`

`python/` contains user-facing Python packages.

Current example:

1. `python/paro_udf`: the Python SDK used by routine authors

If code is meant to be imported by user handlers or tested as a normal Python
package, it probably belongs under `python/`, not `runtimes/`.

### `crates/`

`crates/` contains the Rust host-side implementation:

1. planning
2. catalog state
3. execution operators
4. external ABI
5. external runtime host

The host runtime in Rust and the workers in `runtimes/` evolve together, but
they are not the same layer.

## What belongs here

Examples of code and assets that fit `runtimes/` well:

1. worker entrypoints and control loops
2. protocol schemas and generated bindings
3. language-specific runtime glue
4. conformance fixtures and recovery corpora
5. runtime-only helper code that should not leak into the public SDK surface

## What should not go here

Examples that usually do not belong in `runtimes/`:

1. end-user helper APIs
2. public Python SDK types for routine authors
3. planner/catalog Rust code
4. SQL regression ownership

Those belong in `python/`, `crates/`, or `regress/` depending on the actual
responsibility.

## Why not move this under `python/`

Moving `runtimes/` under `python/` would make a few local paths look shorter,
but it would weaken the architectural signal:

1. `runtimes/protocol/` is not Python-specific
2. `runtimes/worker-common/` is designed for multi-language reuse
3. future JS/Lua/WASM workers would look misplaced under `python/`
4. sandboxed or remote runtimes are execution concerns, not SDK concerns

If the repository were permanently Python-only, that tradeoff could be
reasonable. Given the current design direction, the top-level `runtimes/`
directory is the cleaner boundary.

## Near-term expectations

As the external routine stack grows, this directory is expected to expand in a
few predictable directions:

1. more worker implementations
2. richer protocol generation tooling
3. deeper conformance coverage
4. sandbox- and remote-runtime-specific helpers

That future growth is another reason to keep `runtimes/` separate from the
public Python package tree.
