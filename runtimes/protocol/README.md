# Runtime Protocol

`protocol/` contains the shared protocol definitions used by the Paro host
runtime and external workers.

The protocol is split by hot-path needs:

1. the latency-sensitive control header is defined in Rust as a fixed-layout
   `#[repr(C)]` structure
2. richer metadata travels through language-neutral sideband schemas
3. generated bindings are outputs, not handwritten source-of-truth

## Source-of-truth

Current authoritative protocol definitions:

1. Rust control header:
   `crates/external_runtime/src/control/header.rs`
2. Sideband schemas:
   `runtimes/protocol/sideband/*.fbs`

## Directory purpose

This directory exists so that shared protocol artifacts do not get trapped
inside a single implementation language.

That matters because:

1. the Rust host consumes the same protocol as Python workers
2. future workers may be implemented in additional languages
3. sideband schemas need a neutral home for code generation and review

## Layout

### `sideband/`

Language-neutral schema files for structured payloads such as:

1. artifact metadata
2. data-plane descriptors
3. structured errors

### `generated/`

Reserved output location for generated bindings.

Files in `generated/` should be treated as build artifacts or generated code
targets, not as the source-of-truth protocol definition.
