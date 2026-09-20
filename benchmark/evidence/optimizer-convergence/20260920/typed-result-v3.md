# Bound exact result contract v3

This replaces v2 in the comparator, not in historical evidence. Raw captures,
v2 reports and the separately bounded floating-point certificate remain immutable.
No optimizer, budget, cost, handoff or execution setting changes are involved.

## One binding, four verdicts

The pinned DuckDB 1.5.5 parser supplies SQL syntax. A lexical pass supplies quote
provenance only. An explicitly hashed catalog supplies base-column expansion.
CTE/subquery scopes, projection expressions and explicit aliases bind once and
feed identity, wire/type, exact bag and ORDER checks. Unsupported constructs
remain Uncovered; a parser accepting an expression is not a proof of its type.

Explicit aliases retain quoted spelling; unquoted names follow each engine's
identifier contract. A qualified display name is accepted only for the bound
qualifier and column. Derived labels must parse to the projected expression,
without an alias or extra SELECT clauses. No global case/qualifier rewriting
is performed.

Paro wire descriptors are validated independently of DuckDB. Bound integer SUM
lineage permits Paro BIGINT/HUGEINT to correspond to DuckDB HUGEINT. Paro's
HUGEINT uses NUMERIC on the PostgreSQL wire; checked HUGEINT arithmetic uses
DECIMAL(38,0). These mappings require expression evidence and exact value/range
checks, not merely equal sample values. Other widths and scales remain distinct.

Exact numbers use Decimal coefficient/exponent decomposition and integer rational
arithmetic, independent of the Decimal context. Boolean is not integer. The same
representation serves bag keys, ORDER keys and evidence serialization. Digests
identify evidence; actual bag and key-sequence equality decide acceptance.

ORDER accepts bound aliases, ordinals, projected expressions and a limited set
of CASE/comparison/arithmetic expressions over returned columns. It is not a SQL
executor. ASC/DESC and explicit/default NULL placement are retained. GROUPING
must be projected, never inferred from NULL. Duplicate multiplicities and LIMIT
output bags remain exact obligations even when ORDER keys are peers. Hidden keys
that cannot be reconstructed are Uncovered.

## Floating point and replay

No approximate exception is added. Finite floating outputs remain exact in this
contract; non-finite outputs require a separately declared contract. The existing
bounded independent certificate remains separate and must bind the original
capture and inputs; a failing raw exact comparison is not erased by that certificate.

`recheck_result_captures.py` rejects mismatched parser/extension/database/SQL,
unregistered binaries, absent server observations and incomplete capture sets.
It never executes target SQL. Every result set receives all available identity,
schema, own-wire/value, bag and order verdicts. Unsupported binding is explicit.

## Counterexamples

191 benchmark and independent-oracle tests pass, including 38-digit neighbors,
Decimal context changes, scale/range violations, bool/int confusion, aliases
with dots and quote differences, scoped stars, hidden/ambiguous ORDER keys,
GROUPING/NULL separation, wrong order and injected derived-label clauses.
One pre-existing pytest return-value warning remains; it is not suppressed.
