**Paro Error System**

A PostgreSQL-compatible structured error handling system with full SQLSTATE support.

---

**Quick Start**

```rust
use paro_common::error;

// Create errors with semantic constructors
let err = error::table_not_found("users");
let err = error::syntax("unexpected token");
let err = error::not_implemented("LATERAL JOIN");
```

---

**Adding Context**

```rust
use paro_common::error;

let err = error::column_not_found("email")
    .table("users")
    .schema("public")
    .hint("Did you mean 'mail'?");
```

---

**Error Identification**

```rust
use paro_common::error::{codes, ErrorClass};

// Exact SQLSTATE match
if err.is(codes::syntax::UNDEFINED_TABLE) {
    // Handle missing table
}

// Pattern matching on error class
match err.error_class() {
    ErrorClass::Syntax => { /* syntax error */ }
    ErrorClass::Constraint => { /* constraint violation */ }
    ErrorClass::Internal => { /* internal error */ }
    _ => {}
}

// Semantic predicates
if err.is_retryable() {
    // Deadlock or serialization failure - can retry
}
```

---

**Common Constructors**

| Constructor | Purpose |
|-------------|---------|
| `syntax(msg)` | Syntax error |
| `table_not_found(name)` | Undefined table |
| `column_not_found(name)` | Undefined column |
| `not_implemented(feat)` | Feature not implemented |
| `internal(msg)` | Internal error |
| `unique_violation(name)` | Unique constraint violation |
| `division_by_zero()` | Division by zero |
| `io(err)` | I/O error |

---

**Error Predicates**

| Method | Description |
|--------|-------------|
| `is(code)` | Exact SQLSTATE match |
| `error_class()` | Get error category |
| `is_syntax_error()` | Syntax class (42) |
| `is_constraint_error()` | Constraint class (23) |
| `is_retryable()` | Serialization/deadlock |
| `is_query_canceled()` | Query was canceled |
| `is_undefined_object()` | Object does not exist |
| `is_duplicate_object()` | Object already exists |

---

**Module Structure**

```
error/
├── mod.rs           Flat API exports
├── error_type.rs    ParoError main type
├── error_class.rs   ErrorClass enum
├── sqlstate.rs      SqlState type
├── codes/           SQLSTATE constants
└── make_*.rs        Convenience constructors
```
