**Paro SQL Parser**

A PostgreSQL-first SQL parser for Paro, with token-level entrypoints and Paro-specific extensions where the engine still needs them.

---

**Technical Stack**:
- **Lexer**: [logos](https://crates.io/crates/logos) - Fast lexer generator
- **Parser**: [nom](https://crates.io/crates/nom) + [pratt](https://crates.io/crates/pratt) - Recursive descent with Pratt parsing for expressions

**Highlights**:
- String-level and token-level parse APIs: `parse`, `parse_one`, `parse_expr`, `parse_tokens`, `parse_one_tokens`, `parse_expr_tokens`
- Multi-statement parsing that preserves trailing `FORMAT` clauses through `StatementWithFormat`
- PostgreSQL-oriented grammar with retained Paro legacy statements where compatibility still matters
- Internal `parser_testing` facade for integration tests and benchmarks without reopening the whole parser module tree

**Structure**:
```
src/
├── ast/           # AST node definitions (expr, statement, query, etc.)
├── parser/        # Parser implementation (lexer, expr, query, statement)
├── span.rs        # Source location tracking
├── visitor.rs     # AST visitor patterns
└── lib.rs         # Public API
```

**Usage**:

### Single Statement
```rust
use paro_parser::parse_one;

let sql = "SELECT a, b FROM users WHERE id > 10";
let statement = parse_one(sql).unwrap();
println!("{}", statement.stmt);
```

### Multiple Statements
```rust
use paro_parser::parse;

let sql = "SELECT 1; INSERT INTO t VALUES (2); SELECT 3;";
let stmts = parse(sql).unwrap();

for stmt in stmts {
    println!("Parsed: {}", stmt.stmt);
    if let Some(format) = stmt.format {
        println!("FORMAT: {}", format);
    }
}
```

**Key Features (PostgreSQL Optimized)**:
- **Multi-Statement Support**: `parse()` 一次性解析包含多条语句的 SQL 字符串，返回语句列表，兼容 `;`、dollar-quoted 以及复杂脚本。
- **PG Dialect Support**: Correctly handles dollar-quoted strings (`$$`), procedural code blocks, and consecutive semicolons (`;;;`).
- **Excellent Error Reports**: Precise error location and smart keyword suggestions even in multi-statement flows.

---

## License

Licensed under [Apache License 2.0](../../LICENSE).

**Attribution**: Parts of this crate are derived from [Databend](https://github.com/datafuselabs/databend), Copyright 2021 Datafuse Labs, and remain available under Apache License 2.0.
