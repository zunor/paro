// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use paro_parser::ast::quote::ident_needs_quote;
use paro_parser::ast::quote::QuotedIdent;

#[test]
fn test_quote() {
    let cases = &[
        ("a", "a"),
        ("_", "_"),
        ("_abc", "_abc"),
        ("_abc12", "_abc12"),
        ("_12a", "_12a"),
        ("12a", "\"12a\""),
        ("a\\\"b", "\"a\\\"\"b\""),
        ("12", "\"12\""),
        ("🍣", "\"🍣\""),
        ("価格", "\"価格\""),
        ("\t", "\"\t\""),
        ("complex \"string\"", "\"complex \"\"string\"\"\""),
        ("\"\"\"", "\"\"\"\"\"\"\"\""),
        ("'''", "\"'''\""),
        ("name\"with\"quote", "\"name\"\"with\"\"quote\""),
    ];

    for (input, expected) in cases {
        if ident_needs_quote(input) {
            let quoted = QuotedIdent(input, '"').to_string();
            assert_eq!(quoted, *expected);

            let QuotedIdent(ident, quote) = quoted.parse().unwrap();
            assert_eq!(ident, *input);
            assert_eq!(quote, '"');
        } else {
            assert_eq!(input, expected);
        };
    }
}
