// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use std::fs::File;
use std::io::Write;

use goldenfile::Mint;
use paro_parser::parser_testing::token::*;
use paro_parser::Result;

#[path = "support/common.rs"]
mod common;

use common::GOLDEN_ROOT;

fn run_lexer(file: &mut File, source: &str) {
    let tokens = Tokenizer::new(source).collect::<Result<Vec<_>>>();
    match tokens {
        Ok(tokens) => {
            let tuples: Vec<_> = tokens
                .into_iter()
                .map(|token| (token.kind, token.text(), token.span))
                .collect();
            writeln!(file, "---------- Input ----------").unwrap();
            writeln!(file, "{}", source).unwrap();
            writeln!(file, "---------- Output ---------").unwrap();
            writeln!(file, "{:?}", tuples).unwrap();
            writeln!(file, "\n").unwrap();
        }
        Err(err) => {
            let report = err
                .display_with_source(source)
                .to_string()
                .trim()
                .to_string();
            writeln!(file, "---------- Input ----------").unwrap();
            writeln!(file, "{}", source).unwrap();
            writeln!(file, "---------- Output ---------").unwrap();
            writeln!(file, "{}", report).unwrap();
            writeln!(file, "\n").unwrap();
        }
    }
}

#[test]
fn test_lexer() {
    let mut mint = Mint::new(GOLDEN_ROOT);
    let mut file = mint.new_goldenfile("lexer.txt").unwrap();

    let cases = vec![
        r#""#,
        r#"$$ab$cd$$  $$ab$$"#,
        r#"x'deadbeef' -- a hex string\n 'a string literal\n escape quote by '' or \\\'. '"#,
        r#"'中文' '日本語'"#,
        r#"@abc 123"#,
        r#"42 3.5 4. .001 5e2 1.925e-3 .38e+7 1.e-01 0xfff x'deedbeef'"#,
        // select /*+ x          */ 1
        r#"select /*+ x /* yy */ */ 1"#,
        // select                */ 1
        r#"select /* x /*+ yy */ */ 1"#,
        r#"select 1 + /*+ foo"#,
        r#"select 1 /*+ foo"#,
        r#"select /*++  */ /*++ abc x*/ /*+ SET_VAR(timezone='Asia/Shanghai') */ 1;"#,
        r#"select /* the user name */ /*+SET_VAR(timezone='Asia/Shanghai') */ 1;"#,
        r#"create view v_t as select /*+ SET_VAR(timezone='Asia/Shanghai') */ 1;"#,
        r#"create table "user" (id int, name varchar /* the user name */);"#,
    ];

    for case in cases {
        run_lexer(&mut file, case);
    }
}

#[test]
fn test_lexer_error() {
    let mut mint = Mint::new(GOLDEN_ROOT);
    let mut file = mint.new_goldenfile("lexer-error.txt").unwrap();

    let cases = vec![
        r#"select †∑∂ from t;"#,
        r#"select /* x  1"#,
        // Test fullwidth comma (Chinese comma)
        "INSERT INTO items VALUES (1，'[1,2,3]');",
    ];

    for case in cases {
        run_lexer(&mut file, case);
    }
}
