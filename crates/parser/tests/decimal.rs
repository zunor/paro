// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use ethnum::i256;
use paro_parser::ast::Literal;
use paro_parser::parser_testing::expr::parse_float;
use paro_parser::parser_testing::expr::parse_uint;

#[test]
fn test_decimal() {
    let cases = [
        (
            "1.1".to_string(),
            Literal::Decimal256 {
                value: 11.into(),
                precision: 76,
                scale: 1,
            },
        ),
        (
            "1.1e2".to_string(),
            Literal::Decimal256 {
                value: 110.into(),
                precision: 76,
                scale: 0,
            },
        ),
        (
            "1.1e-3".to_string(),
            Literal::Decimal256 {
                value: 11.into(),
                precision: 76,
                scale: 4,
            },
        ),
        (
            "0.".to_string(),
            Literal::Decimal256 {
                value: 0.into(),
                precision: 76,
                scale: 0,
            },
        ),
    ];

    for (i, (s, l)) in cases.iter().enumerate() {
        let r = parse_float(s);
        assert_eq!(Ok(l.clone()), r, "{i}: {s}");
    }
}

#[test]
fn test_decimal_uint() {
    let min_decimal256 = i256::from(u64::MAX) + 1;
    let float_str = "1".to_string() + &vec!["0"; 76].join("");
    let cases = [
        ("1".to_string(), Literal::UInt64(1)),
        (u64::MAX.to_string(), Literal::UInt64(u64::MAX)),
        (
            min_decimal256.to_string(),
            Literal::Decimal256 {
                value: min_decimal256,
                precision: 76,
                scale: 0,
            },
        ),
        (float_str, Literal::Float64(1E76_f64)),
    ];

    for (i, (s, l)) in cases.iter().enumerate() {
        let r = parse_uint(s, 10);
        assert_eq!(Ok(l.clone()), r, "{i}: {s}");
    }
}
