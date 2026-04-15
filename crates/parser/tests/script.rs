// Copyright 2024-2026 Zunor
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use goldenfile::Mint;
use paro_parser::parser_testing::script::script_block;
use paro_parser::parser_testing::script::script_stmt;
use paro_parser::parser_testing::ParseMode;

#[path = "support/common.rs"]
mod common;

use common::{run_parser_with_mode, GOLDEN_ROOT};

#[test]
fn test_script() {
    let mut mint = Mint::new(GOLDEN_ROOT);
    let file = &mut mint.new_goldenfile("script.txt").unwrap();

    let cases = &[
        r#"LET cost FLOAT"#,
        r#"LET cost FLOAT default 3.0"#,
        r#"LET cost FLOAT := 100.0"#,
        r#"LET cost := 100.0"#,
        r#"LET t1 RESULTSET := SELECT * FROM numbers(100)"#,
        r#"LET t1 cursor FOR SELECT * FROM numbers(100)"#,
        r#"profit := revenue - cost"#,
        r#"RETURN"#,
        r#"RETURN profit"#,
        r#"RETURN TABLE(t1)"#,
        r#"RETURN TABLE(select count(*) from t1)"#,
        r#"THROW 'Email already exists.'"#,
        r#"
            FOR i IN REVERSE 1 TO maximum_count DO
                counter := counter + 1;
            END FOR label1
        "#,
        r#"
            FOR rec IN resultset DO
                CONTINUE;
            END FOR label1
        "#,
        r#"
            FOR rec IN SELECT * FROM numbers(100) DO
                CONTINUE;
            END FOR label1
        "#,
        r#"
            WHILE counter < maximum_count DO
                CONTINUE label1;
            END WHILE label1
        "#,
        r#"
            REPEAT
                BREAK;
            UNTIL counter = maximum_count
            END REPEAT label1
        "#,
        r#"
            LOOP
                BREAK label1;
            END LOOP label1
        "#,
        r#"
            CASE
                WHEN counter = 1 THEN
                    counter := counter + 1;
                WHEN counter = 2 THEN
                    counter := counter + 2;
                ELSE
                    counter := counter + 3;
            END
        "#,
        r#"
            CASE counter
                WHEN 1 THEN
                    counter := counter + 1;
                WHEN 2 THEN
                    counter := counter + 2;
                ELSE
                    counter := counter + 3;
            END CASE
        "#,
        r#"
            IF counter = 1 THEN
                counter := counter + 1;
            ELSEIF counter = 2 THEN
                counter := counter + 2;
            ELSE
                counter := counter + 3;
            END IF
        "#,
        r#"
            LOOP
                SELECT c1, c2 FROM t WHERE c1 = 1;
            END LOOP
        "#,
        r#"select :a + 1"#,
        r#"select IDENTIFIER(:b)"#,
        r#"select a.IDENTIFIER(:b).c + minus(:d)"#,
        r#"EXECUTE TASK IDENTIFIER(:my_task)"#,
        r#"DESC TASK IDENTIFIER(:my_task)"#,
    ];

    for case in cases {
        run_parser_with_mode(file, script_stmt, ParseMode::Template, case);
    }

    let block_cases = vec![
        r#"
            BEGIN
                LOOP
                    CONTINUE;
                END LOOP;
            END;
        "#,
        r#"
            DECLARE
                x := 1;
            BEGIN
                FOR y in x TO 10 DO
                    CONTINUE;
                END FOR;
            END;
        "#,
    ];

    for case in block_cases {
        run_parser_with_mode(file, script_block, ParseMode::Template, case);
    }
}
