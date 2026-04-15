// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use goldenfile::Mint;
use paro_parser::parser_testing::query::query;

#[path = "support/common.rs"]
mod common;

use common::{run_parser, GOLDEN_ROOT};

#[test]
fn test_query() {
    let mut mint = Mint::new(GOLDEN_ROOT);
    let file = &mut mint.new_goldenfile("query.txt").unwrap();
    let cases = &[
        r#"select ?"#,
        r#"select * exclude c1, b.* exclude (c2, c3, c4) from customer inner join orders on a = b limit 1"#,
        r#"select columns('abc'), columns(a -> length(a) = 3) from t"#,
        r#"select count(t.*) from t"#,
        r#"select * from customer at(offset => -10 * 30)"#,
        r#"select * from customer changes(information => default) at (stream => s) order by a, b"#,
        r#"select * from customer with consume as s"#,
        r#"select * from t12_0004 at (TIMESTAMP => 'xxxx') as t"#,
        r#"select count(t.c) from t12_0004 at (snapshot => 'xxxx') as t"#,
        r#"select * from customer inner join orders"#,
        r#"select * from customer cross join orders"#,
        r#"select * from customer inner join orders on (a = b)"#,
        r#"select * from customer inner join orders on a = b limit 1"#,
        r#"select * from customer inner join orders on a = b limit 2 offset 3"#,
        r#"select * from customer natural full join orders"#,
        r#"select * from customer natural join orders left outer join detail using (id)"#,
        r#"with t2(tt) as (select a from t) select t2.tt from t2  where t2.tt > 1"#,
        r#"with t2(tt) as materialized (select a from t) select t2.tt from t2  where t2.tt > 1"#,
        r#"with t2 as (select a from t) select t2.a from t2  where t2.a > 1"#,
        r#"with t2(tt) as materialized (select a from t), t3 as materialized (select * from t), t4 as (select a from t where a > 1) select t2.tt, t3.a, t4.a from t2, t3, t4 where t2.tt > 1"#,
        r#"with recursive t2(tt) as (select a from t1 union select tt from t2) select t2.tt from t2"#,
        r#"with t(a,b) as (values(1,1),(2,null),(null,5)) select t.a, t.b from t"#,
        r#"select c_count cc, count(*) as custdist, sum(c_acctbal) as totacctbal
            from customer, orders ODS,
                (
                    select
                        c_custkey,
                        count(o_orderkey)
                    from
                        customer left outer join orders on
                            c_custkey = o_custkey
                            and o_comment not like '%:1%:2%'
                    group by
                        c_custkey
                ) as c_orders
            group by c_count
            order by custdist desc nulls first, c_count asc, totacctbal nulls last
            limit 10, totacctbal"#,
        r#"select * from t1 union select * from t2"#,
        r#"select * from t1 except select * from t2"#,
        r#"select * from t1 union select * from t2 union select * from t3"#,
        r#"select * from t1 union select * from t2 union all select * from t3"#,
        r#"select * from (
                (SELECT f, g FROM union_fuzz_result1
                EXCEPT
                SELECT f, g FROM union_fuzz_result2)
                UNION ALL
                (SELECT f, g FROM union_fuzz_result2
                EXCEPT
                SELECT f, g FROM union_fuzz_result1)
        )"#,
        r#"select * from t1 union select * from t2 intersect select * from t3"#,
        r#"(select * from t1 union select * from t2) intersect select * from t3"#,
        r#"(select * from t1 union select * from t2) union select * from t3"#,
        r#"select * from t1 union (select * from t2 union select * from t3)"#,
        r#"SELECT * FROM ((SELECT *) EXCEPT (SELECT *)) foo"#,
        r#"SELECT * FROM (((SELECT *) EXCEPT (SELECT *))) foo"#,
        r#"SELECT * FROM (SELECT * FROM xyu ORDER BY x, y) AS xyu"#,
        r#"SELECT 'Lowest value sale' AS aggregate, * FROM quarterly_sales PIVOT(MIN(amount) FOR quarter IN (ANY ORDER BY quarter))"#,
        r#"select * from monthly_sales pivot(sum(amount) for month in ('JAN', 'FEB', 'MAR', 'APR')) order by empid"#,
        r#"select * from (select * from monthly_sales) pivot(sum(amount) for month in ('JAN', 'FEB', 'MAR', 'APR')) order by empid"#,
        r#"select * from monthly_sales pivot(sum(amount) for month in (select distinct month from monthly_sales)) order by empid"#,
        r#"select * from (select * from monthly_sales) pivot(sum(amount) for month in ((select distinct month from monthly_sales))) order by empid"#,
        r#"select * from monthly_sales_1 unpivot(sales for month in (jan as '1月', feb 'February', mar As 'MARCH', april)) order by empid"#,
        r#"select * from (select * from monthly_sales_1) unpivot(sales for month in (jan, feb, mar, april)) order by empid"#,
        r#"select * from range(1, 2)"#,
        r#"select sum(a) over w from customer window w as (partition by a order by b)"#,
        r#"select a, sum(a) over w, sum(a) over w1, sum(a) over w2 from t1 window w as (partition by a), w2 as (w1 rows current row), w1 as (w order by a) order by a"#,
        r#"SELECT * FROM ((SELECT * FROM xyu ORDER BY x, y)) AS xyu"#,
        r#"SELECT * FROM (VALUES(1,1),(2,null),(null,5)) AS t(a,b)"#,
        r#"VALUES(1,'a'),(2,'b'),(null,'c') order by col0 limit 2"#,
        r#"select * from t left join lateral(select 1) on true, lateral(select 2)"#,
        r#"select * from t, lateral flatten(input => u.col) f"#,
        r#"select * from flatten(input => parse_json('{"a":1, "b":[77,88]}'), outer => true)"#,
    ];

    for case in cases {
        run_parser(file, query, case);
    }
}

#[test]
fn test_query_error() {
    let mut mint = Mint::new(GOLDEN_ROOT);
    let file = &mut mint.new_goldenfile("query-error.txt").unwrap();
    let cases = &[
        r#"select * from customer join where a = b"#,
        r#"from t1 select * from t2"#,
        r#"from t1 select * from t2 where a = b"#,
        r#"select * from join customer"#,
        r#"select * from customer natural inner join orders on a = b"#,
        r#"select * order a"#,
        r#"select * order"#,
        r#"select number + 5 as a, cast(number as float(255))"#,
        r#"select 1 1"#,
    ];

    for case in cases {
        run_parser(file, query, case);
    }
}
