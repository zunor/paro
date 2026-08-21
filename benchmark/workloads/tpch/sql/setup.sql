-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

DROP TABLE IF EXISTS lineitem;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS partsupp;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS part;
DROP TABLE IF EXISTS nation;
DROP TABLE IF EXISTS region;

SET threads = ${thread_count};
SET memory_limit = '${memory_limit}';
SET rowset_scan_pushdown = ${rowset_scan_pushdown};

-- TPC-H declares logical keys, but an analytical baseline must not pay for
-- OLTP enforcement indexes. UNIQUE NOT ENFORCED records the data generator's
-- guarantee for join cardinality and elimination while keeping every table
-- uniformly heap-backed.
CREATE TABLE region (
    r_regionkey INTEGER,
    r_name VARCHAR,
    r_comment VARCHAR,
    UNIQUE (r_regionkey) NOT ENFORCED
);

CREATE TABLE nation (
    n_nationkey INTEGER,
    n_name VARCHAR,
    n_regionkey INTEGER,
    n_comment VARCHAR,
    UNIQUE (n_nationkey) NOT ENFORCED
);

CREATE TABLE supplier (
    s_suppkey BIGINT,
    s_name VARCHAR,
    s_address VARCHAR,
    s_nationkey INTEGER,
    s_phone VARCHAR,
    s_acctbal DECIMAL(15, 2),
    s_comment VARCHAR,
    UNIQUE (s_suppkey) NOT ENFORCED
);

CREATE TABLE customer (
    c_custkey BIGINT,
    c_name VARCHAR,
    c_address VARCHAR,
    c_nationkey INTEGER,
    c_phone VARCHAR,
    c_acctbal DECIMAL(15, 2),
    c_mktsegment VARCHAR,
    c_comment VARCHAR,
    UNIQUE (c_custkey) NOT ENFORCED
);

CREATE TABLE part (
    p_partkey BIGINT,
    p_name VARCHAR,
    p_mfgr VARCHAR,
    p_brand VARCHAR,
    p_type VARCHAR,
    p_size INTEGER,
    p_container VARCHAR,
    p_retailprice DECIMAL(15, 2),
    p_comment VARCHAR,
    UNIQUE (p_partkey) NOT ENFORCED
);

CREATE TABLE partsupp (
    ps_partkey BIGINT,
    ps_suppkey BIGINT,
    ps_availqty BIGINT,
    ps_supplycost DECIMAL(15, 2),
    ps_comment VARCHAR,
    UNIQUE (ps_partkey, ps_suppkey) NOT ENFORCED
);

CREATE TABLE orders (
    o_orderkey BIGINT,
    o_custkey BIGINT,
    o_orderstatus VARCHAR,
    o_totalprice DECIMAL(15, 2),
    o_orderdate DATE,
    o_orderpriority VARCHAR,
    o_clerk VARCHAR,
    o_shippriority INTEGER,
    o_comment VARCHAR,
    UNIQUE (o_orderkey) NOT ENFORCED
);

CREATE TABLE lineitem (
    l_orderkey BIGINT,
    l_partkey BIGINT,
    l_suppkey BIGINT,
    l_linenumber BIGINT,
    l_quantity DECIMAL(15, 2),
    l_extendedprice DECIMAL(15, 2),
    l_discount DECIMAL(15, 2),
    l_tax DECIMAL(15, 2),
    l_returnflag VARCHAR,
    l_linestatus VARCHAR,
    l_shipdate DATE,
    l_commitdate DATE,
    l_receiptdate DATE,
    l_shipinstruct VARCHAR,
    l_shipmode VARCHAR,
    l_comment VARCHAR,
    UNIQUE (l_orderkey, l_linenumber) NOT ENFORCED
);

COPY region FROM '${data_dir}/region.tbl' WITH (FORMAT csv, DELIMITER '|');
COPY nation FROM '${data_dir}/nation.tbl' WITH (FORMAT csv, DELIMITER '|');
COPY supplier FROM '${data_dir}/supplier.tbl' WITH (FORMAT csv, DELIMITER '|');
COPY customer FROM '${data_dir}/customer.tbl' WITH (FORMAT csv, DELIMITER '|');
COPY part FROM '${data_dir}/part.tbl' WITH (FORMAT csv, DELIMITER '|');
COPY partsupp FROM '${data_dir}/partsupp.tbl' WITH (FORMAT csv, DELIMITER '|');
COPY orders FROM '${data_dir}/orders.tbl' WITH (FORMAT csv, DELIMITER '|');
COPY lineitem FROM '${data_dir}/lineitem.tbl' WITH (FORMAT csv, DELIMITER '|');
