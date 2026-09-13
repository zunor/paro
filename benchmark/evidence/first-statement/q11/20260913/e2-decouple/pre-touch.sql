-- Diagnostic only: force consumption of all columns referenced by original Q11.
-- No Q11 joins, filters, GROUP BY shape or fingerprint; four tiny checksum rows.
SELECT source_name, input_rows, CAST(checksum AS BIGINT) AS checksum FROM (
SELECT 'store' AS source_name, count(*) AS input_rows,
       sum(COALESCE(CAST(ss_customer_sk AS BIGINT),0)
         + COALESCE(CAST(ss_sold_date_sk AS BIGINT),0)
         + COALESCE(CAST(ss_ext_list_price * 100 AS BIGINT),0)
         + COALESCE(CAST(ss_ext_discount_amt * 100 AS BIGINT),0)) AS checksum
FROM store_sales
UNION ALL
SELECT 'web', count(*),
       sum(COALESCE(CAST(ws_bill_customer_sk AS BIGINT),0)
         + COALESCE(CAST(ws_sold_date_sk AS BIGINT),0)
         + COALESCE(CAST(ws_ext_list_price * 100 AS BIGINT),0)
         + COALESCE(CAST(ws_ext_discount_amt * 100 AS BIGINT),0))
FROM web_sales
UNION ALL
SELECT 'customer', count(*),
       sum(COALESCE(CAST(c_customer_sk AS BIGINT),0)
         + COALESCE(CAST(length(c_customer_id) AS BIGINT),0)
         + COALESCE(CAST(length(c_first_name) AS BIGINT),0)
         + COALESCE(CAST(length(c_last_name) AS BIGINT),0)
         + COALESCE(CAST(length(c_preferred_cust_flag) AS BIGINT),0)
         + COALESCE(CAST(length(c_birth_country) AS BIGINT),0)
         + COALESCE(CAST(length(c_login) AS BIGINT),0)
         + COALESCE(CAST(length(c_email_address) AS BIGINT),0))
FROM customer
UNION ALL
SELECT 'date', count(*),
       sum(COALESCE(CAST(d_date_sk AS BIGINT),0)
         + COALESCE(CAST(d_year AS BIGINT),0))
FROM date_dim
) AS touch_checksum;
