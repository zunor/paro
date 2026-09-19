-- Independent sufficient statistics for Q39's floating aggregates.
-- Integer arithmetic avoids reusing either engine's stddev implementation.
SELECT inv_warehouse_sk AS warehouse_key, inv_item_sk AS item_key, d_moy AS month_key,
       count(inv_quantity_on_hand) AS n,
       sum(cast(inv_quantity_on_hand AS BIGINT)) AS sum_q,
       sum(cast(inv_quantity_on_hand AS BIGINT) * cast(inv_quantity_on_hand AS BIGINT)) AS sum_q2
FROM inventory JOIN date_dim ON inv_date_sk = d_date_sk
WHERE d_year = 2001 AND d_moy IN (1, 2)
GROUP BY inv_warehouse_sk, inv_item_sk, d_moy
ORDER BY warehouse_key, item_key, month_key;
