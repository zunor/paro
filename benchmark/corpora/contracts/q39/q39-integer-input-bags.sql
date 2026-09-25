-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Exact input bags for the registered small-integer aggregate oracle.
-- Includes all original joins and grouping identities, not just returned keys.
SELECT w_warehouse_sk AS warehouse_key, i_item_sk AS item_key,
       d_moy AS month_key, inv_quantity_on_hand AS quantity
FROM inventory
JOIN item ON inv_item_sk = i_item_sk
JOIN warehouse ON inv_warehouse_sk = w_warehouse_sk
JOIN date_dim ON inv_date_sk = d_date_sk
WHERE d_year = 2001 AND d_moy IN (1, 2)
ORDER BY warehouse_key, item_key, month_key, quantity;
