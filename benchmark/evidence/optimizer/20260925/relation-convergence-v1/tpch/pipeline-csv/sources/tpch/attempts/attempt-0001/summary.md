# Paro Benchmark Report

**Date**: 2026-09-25T03:38:57+08:00  |  **Commit**: f96d5f76a  |  **Branch**: re-op

## tpch

| Query | P50 (ms) | P99 (ms) | P999 (ms) | QPS | RSS Peak | Validation | Plan Guard | Explain |
|-------|----------|----------|-----------|-----|----------|------------|------------|---------|
| q01_pricing_summary | 125.60 | 125.60 | 125.60 | 7.96 | - | FAIL | SKIP | SKIP |
| q02_minimum_cost_supplier | 11.92 | 11.92 | 11.92 | 83.89 | - | FAIL | SKIP | SKIP |
| q03_shipping_priority | 14.71 | 14.71 | 14.71 | 68.00 | - | PASS | SKIP | SKIP |
| q04_order_priority_checking | 29.81 | 29.81 | 29.81 | 33.54 | - | PASS | SKIP | SKIP |
| q05_local_supplier_volume | 15.80 | 15.80 | 15.80 | 63.29 | - | PASS | SKIP | SKIP |
| q06_forecast_revenue | 5.28 | 5.28 | 5.28 | 189.44 | - | PASS | SKIP | SKIP |
| q07_volume_shipping | 16.99 | 16.99 | 16.99 | 58.85 | - | PASS | SKIP | SKIP |
| q08_national_market_share | 9.74 | 9.74 | 9.74 | 102.70 | - | PASS | SKIP | SKIP |
| q09_product_type_profit | 76.49 | 76.49 | 76.49 | 13.07 | - | PASS | SKIP | SKIP |
| q10_returned_item_reporting | 56.56 | 56.56 | 56.56 | 17.68 | - | FAIL | SKIP | SKIP |
| q11_important_stock_identification | 4.59 | 4.59 | 4.59 | 218.04 | - | PASS | SKIP | SKIP |
| q12_shipping_modes_priority | 18.89 | 18.89 | 18.89 | 52.94 | - | PASS | SKIP | SKIP |
| q13_customer_distribution | 110.73 | 110.73 | 110.73 | 9.03 | - | FAIL | SKIP | SKIP |
| q14_promotion_effect | 6.81 | 6.81 | 6.81 | 146.86 | - | PASS | SKIP | SKIP |
| q15_top_supplier | 7.84 | 7.84 | 7.84 | 127.62 | - | FAIL | SKIP | SKIP |
| q16_parts_supplier_relationship | 20.96 | 20.96 | 20.96 | 47.72 | - | PASS | SKIP | SKIP |
| q17_small_quantity_order_revenue | 270.82 | 270.82 | 270.82 | 3.69 | - | PASS | SKIP | SKIP |
| q18_large_volume_customer | 383.12 | 383.12 | 383.12 | 2.61 | - | PASS | SKIP | SKIP |
| q19_discounted_revenue | 18.89 | 18.89 | 18.89 | 52.94 | - | PASS | SKIP | SKIP |
| q20_potential_part_promotion | 85.90 | 85.90 | 85.90 | 11.64 | - | FAIL | SKIP | SKIP |
| q21_suppliers_who_kept_orders_waiting | 55.04 | 55.04 | 55.04 | 18.17 | - | PASS | SKIP | SKIP |
| q22_global_sales_opportunity | 22.60 | 22.60 | 22.60 | 44.26 | - | PASS | SKIP | SKIP |
