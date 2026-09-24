# Paro Benchmark Report

**Date**: 2026-09-25T03:52:53+08:00  |  **Commit**: ea595ead9  |  **Branch**: re-op

## tpch

| Query | P50 (ms) | P99 (ms) | P999 (ms) | QPS | RSS Peak | Validation | Plan Guard | Explain |
|-------|----------|----------|-----------|-----|----------|------------|------------|---------|
| q01_pricing_summary | 113.08 | 113.08 | 113.08 | 8.84 | - | FAIL | SKIP | SKIP |
| q02_minimum_cost_supplier | 4.31 | 4.31 | 4.31 | 232.11 | - | FAIL | SKIP | SKIP |
| q03_shipping_priority | 12.07 | 12.07 | 12.07 | 82.85 | - | PASS | SKIP | SKIP |
| q04_order_priority_checking | 12.83 | 12.83 | 12.83 | 77.92 | - | PASS | SKIP | SKIP |
| q05_local_supplier_volume | 30.82 | 30.82 | 30.82 | 32.45 | - | PASS | SKIP | SKIP |
| q06_forecast_revenue | 5.00 | 5.00 | 5.00 | 199.84 | - | PASS | SKIP | SKIP |
| q07_volume_shipping | 20.97 | 20.97 | 20.97 | 47.68 | - | PASS | SKIP | SKIP |
| q08_national_market_share | 186.28 | 186.28 | 186.28 | 5.37 | - | PASS | SKIP | SKIP |
| q09_product_type_profit | - | - | - | - | - | FAIL | SKIP | SKIP |
| q10_returned_item_reporting | 52.63 | 52.63 | 52.63 | 19.00 | - | FAIL | SKIP | SKIP |
| q11_important_stock_identification | 10.95 | 10.95 | 10.95 | 91.29 | - | PASS | SKIP | SKIP |
| q12_shipping_modes_priority | 13.27 | 13.27 | 13.27 | 75.39 | - | PASS | SKIP | SKIP |
| q13_customer_distribution | 68.07 | 68.07 | 68.07 | 14.69 | - | FAIL | SKIP | SKIP |
| q14_promotion_effect | 6.01 | 6.01 | 6.01 | 166.46 | - | PASS | SKIP | SKIP |
| q15_top_supplier | 7.67 | 7.67 | 7.67 | 130.35 | - | FAIL | SKIP | SKIP |
| q16_parts_supplier_relationship | 21.31 | 21.31 | 21.31 | 46.93 | - | PASS | SKIP | SKIP |
| q17_small_quantity_order_revenue | 13.05 | 13.05 | 13.05 | 76.63 | - | PASS | SKIP | SKIP |
| q18_large_volume_customer | 177.59 | 177.59 | 177.59 | 5.63 | - | PASS | SKIP | SKIP |
| q19_discounted_revenue | 16.43 | 16.43 | 16.43 | 60.88 | - | PASS | SKIP | SKIP |
| q20_potential_part_promotion | 20.31 | 20.31 | 20.31 | 49.24 | - | FAIL | SKIP | SKIP |
| q21_suppliers_who_kept_orders_waiting | 87.60 | 87.60 | 87.60 | 11.42 | - | PASS | SKIP | SKIP |
| q22_global_sales_opportunity | 9.32 | 9.32 | 9.32 | 107.26 | - | PASS | SKIP | SKIP |
