# Paro Benchmark Report

**Date**: 2026-09-25T03:52:20+08:00  |  **Commit**: ea595ead9  |  **Branch**: re-op

## tpch

| Query | P50 (ms) | P99 (ms) | P999 (ms) | QPS | RSS Peak | Validation | Plan Guard | Explain |
|-------|----------|----------|-----------|-----|----------|------------|------------|---------|
| q01_pricing_summary | 113.30 | 113.30 | 113.30 | 8.83 | - | FAIL | SKIP | SKIP |
| q02_minimum_cost_supplier | 15.92 | 15.92 | 15.92 | 62.81 | - | FAIL | SKIP | SKIP |
| q03_shipping_priority | 15.76 | 15.76 | 15.76 | 63.45 | - | PASS | SKIP | SKIP |
| q04_order_priority_checking | 32.38 | 32.38 | 32.38 | 30.89 | - | PASS | SKIP | SKIP |
| q05_local_supplier_volume | 18.31 | 18.31 | 18.31 | 54.63 | - | PASS | SKIP | SKIP |
| q06_forecast_revenue | 7.84 | 7.84 | 7.84 | 127.52 | - | PASS | SKIP | SKIP |
| q07_volume_shipping | 17.18 | 17.18 | 17.18 | 58.21 | - | PASS | SKIP | SKIP |
| q08_national_market_share | 8.85 | 8.85 | 8.85 | 112.98 | - | PASS | SKIP | SKIP |
| q09_product_type_profit | 77.80 | 77.80 | 77.80 | 12.85 | - | PASS | SKIP | SKIP |
| q10_returned_item_reporting | 39.36 | 39.36 | 39.36 | 25.41 | - | FAIL | SKIP | SKIP |
| q11_important_stock_identification | 3.53 | 3.53 | 3.53 | 282.89 | - | PASS | SKIP | SKIP |
| q12_shipping_modes_priority | 13.64 | 13.64 | 13.64 | 73.34 | - | PASS | SKIP | SKIP |
| q13_customer_distribution | 81.36 | 81.36 | 81.36 | 12.29 | - | FAIL | SKIP | SKIP |
| q14_promotion_effect | 6.07 | 6.07 | 6.07 | 164.86 | - | PASS | SKIP | SKIP |
| q15_top_supplier | 6.61 | 6.61 | 6.61 | 151.37 | - | FAIL | SKIP | SKIP |
| q16_parts_supplier_relationship | 21.25 | 21.25 | 21.25 | 47.07 | - | PASS | SKIP | SKIP |
| q17_small_quantity_order_revenue | 230.18 | 230.18 | 230.18 | 4.34 | - | PASS | SKIP | SKIP |
| q18_large_volume_customer | 342.99 | 342.99 | 342.99 | 2.92 | - | PASS | SKIP | SKIP |
| q19_discounted_revenue | 21.08 | 21.08 | 21.08 | 47.43 | - | PASS | SKIP | SKIP |
| q20_potential_part_promotion | 120.89 | 120.89 | 120.89 | 8.27 | - | FAIL | SKIP | SKIP |
| q21_suppliers_who_kept_orders_waiting | 47.23 | 47.23 | 47.23 | 21.17 | - | PASS | SKIP | SKIP |
| q22_global_sales_opportunity | 13.73 | 13.73 | 13.73 | 72.85 | - | PASS | SKIP | SKIP |
