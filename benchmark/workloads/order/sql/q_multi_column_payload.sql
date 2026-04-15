SELECT id
FROM bench_order_payload
ORDER BY priority DESC, tie_break ASC, id ASC
LIMIT 12;
