EXPLAIN
SELECT
    part,
    id,
    ROW_NUMBER() OVER (PARTITION BY part ORDER BY score DESC, id ASC) AS rn
FROM spill_window;
