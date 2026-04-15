EXPLAIN
SELECT o.id
FROM (VALUES (1, 10, 6), (2, 20, 5), (3, 30, 2)) AS o(id, grp, threshold)
WHERE EXISTS (
  SELECT 1
  WHERE EXISTS (
    SELECT 1
    FROM (
      SELECT (
        SELECT d.score
        FROM (VALUES (10, 4), (10, 9), (20, 5), (30, 3)) AS d(grp, score)
        WHERE d.grp = o.grp
      ) AS top_score
    ) AS nested
    WHERE nested.top_score >= o.threshold
  )
);
