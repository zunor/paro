EXPLAIN
SELECT o.id,
       (
         SELECT (
           SELECT (
             SELECT d.score
             FROM (VALUES (10, 9), (20, 5), (30, 7)) AS d(grp, score)
             WHERE d.grp = o.grp
           )
         )
       ) AS nested_score
FROM (VALUES (1, 10), (2, 20), (3, 30)) AS o(id, grp);
