EXPLAIN
SELECT id
FROM (VALUES (1), (2), (6)) AS t(id)
WHERE id IN (1, 2, 3, 4, 5);

SELECT id
FROM (VALUES (1), (2), (6)) AS t(id)
WHERE id IN (1, 2, 3, 4, 5)
ORDER BY id;

SELECT id
FROM (VALUES (1), (2), (NULL)) AS t(id)
WHERE id NOT IN (1, 3, NULL)
ORDER BY id;
