-- @setup
DROP TABLE IF EXISTS type_roundtrip_fulltext;

CREATE TABLE type_roundtrip_fulltext (
  id INT,
  doc TSVECTOR,
  q TSQUERY
);

INSERT INTO type_roundtrip_fulltext VALUES
  (1, to_tsvector('simple', 'vector database search engine'),
      plainto_tsquery('simple', 'vector database search engine')),
  (2, to_tsvector('simple', 'graph database query planner'),
      to_tsquery('simple', 'graph & database & query & planner')),
  (3, NULL, NULL);

-- @query rowsort
SELECT id,
  CAST(doc AS VARCHAR) AS doc,
  CAST(q AS VARCHAR) AS q
FROM type_roundtrip_fulltext
ORDER BY id;

-- @teardown
DROP TABLE IF EXISTS type_roundtrip_fulltext;
