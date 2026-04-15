DROP TABLE IF EXISTS obs_docs;

CREATE TABLE obs_docs (
    id INT PRIMARY KEY,
    title VARCHAR
);

INSERT INTO obs_docs VALUES
    (1, 'graph database systems'),
    (2, 'vector database systems'),
    (3, NULL);

CREATE INDEX idx_obs_docs_title_fts
ON obs_docs USING GIN (to_tsvector('simple', title));

SELECT table_name, estimated_rows, estimated_size_bytes >= 0 AS has_size
FROM paro_tables()
WHERE table_name = 'obs_docs';

SELECT index_name, index_type, build_state, entry_count, position('column_names' IN extra_info) > 0 AS has_columns
FROM paro_indexes()
WHERE table_name = 'obs_docs'
ORDER BY index_name;

SELECT column_name, num_rows, null_count, distinct_count, has_fulltext_index
FROM paro_storage_info('obs_docs')
WHERE column_name IN ('id', 'title')
ORDER BY column_name, segment_id;

SELECT name, enabled, invocation_count >= 0 AS has_invocations
FROM paro_optimizers()
WHERE name IN ('search_optimization', 'statistics_gathering')
ORDER BY name;

EXPLAIN (VERBOSE)
SELECT id
FROM obs_docs
WHERE fulltext_match(title, 'graph')
ORDER BY bm25(title, 'graph') DESC, id
LIMIT 2;

DROP TABLE obs_docs;
