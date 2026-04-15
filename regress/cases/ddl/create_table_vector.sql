-- @setup
DROP TABLE IF EXISTS vector_test;

CREATE TABLE vector_test (
    id INT,
    name VARCHAR,
    embedding VECTOR(3),
    PRIMARY KEY (id)
);

INSERT INTO vector_test VALUES (1, 'alpha', '[1.0, 2.0, 3.0]');
INSERT INTO vector_test VALUES (2, 'beta',  '[4.0, 5.0, 6.0]');
INSERT INTO vector_test VALUES (3, 'gamma', '[7.0, 8.0, 9.0]');

-- @query rowsort
SELECT id, name, embedding FROM vector_test;

-- @teardown
DROP TABLE IF EXISTS vector_test;
