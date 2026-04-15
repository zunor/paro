SELECT count(*)
FROM GRAPH_TABLE(bench_graph
    MATCH ANY SHORTEST (a:Person WHERE a.id = 1)-[e:Link]->{1,6}(b:Person)
    COLUMNS (a.id AS src, b.id AS dst)
) gt;
