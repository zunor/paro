SELECT count(r.id)
FROM memory_join_l l
LEFT JOIN memory_join_r r
  ON l.id = r.id;
