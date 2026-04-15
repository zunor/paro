SELECT count(r.id)
FROM spill_join_l l
LEFT JOIN spill_join_r r
  ON l.id = r.id;
