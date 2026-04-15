SELECT count(r.payload)
FROM phase0_join_short_l l
LEFT JOIN phase0_join_short_r r
  ON l.id = r.id;
