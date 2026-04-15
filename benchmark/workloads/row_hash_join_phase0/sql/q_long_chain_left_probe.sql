SELECT count(*)
FROM phase0_join_long_l l
LEFT JOIN phase0_join_long_r r
  ON l.k = r.k;
