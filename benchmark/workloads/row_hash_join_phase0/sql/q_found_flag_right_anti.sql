SELECT count(*)
FROM phase0_join_found_l l
RIGHT ANTI JOIN phase0_join_found_r r
  ON l.id = r.id;
