SELECT count(r.payload)
FROM phase0_join_external_l l
LEFT JOIN phase0_join_external_r r
  ON l.id = r.id;
