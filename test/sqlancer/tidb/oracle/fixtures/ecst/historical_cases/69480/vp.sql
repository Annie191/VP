-- 分表重组: 结果 2,3,4,1 (NULL最后)
SELECT vp_rowid FROM (
  SELECT l.vp_rowid AS vp_rowid, l.c0 AS c0
  FROM l JOIN LATERAL (
    SELECT * FROM r rr WHERE rr.vp_rowid = l.vp_rowid
  ) r ON TRUE
) source2 WHERE '2018-04-13' ORDER BY (-c0) DESC, vp_rowid;