SELECT
  NULL << 'aMI x*' AS a,
  403837559 AS b,
  BIN(c0) AS c
FROM (
  SELECT l.vp_rowid, r.c0
  FROM l
  JOIN (
    SELECT vp_rowid, COUNT(*) AS vp_count
    FROM r
    GROUP BY vp_rowid
  ) rg ON l.vp_rowid = rg.vp_rowid
  JOIN r ON r.vp_rowid = rg.vp_rowid
) v
WHERE BIN((BINARY(CAST(vp_rowid AS DECIMAL)) <= '|'))
ORDER BY 0.9328472781954409;