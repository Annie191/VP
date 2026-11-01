-- split/rewrite：返回 0
SELECT COUNT(
  CAST(
    CASE 'x'
      WHEN v.id >= v.id THEN v.c0
      ELSE CAST(FALSE AS DATE)
    END AS DATETIME
  )
) AS split_count
FROM (
  SELECT l.id, l.c0
  FROM l JOIN r ON l.id = r.id
) AS v;