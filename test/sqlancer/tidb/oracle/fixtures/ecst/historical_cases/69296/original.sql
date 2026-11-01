-- 单表：返回 0000-00-00
SELECT MAX(
  CAST(
    CASE c2
      WHEN id THEN id | c4
      ELSE CAST(c4 AS DATETIME)
    END AS DATE
  )
) AS single_max
FROM src
ORDER BY id;