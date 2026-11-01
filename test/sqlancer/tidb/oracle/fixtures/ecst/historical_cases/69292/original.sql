-- 单表：返回 2
SELECT COUNT(
  CAST(
    CASE 'x'
      WHEN id >= id THEN c0
      ELSE CAST(FALSE AS DATE)
    END AS DATETIME
  )
) AS single_count
FROM src;