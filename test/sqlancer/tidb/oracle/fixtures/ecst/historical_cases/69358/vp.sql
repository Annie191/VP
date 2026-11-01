SELECT
  CAST(
    CASE '2025-02-01 20:39:39'
      WHEN v.c0 THEN ((false) REGEXP NULL)
      ELSE CAST(v.c2 AS TIME)
    END AS DATETIME
  ) AS val
FROM (
  SELECT l.id, r.c0, l.c2
  FROM l
  JOIN (
    SELECT id, COUNT(*) AS cnt
    FROM r
    GROUP BY id
  ) rg ON l.id = rg.id
  JOIN r ON r.id = rg.id
) v
GROUP BY
  CAST(
    CASE '2025-02-01 20:39:39'
      WHEN v.c0 THEN ((false) REGEXP NULL)
      ELSE CAST(v.c2 AS TIME)
    END AS DATETIME
  );