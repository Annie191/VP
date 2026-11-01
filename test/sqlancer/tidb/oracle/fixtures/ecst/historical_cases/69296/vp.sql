-- split/scalar subquery：返回 NULL
SELECT MAX(
  CAST(
    CASE v.c2
      WHEN v.id THEN v.id | v.c4
      ELSE CAST(v.c4 AS DATETIME)
    END AS DATE
  )
) AS split_max
FROM (
  SELECT
    l.id,
    (SELECT r.c2 FROM r WHERE r.id = l.id) AS c2,
    l.c4
  FROM l
) v
ORDER BY v.id;