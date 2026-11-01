SELECT
  'split' AS q,
  AVG(id NOT REGEXP ((((377946620)&c1) IS NOT NULL))) AS a,
  id,
  c0
FROM (
  SELECT l.id, l.c0, r.c1, l.c2
  FROM l RIGHT JOIN r ON l.id = r.id
) v
WHERE c1 <=> (
  CASE 0.0670996200630315
    WHEN (('850389211')=('2019-04-17 06:48:12')) THEN
      CASE ''
        WHEN c0 THEN -1819383790
        WHEN '2025-07-21' THEN c0
        ELSE c0
      END
    ELSE CAST(c2 AS DATETIME)
  END
)
GROUP BY id,c0;