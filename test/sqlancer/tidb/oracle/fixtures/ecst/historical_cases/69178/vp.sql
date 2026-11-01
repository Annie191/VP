-- Join query (returns correct count)
SELECT COUNT(CAST(
         CASE WHEN FALSE
              THEN r.c0
              ELSE CAST(r.c0 AS TIME)
         END AS DATETIME
       )) AS join_cnt
FROM l JOIN r ON l.id = r.id;