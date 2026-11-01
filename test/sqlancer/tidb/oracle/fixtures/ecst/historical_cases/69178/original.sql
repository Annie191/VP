-- Single-table query (returns wrong count)
SELECT COUNT(CAST(
         CASE WHEN FALSE
              THEN c0
              ELSE CAST(c0 AS TIME)
         END AS DATETIME
       )) AS single_cnt
FROM src;