-- split/join：错误地返回空集
SELECT 1 AS ok
FROM (
  SELECT l.id, r.c0
  FROM l JOIN r ON l.id = r.id
) v
WHERE DATE_FORMAT(
  CASE 0
    WHEN CAST(v.id AS CHAR) THEN NULL
    WHEN TRUE THEN v.c0
    ELSE CAST(v.c0 AS DATE)
  END,
  (v.c0 OR 1) NOT LIKE TRUE
) | v.id;