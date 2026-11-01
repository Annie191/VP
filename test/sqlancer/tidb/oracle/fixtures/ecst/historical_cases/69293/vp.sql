-- split/join：错误地返回空集
SELECT BIT_AND(v.id) AS a, v.id
FROM (
  SELECT l.id, l.c0
  FROM l JOIN r ON l.id = r.id
) v
WHERE '^' >= BINARY(v.id | '2020-01-18')
GROUP BY v.id;