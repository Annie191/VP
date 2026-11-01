-- split/join：错误地返回空集
SELECT v.c0, v.id, 1168174079 AS marker
FROM (
  SELECT l.id, r.c0
  FROM l JOIN r ON l.id = r.id
) v
WHERE BINARY(v.id > v.id) < '}'
GROUP BY v.c0, v.id
HAVING (
  CASE v.id
    WHEN 'h' THEN v.c0 IS NULL
    WHEN v.id THEN v.c0
    ELSE NULL
  END
) IS NULL
ORDER BY v.id;