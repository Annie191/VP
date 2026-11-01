-- 单表：返回 1 行
SELECT c0, id, 1168174079 AS marker
FROM src
WHERE BINARY(id > id) < '}'
GROUP BY c0, id
HAVING (
  CASE id
    WHEN 'h' THEN c0 IS NULL
    WHEN id THEN c0
    ELSE NULL
  END
) IS NULL
ORDER BY id;