-- 单表：返回 1 行
SELECT BIT_AND(id) AS a, id
FROM src
WHERE '^' >= BINARY(id | '2020-01-18')
GROUP BY id;