DROP DATABASE IF EXISTS repro_databend922_db59_min;
CREATE DATABASE repro_databend922_db59_min;
USE repro_databend922_db59_min;

CREATE TABLE s(id BIGINT NOT NULL, v INT NULL) ENGINE=MEMORY;
CREATE TABLE l(id BIGINT NOT NULL) ENGINE=MEMORY;
CREATE TABLE r(id BIGINT NOT NULL, v INT NULL) ENGINE=MEMORY;

INSERT INTO s VALUES (1, 1);
INSERT INTO l SELECT id FROM s;
INSERT INTO r SELECT id, v FROM s;

-- 单表：成功，返回 NULL
SELECT AVG(id)
FROM s
WHERE -2 * id NOT IN (id, -id)
  AND -id = id;

-- 多表：报错，预期同样返回 NULL
SELECT AVG(id)
FROM (
    SELECT l.id,
           (SELECT r.v FROM r WHERE r.id = l.id) AS v
    FROM l
    WHERE EXISTS (
        SELECT 1 FROM r WHERE r.id = l.id
    )
) AS s
WHERE -2 * id NOT IN (id, -id)
  AND -id = id;