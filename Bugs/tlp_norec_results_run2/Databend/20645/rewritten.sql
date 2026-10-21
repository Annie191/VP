DROP DATABASE IF EXISTS repro_databend922_db59_min;
CREATE DATABASE repro_databend922_db59_min;
USE repro_databend922_db59_min;
CREATE TABLE s(id BIGINT NOT NULL, v INT NULL) ENGINE=MEMORY;
CREATE TABLE l(id BIGINT NOT NULL) ENGINE=MEMORY;
CREATE TABLE r(id BIGINT NOT NULL, v INT NULL) ENGINE=MEMORY;
INSERT INTO s VALUES (1, 1);
INSERT INTO l SELECT id FROM s;
INSERT INTO r SELECT id, v FROM s;

-- ORIGINAL SINGLE-TABLE QUERY
-- 单表：成功，返回 NULL
SELECT AVG(id)
FROM s
WHERE -2 * id NOT IN (id, -id)
  AND -id = id;

-- TLP BASE (original query with WHERE removed)
/* 单表：成功，返回 NULL */ SELECT AVG(id) FROM s;

-- TLP UNION ALL
SELECT SUM(agg_sum) / NULLIF(SUM(agg_count), 0) FROM (/* 单表：成功，返回 NULL */ SELECT SUM(id) AS agg_sum, COUNT(id) AS agg_count FROM s WHERE NOT -2 * id IN (id, -id) AND -id = id UNION ALL /* 单表：成功，返回 NULL */ SELECT SUM(id) AS agg_sum, COUNT(id) AS agg_count FROM s WHERE NOT NOT -2 * id IN (id, -id) AND -id = id UNION ALL /* 单表：成功，返回 NULL */ SELECT SUM(id) AS agg_sum, COUNT(id) AS agg_count FROM s WHERE NOT -2 * id IN (id, -id) AND -id = id IS NULL) AS tlp_parts;

-- NoREC optimized
/* 单表：成功，返回 NULL */ SELECT COUNT(*) FROM s WHERE NOT -2 * id IN (id, -id) AND -id = id;

-- NoREC unoptimized
/* 单表：成功，返回 NULL */ SELECT SUM(CASE WHEN NOT -2 * id IN (id, -id) AND -id = id THEN 1 ELSE NULL END) FROM s;
