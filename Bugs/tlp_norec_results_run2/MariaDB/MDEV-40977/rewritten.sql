DROP DATABASE IF EXISTS repro_mariadb_db13_904_min;
CREATE DATABASE repro_mariadb_db13_904_min;
USE repro_mariadb_db13_904_min;
-- 单表结构
CREATE TABLE source_t (
    v DOUBLE UNSIGNED NOT NULL
);
INSERT INTO source_t VALUES (9223372036854775807);
-- 多表标量子查询重构结构
CREATE TABLE left_t (
    vp_rowid BIGINT NOT NULL
);
CREATE TABLE right_t (
    vp_rowid BIGINT NOT NULL,
    v DOUBLE UNSIGNED NOT NULL
);
INSERT INTO left_t VALUES (1);
INSERT INTO right_t VALUES (1, 9223372036854775807);

-- ORIGINAL SINGLE-TABLE QUERY
-- 单表直接计算：实际返回 0
SELECT ((1503381287 << v) AND -553173254) AS result
FROM source_t;

-- TLP BASE (original query with WHERE removed)
/* 单表直接计算：实际返回 0 */ SELECT ((1503381287 << v) AND -553173254) AS result FROM source_t;

-- TLP UNION ALL
/* 单表直接计算：实际返回 0 */ SELECT ((1503381287 << v) AND -553173254) AS result FROM source_t WHERE TRUE UNION ALL /* 单表直接计算：实际返回 0 */ SELECT ((1503381287 << v) AND -553173254) AS result FROM source_t WHERE NOT TRUE UNION ALL /* 单表直接计算：实际返回 0 */ SELECT ((1503381287 << v) AND -553173254) AS result FROM source_t WHERE TRUE IS NULL;

-- NoREC optimized
/* 单表直接计算：实际返回 0 */ SELECT COUNT(*) FROM source_t;

-- NoREC unoptimized
/* 单表直接计算：实际返回 0 */ SELECT SUM(CASE WHEN TRUE THEN 1 ELSE NULL END) FROM source_t;
