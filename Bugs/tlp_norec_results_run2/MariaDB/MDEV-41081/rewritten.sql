DROP DATABASE IF EXISTS repro_mariadb808_db12_min;
CREATE DATABASE repro_mariadb808_db12_min;
USE repro_mariadb808_db12_min;
CREATE TABLE source_rows (
    vp_rowid BIGINT NOT NULL PRIMARY KEY,
    c0 TINYINT(1),
    c1 CHAR(1) NOT NULL,
    KEY idx_source (vp_rowid, c0)
);
CREATE TABLE left_rows (
    vp_rowid BIGINT NOT NULL PRIMARY KEY,
    c0 TINYINT(1),
    KEY idx_left (c0, vp_rowid)
);
CREATE TABLE right_rows (
    vp_rowid BIGINT NOT NULL PRIMARY KEY,
    c1 CHAR(1) NOT NULL
);
INSERT INTO source_rows VALUES
    (1, 0, 'a'),
    (2, 0, 'b'),
    (3, 1, 'c');
INSERT INTO left_rows
SELECT vp_rowid, c0 FROM source_rows;
INSERT INTO right_rows
SELECT vp_rowid, c1 FROM source_rows;

-- ORIGINAL SINGLE-TABLE QUERY
-- 原始单表查询：实际返回 0 行
SELECT vp_rowid, c0
FROM source_rows
WHERE '0.45' IN (c0);

-- TLP BASE (original query with WHERE removed)
/* 原始单表查询：实际返回 0 行 */ SELECT vp_rowid, c0 FROM source_rows;

-- TLP UNION ALL
/* 原始单表查询：实际返回 0 行 */ SELECT vp_rowid, c0 FROM source_rows WHERE '0.45' IN (c0) UNION ALL /* 原始单表查询：实际返回 0 行 */ SELECT vp_rowid, c0 FROM source_rows WHERE NOT '0.45' IN (c0) UNION ALL /* 原始单表查询：实际返回 0 行 */ SELECT vp_rowid, c0 FROM source_rows WHERE '0.45' IN (c0) IS NULL;

-- NoREC optimized
/* 原始单表查询：实际返回 0 行 */ SELECT COUNT(*) FROM source_rows WHERE '0.45' IN (c0);

-- NoREC unoptimized
/* 原始单表查询：实际返回 0 行 */ SELECT SUM(CASE WHEN '0.45' IN (c0) THEN 1 ELSE NULL END) FROM source_rows;
