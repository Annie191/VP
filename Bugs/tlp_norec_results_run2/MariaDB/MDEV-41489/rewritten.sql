DROP DATABASE IF EXISTS repro_mariadb816_db9_2152_min;
CREATE DATABASE repro_mariadb816_db9_2152_min;
USE repro_mariadb816_db9_2152_min;
SET SESSION optimizer_switch =
    'derived_merge=off,condition_pushdown_for_derived=on';
-- 未拆分的单表
CREATE TABLE source_2152 (
    rid BIGINT NOT NULL,
    b   BIT(1) NOT NULL,
    d   DATE NOT NULL,
    UNIQUE KEY (rid),
    KEY idx_source_2152 (rid, b)
) ENGINE=InnoDB;
-- 纵向拆分后的两个表
CREATE TABLE left_2152 (
    rid BIGINT PRIMARY KEY,
    b   BIT(1) NOT NULL
) ENGINE=InnoDB;
CREATE TABLE right_2152 (
    rid BIGINT PRIMARY KEY,
    d   DATE NOT NULL
) ENGINE=InnoDB;
INSERT INTO source_2152 VALUES (1, 0, '0000-00-00');
INSERT INTO left_2152   VALUES (1, 0);
INSERT INTO right_2152  VALUES (1, '0000-00-00');

-- ORIGINAL SINGLE-TABLE QUERY
-- 查询一：单表结构，实际返回 1 行：0000-00-00
SELECT d
FROM source_2152
WHERE (b IS NOT NULL) IN (d);

-- TLP BASE (original query with WHERE removed)
/* 查询一：单表结构，实际返回 1 行：0000-00-00 */ SELECT d FROM source_2152;

-- TLP UNION ALL
/* 查询一：单表结构，实际返回 1 行：0000-00-00 */ SELECT d FROM source_2152 WHERE (NOT b IS NULL) IN (d) UNION ALL /* 查询一：单表结构，实际返回 1 行：0000-00-00 */ SELECT d FROM source_2152 WHERE NOT (NOT b IS NULL) IN (d) UNION ALL /* 查询一：单表结构，实际返回 1 行：0000-00-00 */ SELECT d FROM source_2152 WHERE (NOT b IS NULL) IN (d) IS NULL;

-- NoREC optimized
/* 查询一：单表结构，实际返回 1 行：0000-00-00 */ SELECT COUNT(*) FROM source_2152 WHERE (NOT b IS NULL) IN (d);

-- NoREC unoptimized
/* 查询一：单表结构，实际返回 1 行：0000-00-00 */ SELECT SUM(CASE WHEN (NOT b IS NULL) IN (d) THEN 1 ELSE NULL END) FROM source_2152;
