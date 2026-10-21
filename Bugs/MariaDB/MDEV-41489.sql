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

-- 查询一：单表结构，实际返回 1 行：0000-00-00
SELECT d
FROM source_2152
WHERE (b IS NOT NULL) IN (d);

-- 查询二：双 CTE + JOIN 的多表结构，实际返回 0 行
SELECT d
FROM (
    WITH
        l AS (SELECT * FROM left_2152),
        r AS (SELECT * FROM right_2152)
    SELECT l.rid, l.b, r.d
    FROM l
    JOIN r ON l.rid = r.rid
) AS split_2152
WHERE (b IS NOT NULL) IN (d);