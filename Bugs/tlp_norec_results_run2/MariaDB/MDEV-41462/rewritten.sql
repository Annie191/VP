DROP DATABASE IF EXISTS repro_mariadb814_db0_vp123_min;
CREATE DATABASE repro_mariadb814_db0_vp123_min;
USE repro_mariadb814_db0_vp123_min;
SET optimizer_switch='derived_merge=on';
SET join_cache_level=3;
-- 原始单表
CREATE TABLE s(
    id BIGINT,
    c0 BIT(8),
    c1 DECIMAL(10,2)
) ENGINE=Aria;
-- 分表重建结构
CREATE TABLE l(
    id BIGINT PRIMARY KEY,
    c0 BIT(8),
    KEY k(c0,id)
) ENGINE=Aria;
CREATE TABLE r(
    id BIGINT PRIMARY KEY,
    c1 DECIMAL(10,2),
    KEY k(id,c1)
) ENGINE=Aria;
INSERT INTO s VALUES
    (1,NULL,NULL),
    (2,2,2.01),
    (3,3,3.01);
INSERT INTO l SELECT id,c0 FROM s;
INSERT INTO r SELECT id,c1 FROM s;

-- ORIGINAL SINGLE-TABLE QUERY
-- 原始单表查询
SELECT DISTINCT c1
FROM s
WHERE c1 IS NULL;

-- SKIPPED TLP/NoREC: DISTINCT is not handled by this batch rewriter
