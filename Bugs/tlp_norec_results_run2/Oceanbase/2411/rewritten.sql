DROP DATABASE IF EXISTS repro_db7;
CREATE DATABASE repro_db7;
USE repro_db7;
DROP TABLE IF EXISTS vp_s, vp_l, vp_r;
CREATE TABLE vp_s (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY);
INSERT INTO vp_s VALUES (), (), (), ();
CREATE TABLE vp_l (id BIGINT NOT NULL PRIMARY KEY);
INSERT INTO vp_l SELECT id FROM vp_s;
CREATE TABLE vp_r (id BIGINT NOT NULL PRIMARY KEY);
INSERT INTO vp_r SELECT id FROM vp_s;

-- ORIGINAL SINGLE-TABLE QUERY
-- 单表扫描（bug: 返回 4，正确应为 0）
SELECT COUNT((NULL) != (id)) FROM vp_s;

-- TLP BASE (original query with WHERE removed)
/* 单表扫描（bug: 返回 4，正确应为 0） */ SELECT COUNT((NULL) <> (id)) FROM vp_s;

-- TLP UNION ALL
SELECT SUM(agg0) FROM (/* 单表扫描（bug: 返回 4，正确应为 0） */ SELECT COUNT((NULL) <> (id)) AS agg0 FROM vp_s WHERE TRUE UNION ALL /* 单表扫描（bug: 返回 4，正确应为 0） */ SELECT COUNT((NULL) <> (id)) AS agg0 FROM vp_s WHERE NOT TRUE UNION ALL /* 单表扫描（bug: 返回 4，正确应为 0） */ SELECT COUNT((NULL) <> (id)) AS agg0 FROM vp_s WHERE TRUE IS NULL) AS tlp_parts;

-- NoREC optimized
/* 单表扫描（bug: 返回 4，正确应为 0） */ SELECT COUNT(*) FROM vp_s;

-- NoREC unoptimized
/* 单表扫描（bug: 返回 4，正确应为 0） */ SELECT SUM(CASE WHEN TRUE THEN 1 ELSE NULL END) FROM vp_s;
