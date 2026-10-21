DROP DATABASE IF EXISTS repro_db13;
CREATE DATABASE repro_db13;
USE repro_db13;
DROP TABLE IF EXISTS vp_s, vp_l, vp_r;
CREATE TABLE vp_s (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY);
INSERT INTO vp_s VALUES (), (), (), ();
CREATE TABLE vp_l (id BIGINT NOT NULL PRIMARY KEY);
INSERT INTO vp_l SELECT id FROM vp_s;
CREATE TABLE vp_r (id BIGINT NOT NULL PRIMARY KEY);
INSERT INTO vp_r SELECT id FROM vp_s;

-- ORIGINAL SINGLE-TABLE QUERY
-- Single: 返回 4（错误，IF(id, NULL, id)→NULL → XOR→NULL → COUNT(NULL)=0）
SELECT COUNT((CAST(IF(id, NULL, id) AS SIGNED)) XOR (id)) FROM vp_s WHERE 1;

-- TLP BASE (original query with WHERE removed)
/* Single: 返回 4（错误，IF(id, NULL, id)→NULL → XOR→NULL → COUNT(NULL)=0） */ SELECT COUNT((CAST(IF(id, NULL, id) AS SIGNED)) XOR (id)) FROM vp_s;

-- TLP UNION ALL
SELECT SUM(agg0) FROM (/* Single: 返回 4（错误，IF(id, NULL, id)→NULL → XOR→NULL → COUNT(NULL)=0） */ SELECT COUNT((CAST(IF(id, NULL, id) AS SIGNED)) XOR (id)) AS agg0 FROM vp_s WHERE 1 UNION ALL /* Single: 返回 4（错误，IF(id, NULL, id)→NULL → XOR→NULL → COUNT(NULL)=0） */ SELECT COUNT((CAST(IF(id, NULL, id) AS SIGNED)) XOR (id)) AS agg0 FROM vp_s WHERE NOT 1 UNION ALL /* Single: 返回 4（错误，IF(id, NULL, id)→NULL → XOR→NULL → COUNT(NULL)=0） */ SELECT COUNT((CAST(IF(id, NULL, id) AS SIGNED)) XOR (id)) AS agg0 FROM vp_s WHERE 1 IS NULL) AS tlp_parts;

-- NoREC optimized
/* Single: 返回 4（错误，IF(id, NULL, id)→NULL → XOR→NULL → COUNT(NULL)=0） */ SELECT COUNT(*) FROM vp_s WHERE 1;

-- NoREC unoptimized
/* Single: 返回 4（错误，IF(id, NULL, id)→NULL → XOR→NULL → COUNT(NULL)=0） */ SELECT SUM(CASE WHEN 1 THEN 1 ELSE NULL END) FROM vp_s;
