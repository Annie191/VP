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

-- 单表扫描（bug: 返回 4，正确应为 0）
SELECT COUNT((NULL) != (id)) FROM vp_s;

-- ROW_NUMBER 对齐（正确: 返回 0）
SELECT COUNT((NULL) != (id)) FROM (
  WITH l AS (SELECT ROW_NUMBER() OVER (ORDER BY id) AS rn, id FROM vp_l),
       r AS (SELECT ROW_NUMBER() OVER (ORDER BY id) AS rn, id FROM vp_r)
  SELECT l.id AS id FROM l JOIN r ON l.rn = r.rn
) vp_s;
