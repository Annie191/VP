DROP DATABASE IF EXISTS repro_ob_712_2_min;
CREATE DATABASE repro_ob_712_2_min;
USE repro_ob_712_2_min;
SET SESSION sql_mode = 'STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_AUTO_CREATE_USER';
SET SESSION time_zone = '+00:00';
SET SESSION optimizer_switch = '';
SET SESSION group_concat_max_len = 1048576;
CREATE TABLE src (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  c1 FLOAT NULL,
  KEY i_c1(c1, id)
);
INSERT INTO src(c1) VALUES (0.0), (NULL);
CREATE TABLE l (
  id BIGINT NOT NULL PRIMARY KEY,
  c1 FLOAT NULL,
  KEY i_l(id, c1)
);
CREATE TABLE r (
  id BIGINT NOT NULL PRIMARY KEY
);
INSERT INTO l SELECT id, c1 FROM src;
INSERT INTO r SELECT id FROM src;
CREATE VIEW v AS
SELECT l.id, l.c1
FROM l JOIN r ON l.id = r.id;

-- ORIGINAL SINGLE-TABLE QUERY
-- Single table: 走 c1 索引顺序
SELECT 'single' AS q,
       (-1) <= CAST((concat(src.c1, '')) && NULL AS SIGNED) AS e
FROM src FORCE INDEX(i_c1)
GROUP BY (-1) <= CAST((concat(src.c1, '')) && NULL AS SIGNED)
WITH ROLLUP
ORDER BY (-1) <= CAST((concat(src.c1, '')) && NULL AS SIGNED)
LIMIT 3;

-- SKIPPED TLP/NoREC: GROUP BY is not handled by this batch rewriter
