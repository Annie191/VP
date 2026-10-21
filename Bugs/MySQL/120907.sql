DROP DATABASE IF EXISTS repro_vp14;
CREATE DATABASE repro_vp14;
USE repro_vp14;

CREATE TABLE src (
  vp_rowid BIGINT NOT NULL PRIMARY KEY,
  c0 BIGINT NULL,
  UNIQUE KEY (c0)
);
INSERT INTO src VALUES (1, 0), (2, 1);

CREATE TABLE l (vp_rowid BIGINT NOT NULL PRIMARY KEY);
CREATE TABLE r (
  vp_rowid BIGINT NOT NULL PRIMARY KEY,
  c0 BIGINT NULL,
  KEY (c0, vp_rowid)
);
INSERT INTO l SELECT vp_rowid FROM src;
INSERT INTO r SELECT vp_rowid, c0 FROM src;

-- 单表：实际返回空
SELECT c0
FROM src
WHERE +(IF('', NULL, '0.22857643383585602')) IN (c0)
ORDER BY CAST((CASE c0 WHEN vp_rowid THEN vp_rowid ELSE NULL END) IS NOT NULL AS SIGNED) DESC;

-- 多表：实际返回一行 c0 = 0
WITH
  lrn AS (
    SELECT ROW_NUMBER() OVER (ORDER BY vp_rowid) AS rn, vp_rowid FROM l
  ),
  rrn AS (
    SELECT ROW_NUMBER() OVER (ORDER BY vp_rowid) AS rn, vp_rowid, c0 FROM r
  )
SELECT x.c0
FROM (
  SELECT lrn.vp_rowid, rrn.c0
  FROM lrn STRAIGHT_JOIN rrn ON lrn.rn = rrn.rn
) AS x
WHERE +(IF('', NULL, '0.22857643383585602')) IN (x.c0)
ORDER BY CAST((CASE x.c0 WHEN x.vp_rowid THEN x.vp_rowid ELSE NULL END) IS NOT NULL AS SIGNED) DESC;