DROP DATABASE IF EXISTS repro_mariadb_database9_tiny;
CREATE DATABASE repro_mariadb_database9_tiny;
USE repro_mariadb_database9_tiny;

CREATE TABLE src (
  id INT PRIMARY KEY,
  c0 DATETIME NOT NULL,
  c1 BIT(1) NOT NULL,
  c2 JSON NOT NULL,
  c3 DATE NULL
) ENGINE=Aria;

INSERT INTO src VALUES
  (1, '0000-00-00 00:00:00', b'0', 'null', NULL),
  (2, '2000-01-01 00:00:02', b'0', '{"vp":null}', '2000-01-03');

CREATE TABLE l (
  id INT PRIMARY KEY,
  c0 DATETIME NOT NULL,
  c2 JSON NOT NULL
) ENGINE=Aria;

CREATE TABLE r (
  id INT PRIMARY KEY,
  c1 BIT(1) NOT NULL,
  c3 DATE NULL,
  c2 JSON NOT NULL
) ENGINE=Aria;

INSERT INTO l SELECT id, c0, c2 FROM src;
INSERT INTO r SELECT id, c1, c3, c2 FROM src;

SET SESSION optimizer_switch='derived_merge=off';

-- 单表查询
SELECT
  'direct' AS tag,
  CAST((c0 IN (c0, (c0 < c1), c1)) AS UNSIGNED) AS v,
  c3
FROM src
WHERE c1 IN (c2);

-- CTE/derived 重写查询
WITH lq AS (
  SELECT * FROM l
),
rq AS (
  SELECT * FROM r
)
SELECT
  'rewrite' AS tag,
  CAST((c0 IN (c0, (c0 < c1), c1)) AS UNSIGNED) AS v,
  c3
FROM (
  SELECT
    lq.id,
    lq.c0,
    rq.c1,
    lq.c2,
    rq.c3
  FROM lq
  JOIN rq ON lq.id = rq.id
) q
WHERE c1 IN (c2);