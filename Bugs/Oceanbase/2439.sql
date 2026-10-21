DROP DATABASE IF EXISTS repro_ob730_db13_min;
CREATE DATABASE repro_ob730_db13_min;
USE repro_ob730_db13_min;

SET SESSION sql_mode = 'PIPES_AS_CONCAT';
SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci;
SET SESSION ob_enable_aggregation_pushdown = 0;

CREATE TABLE src (
  vp_rowid BIGINT NOT NULL PRIMARY KEY,
  c0 DECIMAL(65,30),
  c1 BIGINT
) ENGINE=InnoDB
PARTITION BY HASH(vp_rowid) PARTITIONS 8;

INSERT INTO src VALUES
  (1, -338707012, -1),
  (2, -338707012, -1);

CREATE TABLE l LIKE src;
CREATE TABLE r (
  vp_rowid BIGINT NOT NULL PRIMARY KEY,
  c1 BIGINT
) ENGINE=InnoDB
PARTITION BY HASH(vp_rowid) PARTITIONS 8;

INSERT INTO l SELECT * FROM src;
INSERT INTO r SELECT vp_rowid, c1 FROM src;

SELECT /*+ PARALLEL(4) */
  IFNULL(GREATEST(vp_rowid, c0), (-44622653) && c0) AS g
FROM src
GROUP BY IFNULL(GREATEST(vp_rowid, c0), (-44622653) && c0)
WITH ROLLUP
ORDER BY g;

SELECT /*+ PARALLEL(4) */
  IFNULL(GREATEST(s.vp_rowid, s.c0), (-44622653) && s.c0) AS g
FROM (
  WITH ll AS (SELECT * FROM l),
       rr AS (SELECT * FROM r)
  SELECT ll.vp_rowid, ll.c0, rr.c1
  FROM ll JOIN rr ON ll.vp_rowid = rr.vp_rowid
) AS s
GROUP BY IFNULL(GREATEST(s.vp_rowid, s.c0), (-44622653) && s.c0)
WITH ROLLUP
ORDER BY g;