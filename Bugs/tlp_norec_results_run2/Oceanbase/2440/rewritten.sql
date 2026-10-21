-- No top-level single-table SELECT found.
DROP DATABASE IF EXISTS repro_oceanbase730_db14_min;
CREATE DATABASE repro_oceanbase730_db14_min;
USE repro_oceanbase730_db14_min;

SET SESSION sql_mode = 'PIPES_AS_CONCAT';
SET SESSION ob_enable_aggregation_pushdown = 0;

CREATE TABLE vp_s (
  vp_rowid BIGINT NOT NULL PRIMARY KEY,
  c0 BIGINT NULL
) PARTITION BY HASH(vp_rowid) PARTITIONS 8;

CREATE TABLE vp_l (
  vp_rowid BIGINT NOT NULL PRIMARY KEY,
  c0 BIGINT NULL
) PARTITION BY HASH(vp_rowid) PARTITIONS 8;

CREATE TABLE vp_r (
  vp_rowid BIGINT NOT NULL PRIMARY KEY,
  c0 BIGINT NULL
) PARTITION BY HASH(vp_rowid) PARTITIONS 8;

INSERT INTO vp_s VALUES
(1,0),(2,0),(3,0),(4,0),(5,0),(6,0),(7,0),(8,0),(9,0),(10,0),(11,0),
(12,1),(13,1),(14,1),(15,1),(16,1),(17,1),(18,1),(19,1),(20,1),(21,1),(22,1);
INSERT INTO vp_l SELECT * FROM vp_s;
INSERT INTO vp_r SELECT * FROM vp_s;

SELECT 'source' AS side, q.g, q.f, q.b
FROM (
  SELECT /*+ PARALLEL(4) */
    GREATEST((-1936882281) NOT IN (s.c0, 375398795),
             CAST('' AS SIGNED), s.vp_rowid, 1) AS g,
    s.c0 IS FALSE AS f,
    BIT_AND(s.vp_rowid) AS b
  FROM vp_s s
  GROUP BY GREATEST((-1936882281) NOT IN (s.c0, 375398795),
                    CAST('' AS SIGNED), s.vp_rowid, 1),
           s.c0 IS FALSE WITH ROLLUP
) q
WHERE q.b = 0

UNION ALL

SELECT 'split', q.g, q.f, q.b
FROM (
  SELECT /*+ PARALLEL(4) */
    GREATEST((-1936882281) NOT IN (s.c0, 375398795),
             CAST('' AS SIGNED), s.vp_rowid, 1) AS g,
    s.c0 IS FALSE AS f,
    BIT_AND(s.vp_rowid) AS b
  FROM (
    SELECT l.vp_rowid, l.c0
    FROM vp_l l JOIN vp_r r ON l.vp_rowid = r.vp_rowid
  ) s
  GROUP BY GREATEST((-1936882281) NOT IN (s.c0, 375398795),
                    CAST('' AS SIGNED), s.vp_rowid, 1),
           s.c0 IS FALSE WITH ROLLUP
) q
WHERE q.b = 0;