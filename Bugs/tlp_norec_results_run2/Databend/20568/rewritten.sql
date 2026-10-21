DROP DATABASE IF EXISTS repro_databend925_db2_min;
CREATE DATABASE repro_databend925_db2_min;
USE repro_databend925_db2_min;
CREATE TABLE source (
  vp_rowid BIGINT NOT NULL,
  c0varchar VARCHAR NULL
) ENGINE=FUSE;
CREATE TABLE part_l (
  vp_rowid BIGINT NOT NULL,
  c0varchar VARCHAR NULL
) ENGINE=FUSE;
CREATE TABLE part_r (
  vp_rowid BIGINT NOT NULL,
  c0varchar VARCHAR NULL
) ENGINE=MEMORY;
INSERT INTO source VALUES (1, '');
INSERT INTO part_l SELECT * FROM source;
INSERT INTO part_r SELECT * FROM source;
SET enable_query_result_cache = 0;

-- ORIGINAL SINGLE-TABLE QUERY
-- 单表查询
SELECT
  (- source.vp_rowid),
  ROW_NUMBER() OVER (ORDER BY `vp_rowid`, `vp_rowid`),
  (((((source.vp_rowid) * (source.vp_rowid)) - ((+ source.vp_rowid)))
    * (((source.vp_rowid) / (-1401453600)) % (source.vp_rowid))))
FROM source
WHERE (((+ source.vp_rowid)) - ((source.vp_rowid) % (((1208992282) / (41992119)))))
ORDER BY source.c0varchar, source.vp_rowid;

-- SKIPPED TLP/NoREC: ORDER BY is not handled by this batch rewriter
