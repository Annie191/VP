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

-- 单表查询
SELECT
  (- source.vp_rowid),
  ROW_NUMBER() OVER (ORDER BY `vp_rowid`, `vp_rowid`),
  (((((source.vp_rowid) * (source.vp_rowid)) - ((+ source.vp_rowid)))
    * (((source.vp_rowid) / (-1401453600)) % (source.vp_rowid))))
FROM source
WHERE (((+ source.vp_rowid)) - ((source.vp_rowid) % (((1208992282) / (41992119)))))
ORDER BY source.c0varchar, source.vp_rowid;

-- 多表重构查询：LEFT JOIN UNION ALL RIGHT JOIN，保持原日志结构
SELECT
  (- reconstructed.vp_rowid),
  ROW_NUMBER() OVER (ORDER BY `vp_rowid`, `vp_rowid`),
  (((((reconstructed.vp_rowid) * (reconstructed.vp_rowid)) - ((+ reconstructed.vp_rowid)))
    * (((reconstructed.vp_rowid) / (-1401453600)) % (reconstructed.vp_rowid))))
FROM (
    SELECT l.vp_rowid AS vp_rowid, r.c0varchar AS c0varchar
    FROM part_l l
    LEFT JOIN part_r r ON l.vp_rowid = r.vp_rowid

    UNION ALL

    SELECT r.vp_rowid AS vp_rowid, r.c0varchar AS c0varchar
    FROM part_l l
    RIGHT JOIN part_r r ON l.vp_rowid = r.vp_rowid
    WHERE l.vp_rowid IS NULL
) reconstructed
WHERE (((+ reconstructed.vp_rowid)) - ((reconstructed.vp_rowid) % (((1208992282) / (41992119)))))
ORDER BY reconstructed.c0varchar, reconstructed.vp_rowid;