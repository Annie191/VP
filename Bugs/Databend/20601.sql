DROP DATABASE IF EXISTS repro_databend913_db2_min;
CREATE DATABASE repro_databend913_db2_min;
USE repro_databend913_db2_min;

CREATE TABLE source (
  vp_rowid BIGINT NOT NULL,
  c0boolean BOOLEAN NULL
) ENGINE=FUSE;

INSERT INTO source VALUES
  (1,FALSE),(2,FALSE),(3,FALSE),(4,FALSE),(5,FALSE),
  (6,FALSE),(7,FALSE),(8,FALSE),(9,FALSE),
  (10,TRUE),(11,TRUE),(12,TRUE),(13,TRUE),(14,TRUE),
  (15,TRUE),(16,TRUE),(17,TRUE),(18,TRUE),(19,TRUE),
  (20,TRUE),(21,TRUE),(22,TRUE),(23,TRUE),(24,TRUE);

CREATE TABLE part_l (
  vp_rowid BIGINT NOT NULL
) ENGINE=MEMORY;

CREATE TABLE part_r (
  vp_rowid BIGINT NOT NULL,
  c0boolean BOOLEAN NULL
) ENGINE=FUSE;

INSERT INTO part_l
SELECT vp_rowid FROM source;

INSERT INTO part_r
SELECT vp_rowid, c0boolean FROM source;

SELECT COUNT((-(vp_rowid * vp_rowid))), vp_rowid, vp_rowid
FROM source
WHERE c0boolean
GROUP BY vp_rowid, vp_rowid
ORDER BY vp_rowid, vp_rowid
LIMIT 21;

SELECT COUNT((-(s.vp_rowid * s.vp_rowid))), s.vp_rowid, s.vp_rowid
FROM (
  SELECT l.vp_rowid AS vp_rowid,
         r.c0boolean AS c0boolean
  FROM part_l l
  LEFT JOIN part_r r
    ON l.vp_rowid = r.vp_rowid
) s
WHERE s.c0boolean
GROUP BY s.vp_rowid, s.vp_rowid
ORDER BY s.vp_rowid, s.vp_rowid
LIMIT 21;
