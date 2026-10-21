DROP DATABASE IF EXISTS repro_db5_final;
CREATE DATABASE repro_db5_final;
USE repro_db5_final;

CREATE TABLE src (
  vp_rowid BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  c0 FLOAT NULL,
  c1 DECIMAL(65,30) NULL,
  UNIQUE KEY i1(c0, c1)
) ENGINE=InnoDB;

INSERT INTO src(c0, c1) VALUES
  (1.1, NULL),
  (2.2, NULL),
  (3.3, 0),
  (4.4, 0);

CREATE TABLE l (
  vp_rowid BIGINT NOT NULL PRIMARY KEY,
  c1 DECIMAL(65,30) NULL
) ENGINE=InnoDB;

CREATE TABLE r (
  vp_rowid BIGINT NOT NULL PRIMARY KEY,
  c0 FLOAT NULL
) ENGINE=InnoDB;

INSERT INTO l
SELECT vp_rowid, c1 FROM src;

INSERT INTO r
SELECT vp_rowid, c0 FROM src;

SELECT 'single' AS qt,
       src.vp_rowid,
       -((src.vp_rowid) = ((src.c0) NOT IN (src.c1))) AS ref2,
       CAST(IFNULL(src.c1, src.vp_rowid) AS SIGNED) AS ref3,
       BIT_AND(CAST(vp_rowid AS SIGNED)) AS ref4
FROM src
GROUP BY
       -((src.vp_rowid) = ((src.c0) NOT IN (src.c1))),
       src.vp_rowid,
       CAST(IFNULL(src.c1, src.vp_rowid) AS SIGNED)
WITH ROLLUP
ORDER BY src.vp_rowid
LIMIT 4;

SELECT 'split' AS qt,
       s.vp_rowid,
       -((s.vp_rowid) = ((s.c0) NOT IN (s.c1))) AS ref2,
       CAST(IFNULL(s.c1, s.vp_rowid) AS SIGNED) AS ref3,
       BIT_AND(CAST(vp_rowid AS SIGNED)) AS ref4
FROM (
    SELECT l.vp_rowid AS vp_rowid, r.c0 AS c0, l.c1 AS c1
    FROM l
    JOIN (
        SELECT vp_rowid, COUNT(*) AS vp_count
        FROM r
        GROUP BY vp_rowid
    ) rg ON l.vp_rowid = rg.vp_rowid
    JOIN r ON r.vp_rowid = rg.vp_rowid
) s
GROUP BY
       -((s.vp_rowid) = ((s.c0) NOT IN (s.c1))),
       s.vp_rowid,
       CAST(IFNULL(s.c1, s.vp_rowid) AS SIGNED)
WITH ROLLUP
ORDER BY s.vp_rowid
LIMIT 4;