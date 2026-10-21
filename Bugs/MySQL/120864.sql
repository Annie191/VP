DROP DATABASE IF EXISTS repro8;
CREATE DATABASE repro8;
USE repro8;

CREATE TABLE src (
  vp_rowid BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  c0 FLOAT NULL, c1 DECIMAL(65,30) NULL
) ENGINE=InnoDB;
INSERT INTO src (c0, c1) VALUES (0.0, NULL), (1.16575E9, 1);
ALTER TABLE src ADD KEY i0 (c0 DESC, c1 DESC);

CREATE TABLE l (vp_rowid BIGINT NOT NULL PRIMARY KEY, c1 DECIMAL(65,30) NULL) ENGINE=InnoDB;
CREATE TABLE r (vp_rowid BIGINT NOT NULL PRIMARY KEY, c0 FLOAT NULL) ENGINE=InnoDB;
INSERT INTO l SELECT vp_rowid, c1 FROM src;
INSERT INTO r SELECT vp_rowid, c0 FROM src;

-- Single: 覆盖索引 → (-2,0), (-1,NULL) ✓
SELECT (- vp_rowid) AS ref0, ('x') = (IFNULL(NULL, c1) XOR 0) AS ref1
FROM src GROUP BY (- vp_rowid), ('x') = (IFNULL(NULL, c1) XOR 0);

-- Split: AGGREGATE_THEN_MERGE → ref1 全部 NULL ❌
SELECT (- vp_rowid) AS ref0, ('x') = (IFNULL(NULL, c1) XOR 0) AS ref1
FROM (
  SELECT l.vp_rowid, r.c0, l.c1
  FROM l
  JOIN (SELECT vp_rowid, COUNT(*) AS c FROM r GROUP BY vp_rowid) rg ON l.vp_rowid = rg.vp_rowid
  JOIN r ON r.vp_rowid = rg.vp_rowid
) AS src GROUP BY (- vp_rowid), ('x') = (IFNULL(NULL, c1) XOR 0);