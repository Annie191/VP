DROP DATABASE IF EXISTS codex_db1_min;
CREATE DATABASE codex_db1_min;
USE codex_db1_min;

CREATE TABLE s(
  vp_rowid BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  c0 TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL
) ENGINE=InnoDB;

INSERT INTO s(c0) VALUES
  ('0'),
  ('1521367719'),
  ('f~O_1');

CREATE TABLE l(
  vp_rowid BIGINT NOT NULL PRIMARY KEY
) ENGINE=InnoDB;

CREATE TABLE r(
  vp_rowid BIGINT NOT NULL PRIMARY KEY,
  c0 TEXT NULL
) ENGINE=InnoDB;

INSERT INTO l(vp_rowid)
SELECT vp_rowid FROM s;

INSERT INTO r(vp_rowid, c0)
SELECT vp_rowid, c0 FROM s;

SELECT CAST(LEAST('z', '_ejEZpA(1', '2090248689', c0) AS SIGNED) AS ref0
FROM s
ORDER BY vp_rowid;

SELECT CAST(LEAST('z', '_ejEZpA(1', '2090248689', d.c0) AS SIGNED) AS ref0
FROM (
  WITH lq AS (
    SELECT ROW_NUMBER() OVER (ORDER BY vp_rowid) AS rn, vp_rowid
    FROM l
  ),
  rq AS (
    SELECT ROW_NUMBER() OVER (ORDER BY vp_rowid) AS rn, vp_rowid, c0
    FROM r
  )
  SELECT lq.vp_rowid AS vp_rowid, rq.c0 AS c0
  FROM lq
  JOIN rq ON lq.rn = rq.rn
) d
ORDER BY d.vp_rowid;