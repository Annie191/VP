DROP DATABASE IF EXISTS repro_mysql619_11;
CREATE DATABASE repro_mysql619_11;
USE repro_mysql619_11;

CREATE TABLE source (
  vp_rowid BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY
) ENGINE=InnoDB;

INSERT INTO source VALUES ();

CREATE TABLE l (
  vp_rowid BIGINT NOT NULL PRIMARY KEY
) ENGINE=InnoDB;

CREATE TABLE r (
  vp_rowid BIGINT NOT NULL PRIMARY KEY
) ENGINE=InnoDB;

INSERT INTO l SELECT vp_rowid FROM source;
INSERT INTO r SELECT vp_rowid FROM source;

-- Single query: returns 1
SELECT MAX(DISTINCT source.vp_rowid) AS ref0
FROM source
WHERE LEAST('Uk]6T', 0.8087354366787728) IN (source.vp_rowid);

-- Split query with SQLancer VP full outer simulation: returns NULL
SELECT MAX(DISTINCT vp_source.vp_rowid) AS ref0
FROM (
  SELECT l.vp_rowid AS vp_rowid
  FROM l
  LEFT JOIN r ON l.vp_rowid = r.vp_rowid

  UNION ALL

  SELECT r.vp_rowid AS vp_rowid
  FROM l
  RIGHT JOIN r ON l.vp_rowid = r.vp_rowid
  WHERE l.vp_rowid IS NULL
) AS vp_source
WHERE LEAST('Uk]6T', 0.8087354366787728) IN (vp_source.vp_rowid);