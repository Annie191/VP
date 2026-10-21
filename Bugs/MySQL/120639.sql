DROP DATABASE IF EXISTS test_db2;
CREATE DATABASE test_db2;
USE test_db2;

-- 原始表
CREATE TABLE s (
  id BIGINT NOT NULL,
  KEY (id)
) ENGINE = InnoDB;

INSERT INTO s VALUES (1);

-- VP source：单表形式
CREATE TABLE vp_source (
  vp_rowid BIGINT NOT NULL,
  id BIGINT NOT NULL,
  PRIMARY KEY (vp_rowid),
  KEY (id)
) ENGINE = InnoDB;

INSERT INTO vp_source (vp_rowid, id)
SELECT id, id
FROM s;

-- VP split：多表形式
CREATE TABLE vp_l (
  vp_rowid BIGINT NOT NULL,
  PRIMARY KEY (vp_rowid)
) ENGINE = InnoDB;

CREATE TABLE vp_r (
  vp_rowid BIGINT NOT NULL,
  id BIGINT NOT NULL,
  PRIMARY KEY (vp_rowid),
  KEY (id)
) ENGINE = InnoDB;

INSERT INTO vp_l (vp_rowid)
SELECT id
FROM s;

INSERT INTO vp_r (vp_rowid, id)
SELECT id, id
FROM s;

SELECT COUNT(*) AS cnt
FROM vp_source
WHERE NOT (IFNULL(0.5, 0) NOT IN (id));

SELECT COUNT(*) AS cnt
FROM (
  SELECT l.vp_rowid, r.id
  FROM vp_l AS l
  LEFT JOIN vp_r AS r
    ON l.vp_rowid = r.vp_rowid

  UNION ALL

  SELECT r.vp_rowid, r.id
  FROM vp_l AS l
  RIGHT JOIN vp_r AS r
    ON l.vp_rowid = r.vp_rowid
  WHERE l.vp_rowid IS NULL
) AS vp_reconstructed
WHERE NOT (IFNULL(0.5, 0) NOT IN (id));