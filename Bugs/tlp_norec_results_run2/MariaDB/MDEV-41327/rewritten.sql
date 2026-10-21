-- No top-level single-table SELECT found.
DROP DATABASE IF EXISTS repro_mariadb_database3_tiny;
CREATE DATABASE repro_mariadb_database3_tiny;
USE repro_mariadb_database3_tiny;

CREATE TABLE source (
  vp_rowid BIGINT PRIMARY KEY,
  c0 SET('a','b','c') NOT NULL,
  c1 DATE NOT NULL,
  c2 ENUM('a','b','c') NOT NULL,
  c3 TIMESTAMP NOT NULL
) ENGINE=MyISAM;

INSERT INTO source VALUES
  (1, '', '0000-00-00', 'a', '0000-00-00 00:00:00');
INSERT INTO source VALUES
  (2, 'a', '2000-01-03', 'b', '2000-01-01 00:00:02');

CREATE TABLE l (
  vp_rowid BIGINT PRIMARY KEY,
  c0 SET('a','b','c') NOT NULL,
  c2 ENUM('a','b','c') NOT NULL
) ENGINE=MyISAM;

CREATE TABLE r (
  vp_rowid BIGINT PRIMARY KEY,
  c1 DATE NOT NULL,
  c3 TIMESTAMP NOT NULL
) ENGINE=MyISAM;

INSERT INTO l SELECT vp_rowid, c0, c2 FROM source;
INSERT INTO r SELECT vp_rowid, c1, c3 FROM source;

SET SESSION optimizer_switch='derived_merge=off';

-- 单表查询
SELECT 'direct' AS tag, x
FROM (
  SELECT DISTINCT c3 AS x
  FROM source
  WHERE c3 IN (c2 << c2)
) q;

-- 多表重写查询
SELECT 'rewrite' AS tag, x
FROM (
  SELECT DISTINCT c3 AS x
  FROM (
    SELECT
      l.vp_rowid,
      l.c0,
      r.c1,
      l.c2,
      r.c3
    FROM l
    JOIN (
      SELECT r0.*
      FROM r r0
      WHERE r0.vp_rowid IN (
        SELECT vp_rowid FROM l
      )
    ) r ON l.vp_rowid = r.vp_rowid
  ) source
  WHERE c3 IN (c2 << c2)
) q;
