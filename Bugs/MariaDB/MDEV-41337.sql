DROP DATABASE IF EXISTS repro_mariadb812_db9_min;
CREATE DATABASE repro_mariadb812_db9_min;
USE repro_mariadb812_db9_min;

CREATE TABLE source (
  vp_rowid BIGINT NOT NULL UNIQUE,
  c0 DATE UNIQUE,
  c1 INET6 UNIQUE,
  c2 DATETIME NOT NULL UNIQUE PRIMARY KEY,
  c3 INET6 NOT NULL
) ENGINE=InnoDB;

INSERT INTO source VALUES
(1,NULL,NULL,'0000-00-00 00:00:00','::1'),
(2,'2000-01-03','2001:db8::3','2000-01-01 00:00:02','2001:db8::3'),
(3,'2000-01-04','2001:db8::4','2000-01-01 00:00:03','2001:db8::4'),
(4,'2000-01-05','2001:db8::5','2000-01-01 00:00:04','2001:db8::5'),
(5,'2000-01-06','2001:db8::6','2000-01-01 00:00:05','2001:db8::6'),
(6,'2000-01-07','2001:db8::7','2000-01-01 00:00:06','2001:db8::7'),
(7,'2000-01-08','2001:db8::8','2000-01-01 00:00:07','2001:db8::8'),
(8,'2000-01-09','2001:db8::9','2000-01-01 00:00:08','2001:db8::9');

CREATE TABLE l (
  vp_rowid BIGINT NOT NULL PRIMARY KEY,
  c0 DATE
) ENGINE=InnoDB;

CREATE TABLE r (
  vp_rowid BIGINT NOT NULL PRIMARY KEY,
  c1 INET6,
  c2 DATETIME NOT NULL,
  c3 INET6 NOT NULL
) ENGINE=InnoDB;

INSERT INTO l SELECT vp_rowid, c0 FROM source;
INSERT INTO r SELECT vp_rowid, c1, c2, c3 FROM source;

CREATE INDEX idx_l_c0 ON l(c0);

SET SESSION optimizer_prune_level=1;
SET SESSION optimizer_switch='derived_merge=off';

SELECT 'direct' AS tag, c0
FROM (
  SELECT DISTINCTROW c0
  FROM source
  WHERE c2 IS NULL
) q
ORDER BY 1;

SELECT 'rewritten' AS tag, c0
FROM (
  SELECT DISTINCTROW c0
  FROM (
    WITH l1 AS (
      SELECT ROW_NUMBER() OVER (ORDER BY vp_rowid) AS rn,
             vp_rowid, c0
      FROM l
    ),
    r1 AS (
      SELECT ROW_NUMBER() OVER (ORDER BY vp_rowid) AS rn,
             vp_rowid, c1, c2, c3
      FROM r
    )
    SELECT l1.vp_rowid, l1.c0, r1.c1, r1.c2, r1.c3
    FROM l1
    JOIN r1 ON l1.rn = r1.rn
  ) reconstructed
  WHERE c2 IS NULL
) q
ORDER BY 1;