DROP DATABASE IF EXISTS repro_databend925_db2_min;
CREATE DATABASE repro_databend925_db2_min;
USE repro_databend925_db2_min;

CREATE TABLE source(
  vp_rowid BIGINT NOT NULL,
  c2int BIGINT NULL
) ENGINE=FUSE;

INSERT INTO source VALUES (1, -1), (2, 1);

CREATE TABLE part_l(
  vp_rowid BIGINT NOT NULL
) ENGINE=FUSE;

CREATE TABLE part_r(
  vp_rowid BIGINT NOT NULL,
  c2int BIGINT NULL
) ENGINE=FUSE;

INSERT INTO part_l SELECT vp_rowid FROM source;
INSERT INTO part_r SELECT vp_rowid, c2int FROM source;

SELECT c2int % 1.0 AS e1, -c2int AS e2
FROM source
ORDER BY c2int % 1.0, -c2int;

SELECT r.c2int % 1.0 AS e1, -r.c2int AS e2
FROM part_l l
JOIN part_r r ON l.vp_rowid = r.vp_rowid
ORDER BY r.c2int % 1.0, -r.c2int;