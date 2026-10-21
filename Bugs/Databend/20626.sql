DROP DATABASE IF EXISTS repro_databend917_db2_min;
CREATE DATABASE repro_databend917_db2_min;
USE repro_databend917_db2_min;

CREATE TABLE source (
    vp_rowid BIGINT NOT NULL,
    c0map MAP(VARCHAR, BIGINT) NULL
) ENGINE=FUSE;

CREATE TABLE left_part (
    vp_rowid BIGINT NOT NULL
) ENGINE=FUSE;

CREATE TABLE right_part (
    vp_rowid BIGINT NOT NULL,
    c0map MAP(VARCHAR, BIGINT) NULL
) ENGINE=FUSE;

INSERT INTO source VALUES
(1, {'x':1}),
(2, NULL),
(3, {'x':2}),
(4, NULL);

INSERT INTO left_part
SELECT vp_rowid FROM source;

INSERT INTO right_part
SELECT vp_rowid, c0map FROM source;

SELECT COUNT(source.c0map)
FROM source
LIMIT 2;

SELECT COUNT(s.c0map)
FROM (
    SELECT l.vp_rowid, r.c0map
    FROM left_part l
    LEFT JOIN right_part r
        ON l.vp_rowid = r.vp_rowid
) s
LIMIT 2;
