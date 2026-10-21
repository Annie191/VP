DROP DATABASE IF EXISTS ch_vp_not_found_repro2;
CREATE DATABASE ch_vp_not_found_repro2;
USE ch_vp_not_found_repro2;

-- VP source/base table
CREATE TABLE vp_base (
    vp_rowid UInt64,
    a Int32,
    b UInt8,
    c String
) ENGINE = MergeTree
ORDER BY vp_rowid;

-- VP left vertical partition
CREATE TABLE vp_left (
    vp_rowid UInt64,
    a Int32,
    b UInt8
) ENGINE = MergeTree
ORDER BY vp_rowid;

-- VP right vertical partition
CREATE TABLE vp_right (
    vp_rowid UInt64,
    c String
) ENGINE = MergeTree
ORDER BY vp_rowid;

INSERT INTO vp_base VALUES (1, 10, 1, 'x');

INSERT INTO vp_left
SELECT vp_rowid, a, b FROM vp_base;

INSERT INTO vp_right
SELECT vp_rowid, c FROM vp_base;

SELECT a
FROM vp_base AS s
WHERE s.b != 255
ORDER BY s.vp_rowid
LIMIT 1
SETTINGS optimize_move_to_prewhere = 0;

SELECT s.a
FROM (
    SELECT
        l.vp_rowid AS vp_rowid,
        l.a AS a,
        l.b AS b,
        r.c AS c
    FROM vp_left AS l
    LEFT JOIN vp_right AS r
        ON l.vp_rowid = r.vp_rowid
) AS s
WHERE s.b != 255
ORDER BY s.vp_rowid
LIMIT 1
SETTINGS optimize_move_to_prewhere = 0;