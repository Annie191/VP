DROP DATABASE IF EXISTS repro_databend925_db12_min;
CREATE DATABASE repro_databend925_db12_min;
USE repro_databend925_db12_min;
CREATE TABLE vp_repro_databend925_1274_source (
    vp_rowid BIGINT NOT NULL,
    c0boolean BOOLEAN NULL
) ENGINE = FUSE;
INSERT INTO vp_repro_databend925_1274_source
SELECT
    number + 1,
    number % 2 = 0
FROM numbers(103);
CREATE TABLE vp_repro_databend925_1274_l (
    vp_rowid BIGINT NOT NULL
) ENGINE = FUSE;
CREATE TABLE vp_repro_databend925_1274_r (
    vp_rowid BIGINT NOT NULL,
    c0boolean BOOLEAN NULL
) ENGINE = FUSE;
INSERT INTO vp_repro_databend925_1274_l (vp_rowid)
SELECT vp_rowid
FROM vp_repro_databend925_1274_source;
INSERT INTO vp_repro_databend925_1274_r (vp_rowid, c0boolean)
SELECT vp_rowid, c0boolean
FROM vp_repro_databend925_1274_source;

-- ORIGINAL SINGLE-TABLE QUERY
SELECT
    s.vp_rowid,
    (
        SELECT MIN(s2.vp_rowid)
        FROM vp_repro_databend925_1274_source AS s2
        WHERE s2.vp_rowid <= s.vp_rowid
    )
FROM vp_repro_databend925_1274_source AS s;

-- SKIPPED TLP/NoREC: nested query/window/union is not handled by this batch rewriter
