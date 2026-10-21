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

SELECT
    s.vp_rowid,
    (
        SELECT MIN(s2.vp_rowid)
        FROM vp_repro_databend925_1274_source AS s2
        WHERE s2.vp_rowid <= s.vp_rowid
    )
FROM vp_repro_databend925_1274_source AS s;

SELECT
    s.vp_rowid,
    (
        SELECT MIN(s2.vp_rowid)
        FROM (
            SELECT
                l.vp_rowid,
                (
                    SELECT r.c0boolean
                    FROM vp_repro_databend925_1274_r AS r
                    WHERE r.vp_rowid = l.vp_rowid
                ) AS c0boolean
            FROM vp_repro_databend925_1274_l AS l
            WHERE l.vp_rowid IN (
                SELECT r.vp_rowid
                FROM vp_repro_databend925_1274_r AS r
            )
        ) AS s2
        WHERE s2.vp_rowid <= s.vp_rowid
    )
FROM vp_repro_databend925_1274_l AS s;