DROP DATABASE IF EXISTS repro_mariadb809_db16_min;
CREATE DATABASE repro_mariadb809_db16_min;
USE repro_mariadb809_db16_min;

CREATE TABLE base_rows (
    c0 BIT(8) NOT NULL
) ENGINE=MyISAM;

INSERT INTO base_rows(c0)
VALUES (1),(2),(3),(4),(5),(6),(7),(8);

CREATE TABLE vp_source (
    vp_rowid BIGINT NOT NULL,
    c0 BIT(8) NOT NULL,
    PRIMARY KEY (vp_rowid)
) ENGINE=InnoDB;

INSERT INTO vp_source(vp_rowid, c0)
SELECT ROW_NUMBER() OVER (ORDER BY c0), c0
FROM base_rows;

CREATE TABLE vp_left (
    vp_rowid BIGINT NOT NULL PRIMARY KEY,
    c0 BIT(8) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE vp_right (
    vp_rowid BIGINT NOT NULL PRIMARY KEY,
    c0 BIT(8) NOT NULL
) ENGINE=InnoDB;

INSERT INTO vp_left
SELECT vp_rowid, c0 FROM vp_source;

INSERT INTO vp_right
SELECT vp_rowid, c0 FROM vp_source;

CREATE INDEX idx_left_c0 ON vp_left(c0);

SELECT DISTINCT
    CAST(LENGTHB(c0) AS UNSIGNED) AS result_value
FROM vp_source
WHERE c0 IN ('3bf0a4fa-72ff-433c-9a26-eecf4d741c6c');

SELECT DISTINCT
    CAST(LENGTHB(c0) AS UNSIGNED) AS result_value
FROM (
    SELECT
        l.vp_rowid,
        l.c0 AS c0
    FROM vp_left AS l FORCE INDEX (idx_left_c0)
    JOIN (
        SELECT
            vp_rowid,
            JSON_OBJECT('vp', vp_rowid) AS payload
        FROM vp_right
    ) AS p
        ON p.vp_rowid = l.vp_rowid
    JOIN vp_right AS r
        ON r.vp_rowid = p.vp_rowid
) AS reconstructed
WHERE c0 IN ('3bf0a4fa-72ff-433c-9a26-eecf4d741c6c');