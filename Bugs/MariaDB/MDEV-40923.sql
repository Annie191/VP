DROP DATABASE IF EXISTS repro_mariadb805_db3_min;
CREATE DATABASE repro_mariadb805_db3_min;
USE repro_mariadb805_db3_min;

CREATE TABLE source_base (
    c0 DOUBLE UNSIGNED ZEROFILL DEFAULT NULL
) ENGINE=MyISAM;

INSERT INTO source_base VALUES (234567890);

CREATE TABLE vp_source LIKE source_base;
ALTER TABLE vp_source ADD COLUMN vp_rowid BIGINT NOT NULL;

INSERT INTO vp_source (vp_rowid, c0)
SELECT ROW_NUMBER() OVER (ORDER BY c0), c0
FROM source_base;

CREATE TABLE vp_left (
    vp_rowid BIGINT NOT NULL
) ENGINE=MyISAM;

CREATE TABLE vp_right (
    vp_rowid BIGINT NOT NULL,
    c0 DOUBLE UNSIGNED ZEROFILL DEFAULT NULL
) ENGINE=MyISAM;

INSERT INTO vp_left
SELECT vp_rowid FROM vp_source;

INSERT INTO vp_right
SELECT vp_rowid, c0 FROM vp_source;

SELECT 'DIRECT' AS variant,
       c0,
       LOCATE(false, c0) AS locate_result,
       ORD(ORD(c0)) AS value
FROM vp_source
WHERE LOCATE(false, c0);

SELECT 'RECONSTRUCTED' AS variant,
       c0,
       LOCATE(false, c0) AS locate_result,
       ORD(ORD(c0)) AS value
FROM (
    SELECT l.vp_rowid,
           (
               SELECT r.c0
               FROM vp_right r
               WHERE r.vp_rowid = l.vp_rowid
           ) AS c0
    FROM vp_left l
) AS reconstructed
WHERE LOCATE(false, c0);