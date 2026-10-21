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

-- ORIGINAL SINGLE-TABLE QUERY
SELECT 'DIRECT' AS variant,
       c0,
       LOCATE(false, c0) AS locate_result,
       ORD(ORD(c0)) AS value
FROM vp_source
WHERE LOCATE(false, c0);

-- TLP BASE (original query with WHERE removed)
SELECT 'DIRECT' AS variant, c0, LOCATE(FALSE, c0) AS locate_result, ORD(ORD(c0)) AS value FROM vp_source;

-- TLP UNION ALL
SELECT 'DIRECT' AS variant, c0, LOCATE(FALSE, c0) AS locate_result, ORD(ORD(c0)) AS value FROM vp_source WHERE LOCATE(FALSE, c0) UNION ALL SELECT 'DIRECT' AS variant, c0, LOCATE(FALSE, c0) AS locate_result, ORD(ORD(c0)) AS value FROM vp_source WHERE NOT LOCATE(FALSE, c0) UNION ALL SELECT 'DIRECT' AS variant, c0, LOCATE(FALSE, c0) AS locate_result, ORD(ORD(c0)) AS value FROM vp_source WHERE LOCATE(FALSE, c0) IS NULL;

-- NoREC optimized
SELECT COUNT(*) FROM vp_source WHERE LOCATE(FALSE, c0);

-- NoREC unoptimized
SELECT SUM(CASE WHEN LOCATE(FALSE, c0) THEN 1 ELSE NULL END) FROM vp_source;
