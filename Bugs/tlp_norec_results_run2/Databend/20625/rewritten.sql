DROP DATABASE IF EXISTS repro_databend917_db0_min;
CREATE DATABASE repro_databend917_db0_min;
USE repro_databend917_db0_min;
CREATE TABLE source (
    vp_rowid BIGINT NOT NULL,
    c1date DATE NULL,
    c2map MAP(VARCHAR, INT64) NULL,
    c3array ARRAY(INT64) NULL,
    c4array ARRAY(INT64) NULL
) ENGINE=FUSE;
CREATE TABLE left_part (
    vp_rowid BIGINT NOT NULL,
    c1date DATE NULL,
    c3array ARRAY(INT64) NULL,
    c4array ARRAY(INT64) NULL
) ENGINE=FUSE;
CREATE TABLE right_part (
    vp_rowid BIGINT NOT NULL,
    c2map MAP(VARCHAR, INT64) NULL
) ENGINE=FUSE;
INSERT INTO source VALUES
(1, DATE '1969-12-23', {'sqlancer':1}, [1,2,3], [1,2,3]),
(2, DATE '1970-01-15', {'sqlancer':1}, [1,2,3], [1,2,3]),
(3, NULL, NULL, [1,2,3], NULL),
(4, DATE '1969-12-31', {'sqlancer':1}, NULL, NULL);
INSERT INTO source VALUES (5, NULL, NULL, [1], NULL);
INSERT INTO left_part
SELECT vp_rowid, c1date, c3array, c4array FROM source;
INSERT INTO right_part
SELECT vp_rowid, c2map FROM source;

-- ORIGINAL SINGLE-TABLE QUERY
SELECT MIN(s.c1date), s.vp_rowid, s.vp_rowid
FROM source s
WHERE ((s.c2map IS NULL) <> (s.c4array IS NULL))
GROUP BY s.vp_rowid, s.vp_rowid
ORDER BY s.vp_rowid, s.vp_rowid;

-- SKIPPED TLP/NoREC: GROUP BY is not handled by this batch rewriter
