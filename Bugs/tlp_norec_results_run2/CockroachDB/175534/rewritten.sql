DROP DATABASE IF EXISTS repro_cockroach804_db8_min CASCADE;
CREATE DATABASE repro_cockroach804_db8_min;
USE repro_cockroach804_db8_min;
CREATE TABLE vp_source (
    vp_rowid INT8 PRIMARY KEY,
    c0 BOOL NULL
);
CREATE TABLE vp_l (
    vp_rowid INT8 PRIMARY KEY
);
CREATE TABLE vp_r (
    vp_rowid INT8 PRIMARY KEY,
    c0 BOOL NULL
);
INSERT INTO vp_source
SELECT i, CASE WHEN i = 13 THEN false ELSE NULL END
FROM generate_series(1, 13) AS g(i);
INSERT INTO vp_l
SELECT vp_rowid FROM vp_source;
INSERT INTO vp_r
SELECT vp_rowid, c0 FROM vp_source;
CREATE INDEX vp_r_partial
ON vp_r(c0)
WHERE c0 IS NOT NULL;

-- ORIGINAL SINGLE-TABLE QUERY
-- 单表查询：应返回 7 行 NULL
SELECT c0
FROM vp_source
ORDER BY c0, vp_rowid
LIMIT 3 OFFSET 4;

-- SKIPPED TLP/NoREC: ORDER BY is not handled by this batch rewriter
