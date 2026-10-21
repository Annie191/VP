DROP DATABASE IF EXISTS repro_databend912_db7_min;
CREATE DATABASE repro_databend912_db7_min;
USE repro_databend912_db7_min;
CREATE TABLE source (
    vp_rowid BIGINT NOT NULL,
    c0boolean BOOLEAN,
    c1int BIGINT
) ENGINE=FUSE;
INSERT INTO source VALUES
(1, false, -1),
(2, false, -2),
(3, false, -3),
(4, true,  -4),
(5, true,  -5),
(6, true,  -6),
(7, true,  -7),
(8, true,  -8),
(9, true,  -9);
CREATE TABLE part_l (
    vp_rowid BIGINT NOT NULL,
    c1int BIGINT
) ENGINE=MEMORY;
CREATE TABLE part_r (
    vp_rowid BIGINT NOT NULL,
    c0boolean BOOLEAN
) ENGINE=FUSE;
INSERT INTO part_l
SELECT vp_rowid, c1int FROM source;
INSERT INTO part_r
SELECT vp_rowid, c0boolean FROM source;

-- ORIGINAL SINGLE-TABLE QUERY
SELECT
    SUM(c1int),
    vp_rowid,
    c1int
FROM source
WHERE c0boolean
GROUP BY vp_rowid, c1int
ORDER BY vp_rowid, c1int
LIMIT 3;

-- SKIPPED TLP/NoREC: GROUP BY is not handled by this batch rewriter
