DROP DATABASE IF EXISTS repro_databend914_db56_min;
CREATE DATABASE repro_databend914_db56_min;
USE repro_databend914_db56_min;
CREATE TABLE source (
    id INT,
    a ARRAY(INT) NULL
) ENGINE=FUSE;
CREATE TABLE part_l (
    id INT
) ENGINE=FUSE;
CREATE TABLE part_r (
    id INT,
    a ARRAY(INT) NULL
) ENGINE=FUSE;
INSERT INTO source VALUES (1, NULL);
INSERT INTO part_l SELECT id FROM source;
INSERT INTO part_r SELECT id, a FROM source;

-- ORIGINAL SINGLE-TABLE QUERY
SELECT ARRAY_AGG(a)
FROM source;

-- SKIPPED TLP/NoREC: aggregate ARRAYAGG is not supported by this TLP rewrite
