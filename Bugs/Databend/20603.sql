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

SELECT ARRAY_AGG(a)
FROM source;

SELECT ARRAY_AGG(r.a)
FROM part_l AS l
JOIN part_r AS r ON l.id = r.id;