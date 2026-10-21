DROP DATABASE IF EXISTS repro_databend920_db3_min;
CREATE DATABASE repro_databend920_db3_min;
USE repro_databend920_db3_min;

SET enable_query_result_cache = 0;

CREATE TABLE s (
    id BIGINT NOT NULL,
    a DOUBLE NULL,
    b DOUBLE NOT NULL,
    g GEOMETRY NULL
) ENGINE = FUSE CLUSTER BY (id);

CREATE TABLE l (
    id BIGINT NOT NULL,
    a DOUBLE NULL
) ENGINE = FUSE CLUSTER BY (id);

CREATE TABLE r (
    id BIGINT NOT NULL,
    b DOUBLE NULL,
    g GEOMETRY NULL
) ENGINE = FUSE CLUSTER BY (id);

INSERT INTO s VALUES
    (1, 1.0,  1.0, ST_GEOMETRYFROMWKT('POINT(1 2)')),
    (2, NULL, 0.0, ST_GEOMETRYFROMWKT('POINT(0 0)'));

INSERT INTO l
SELECT id, a FROM s;

INSERT INTO r
SELECT id, b, g FROM s;

SELECT id, HEX(ST_ASWKB(g))
FROM s
WHERE b = a
ORDER BY id;

SELECT id, HEX(ST_ASWKB(g))
FROM (
    SELECT
        l.id,
        l.a,
        (SELECT r.b FROM r WHERE r.id = l.id) AS b,
        (SELECT r.g FROM r WHERE r.id = l.id) AS g
    FROM l
    WHERE l.id IN (
        SELECT id FROM r
    )
) x
WHERE b = a
ORDER BY id;