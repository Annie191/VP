DROP DATABASE IF EXISTS repro_cockroachdb803_db6_sqrdiff_plan CASCADE;
CREATE DATABASE repro_cockroachdb803_db6_sqrdiff_plan;
USE repro_cockroachdb803_db6_sqrdiff_plan;

SET vectorize = 'off';
SET distsql = 'always';

CREATE TABLE source (
    vp_rowid INT8 PRIMARY KEY,
    c1 DECIMAL NOT NULL
);

CREATE TABLE vp_l (
    vp_rowid INT8 PRIMARY KEY,
    c1 DECIMAL NOT NULL
);

CREATE TABLE vp_r (
    vp_rowid INT8 PRIMARY KEY
);

INSERT INTO source
SELECT i, 99999999999999999999.9999999999::DECIMAL
FROM generate_series(1, 11) AS g(i);

INSERT INTO vp_l SELECT vp_rowid, c1 FROM source;
INSERT INTO vp_r SELECT vp_rowid FROM source;

ALTER TABLE source SPLIT AT VALUES (3), (5), (7), (9);
ALTER TABLE vp_l SPLIT AT VALUES (3), (5), (7), (9);
ALTER TABLE vp_r SPLIT AT VALUES (3), (5), (7), (9);

ALTER TABLE source SCATTER;
ALTER TABLE vp_l SCATTER;
ALTER TABLE vp_r SCATTER;

SELECT 'single' AS shape, SQRDIFF(c1)
FROM source;

SELECT 'lateral' AS shape, SQRDIFF(c1)
FROM vp_l l
JOIN LATERAL (
    SELECT *
    FROM vp_r r
    WHERE r.vp_rowid = l.vp_rowid
) r ON TRUE;
