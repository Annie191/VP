DROP DATABASE IF EXISTS repro18_final;
CREATE DATABASE repro18_final;
USE repro18_final;

CREATE TABLE src (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    c0 BIGINT NULL,
    UNIQUE KEY i0(c0)
) ENGINE=InnoDB;

INSERT INTO src(c0) VALUES (0),(32767);

CREATE TABLE l (
    id BIGINT NOT NULL PRIMARY KEY,
    c0 BIGINT NULL,
    KEY l_i(id,c0)
) ENGINE=InnoDB;

CREATE TABLE r (
    id BIGINT NOT NULL PRIMARY KEY,
    c0 BIGINT NULL
) ENGINE=InnoDB;

INSERT INTO l SELECT id,c0 FROM src;
INSERT INTO r SELECT id,c0 FROM src;

-- Single: 返回 2 行
SELECT DISTINCTROW
    'single' AS q,
    ('x') OR (CAST((src.c0) XOR (src.id) AS SIGNED)) AS ref0,
    (((src.id) < (src.id)) >= ((src.c0) NOT IN (3.35077561E8))) IN (NULL) AS ref1,
    COUNT(DISTINCT (src.c0) IS NOT TRUE) AS ref2
FROM src
GROUP BY
    (((src.id) < (src.id)) >= ((src.c0) NOT IN (3.35077561E8))) IN (NULL),
    ('x') OR (CAST((src.c0) XOR (src.id) AS SIGNED));

-- Split: 返回 1 行
SELECT DISTINCTROW
    'split' AS q,
    ('x') OR (CAST((s.c0) XOR (s.id) AS SIGNED)) AS ref0,
    (((s.id) < (s.id)) >= ((s.c0) NOT IN (3.35077561E8))) IN (NULL) AS ref1,
    COUNT(DISTINCT (s.c0) IS NOT TRUE) AS ref2
FROM (
    SELECT id, l.c0 AS c0
    FROM l JOIN r USING(id)
) s
GROUP BY
    (((s.id) < (s.id)) >= ((s.c0) NOT IN (3.35077561E8))) IN (NULL),
    ('x') OR (CAST((s.c0) XOR (s.id) AS SIGNED));