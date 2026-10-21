DROP DATABASE IF EXISTS repro8_final;
CREATE DATABASE repro8_final;
USE repro8_final;

CREATE TABLE src (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    c1 FLOAT NULL
) ENGINE=InnoDB;

INSERT INTO src(c1) VALUES (NULL);

CREATE TABLE l (
    id BIGINT NOT NULL PRIMARY KEY,
    c1 FLOAT NULL,
    KEY li(id,c1)
) ENGINE=InnoDB;

CREATE TABLE r (
    id BIGINT NOT NULL PRIMARY KEY
) ENGINE=InnoDB;

INSERT INTO l SELECT id,c1 FROM src;
INSERT INTO r SELECT id FROM src;

-- Single: 返回 1043103758
SELECT 'single' AS q,
    SUM(DISTINCT CASE 0.5 WHEN id THEN c1 ELSE 1043103758 END) AS ref0
FROM src
GROUP BY id;

-- Split: 返回 1043103744
SELECT 'split' AS q,
    SUM(DISTINCT CASE 0.5 WHEN s.id THEN s.c1 ELSE 1043103758 END) AS ref0
FROM (
    SELECT l.id AS id, l.c1 AS c1
    FROM l
    JOIN (
        SELECT id, CONCAT('vp:', id) AS payload
        FROM r
    ) sp ON CONCAT('vp:', l.id) = sp.payload
    JOIN r ON r.id = sp.id
) s
GROUP BY id;
