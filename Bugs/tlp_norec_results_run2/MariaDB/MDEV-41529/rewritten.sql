-- No top-level single-table SELECT found.
DROP DATABASE IF EXISTS repro_mariadb817_db12_min;
CREATE DATABASE repro_mariadb817_db12_min;
USE repro_mariadb817_db12_min;

CREATE TABLE s (
    vp_rowid BIGINT NOT NULL,
    c0 BIGINT NOT NULL,
    c1 DATETIME NOT NULL,
    PRIMARY KEY (c1),
    UNIQUE KEY (vp_rowid),
    KEY idx_s (c0)
) ENGINE=InnoDB;

CREATE TABLE lpart (
    vp_rowid BIGINT NOT NULL PRIMARY KEY,
    c0 BIGINT NOT NULL,
    c1 DATETIME NOT NULL
) ENGINE=InnoDB;

CREATE TABLE rpart (
    vp_rowid BIGINT NOT NULL PRIMARY KEY,
    c1 DATETIME NOT NULL
) ENGINE=InnoDB;

INSERT INTO s VALUES
(1, 0, '0000-00-00 00:00:00'),
(2, 2, '2000-01-01 00:00:02'),
(3, 3, '2000-01-01 00:00:03'),
(4, 4, '2000-01-01 00:00:04'),
(5, 5, '2000-01-01 00:00:05'),
(6, 6, '2000-01-01 00:00:06'),
(7, 7, '2000-01-01 00:00:07'),
(8, 8, '2000-01-01 00:00:08');

INSERT INTO lpart SELECT * FROM s;
INSERT INTO rpart SELECT vp_rowid, c1 FROM s;

SET SESSION optimizer_switch='derived_merge=off';

-- 单表路径
SELECT *
FROM (
    SELECT ALL c0, 45, false
    FROM s
    WHERE (('2096-02-25 15:30:07' NOT IN ('b,c', c0)) IN (c1))
) q
ORDER BY 1, 2, 3;

-- CTE + 多表重建路径
SELECT *
FROM (
    SELECT ALL c0, 45, false
    FROM (
        WITH l AS (SELECT * FROM lpart),
             r AS (SELECT * FROM rpart)
        SELECT l.vp_rowid, l.c0, r.c1
        FROM l JOIN r ON l.vp_rowid = r.vp_rowid
    ) reconstructed
    WHERE (('2096-02-25 15:30:07' NOT IN ('b,c', c0)) IN (c1))
) q
ORDER BY 1, 2, 3;
