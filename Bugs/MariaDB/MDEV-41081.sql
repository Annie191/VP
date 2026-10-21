DROP DATABASE IF EXISTS repro_mariadb808_db12_min;
CREATE DATABASE repro_mariadb808_db12_min;
USE repro_mariadb808_db12_min;

CREATE TABLE source_rows (
    vp_rowid BIGINT NOT NULL PRIMARY KEY,
    c0 TINYINT(1),
    c1 CHAR(1) NOT NULL,
    KEY idx_source (vp_rowid, c0)
);

CREATE TABLE left_rows (
    vp_rowid BIGINT NOT NULL PRIMARY KEY,
    c0 TINYINT(1),
    KEY idx_left (c0, vp_rowid)
);

CREATE TABLE right_rows (
    vp_rowid BIGINT NOT NULL PRIMARY KEY,
    c1 CHAR(1) NOT NULL
);

INSERT INTO source_rows VALUES
    (1, 0, 'a'),
    (2, 0, 'b'),
    (3, 1, 'c');

INSERT INTO left_rows
SELECT vp_rowid, c0 FROM source_rows;

INSERT INTO right_rows
SELECT vp_rowid, c1 FROM source_rows;

-- 原始单表查询：实际返回 0 行
SELECT vp_rowid, c0
FROM source_rows
WHERE '0.45' IN (c0);

-- VP 多表重构查询：错误返回 c0=0 的两行
SELECT vp_rowid, c0
FROM (
    SELECT l.vp_rowid, l.c0, r.c1
    FROM left_rows AS l
    LEFT JOIN right_rows AS r
        ON r.vp_rowid = l.vp_rowid
) AS reconstructed
WHERE '0.45' IN (c0);