DROP DATABASE IF EXISTS repro_mariadb810_db0_min;
CREATE DATABASE repro_mariadb810_db0_min;
USE repro_mariadb810_db0_min;
SET SESSION sql_buffer_result = 0;
SET SESSION optimizer_switch = 'derived_merge=on';
CREATE TABLE src (
    c DATETIME NOT NULL UNIQUE
) ENGINE=MyISAM;
CREATE TABLE lt (
    id INT PRIMARY KEY,
    c DATETIME NOT NULL
) ENGINE=MyISAM;
CREATE TABLE rt (
    id INT PRIMARY KEY
) ENGINE=MyISAM;
INSERT INTO src VALUES ('2000-01-01'), ('2000-01-02');
INSERT INTO lt VALUES (1, '2000-01-01'), (2, '2000-01-02');
INSERT INTO rt VALUES (1), (2);

-- ORIGINAL SINGLE-TABLE QUERY
-- 单表查询
SELECT DISTINCTROW c, NULLIF(-TRUE, c) AS v
FROM src;

-- SKIPPED TLP/NoREC: DISTINCT is not handled by this batch rewriter
