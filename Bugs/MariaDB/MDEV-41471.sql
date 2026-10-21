DROP DATABASE IF EXISTS repro_mariadb815_db10_min;
CREATE DATABASE repro_mariadb815_db10_min;
USE repro_mariadb815_db10_min;

SET SESSION sql_buffer_result = OFF;

CREATE TABLE src (
    rid INT,
    id INT PRIMARY KEY,
    j LONGTEXT
) ENGINE=InnoDB;

INSERT INTO src VALUES (1, 1, '{}');

CREATE TABLE l (
    rid INT PRIMARY KEY,
    id INT
) ENGINE=InnoDB;

CREATE TABLE r (
    rid INT PRIMARY KEY,
    j LONGTEXT
) ENGINE=InnoDB;

INSERT INTO l SELECT rid, id FROM src;
INSERT INTO r SELECT rid, j FROM src;

DROP TEMPORARY TABLE IF EXISTS rebuilt;
CREATE TEMPORARY TABLE rebuilt (
    rid INT PRIMARY KEY,
    id INT,
    j LONGTEXT
) ENGINE=InnoDB;

INSERT INTO rebuilt
SELECT l.rid, l.id, r.j
FROM l JOIN r USING (rid);

-- Q1：单表查询
SELECT DISTINCT id, CONCAT(CAST(-j AS CHAR), 'a') AS v
FROM src;

-- Q2：两表 JOIN 重建临时表后的查询
SELECT DISTINCT id, CONCAT(CAST(-j AS CHAR), 'a') AS v
FROM rebuilt;
