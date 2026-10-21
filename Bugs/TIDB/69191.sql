DROP DATABASE IF EXISTS repro_tidb_old_17;
CREATE DATABASE repro_tidb_old_17 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
USE repro_tidb_old_17;

CREATE TABLE src(
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  c0 DECIMAL(65,30) NULL
);

INSERT INTO src(c0) VALUES
  (0),
  (1);

CREATE TABLE l(id BIGINT NOT NULL PRIMARY KEY);
CREATE TABLE r(id BIGINT NOT NULL PRIMARY KEY, c0 DECIMAL(65,30) NULL);
INSERT INTO l SELECT id FROM src;
INSERT INTO r SELECT id, c0 FROM src;

CREATE VIEW v AS
SELECT l.id AS id, r.c0 AS c0
FROM l JOIN r ON l.id = r.id;

SELECT
  HEX(MIN(BINARY(
    CASE 'ꉈP/<3|9,-'
      WHEN c0 THEN '2020-11-24'
      WHEN c0 THEN id
      ELSE ']'
    END
  ))) AS source_hex,
  MIN(BINARY(
    CASE 'ꉈP/<3|9,-'
      WHEN c0 THEN '2020-11-24'
      WHEN c0 THEN id
      ELSE ']'
    END
  )) AS source_val
FROM src
ORDER BY id;

SELECT
  HEX(MIN(BINARY(
    CASE 'ꉈP/<3|9,-'
      WHEN c0 THEN '2020-11-24'
      WHEN c0 THEN id
      ELSE ']'
    END
  ))) AS split_hex,
  MIN(BINARY(
    CASE 'ꉈP/<3|9,-'
      WHEN c0 THEN '2020-11-24'
      WHEN c0 THEN id
      ELSE ']'
    END
  )) AS split_val
FROM v
ORDER BY id;