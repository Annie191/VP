DROP DATABASE IF EXISTS repro18_min;
CREATE DATABASE repro18_min DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
USE repro18_min;

CREATE TABLE src (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  c0 BOOL
);

INSERT INTO src(c0) VALUES (false), (true);

CREATE TABLE l (
  id BIGINT NOT NULL PRIMARY KEY
);

CREATE TABLE r (
  id BIGINT NOT NULL PRIMARY KEY,
  c0 BOOL
);

INSERT INTO l SELECT id FROM src;
INSERT INTO r SELECT id, c0 FROM src;

-- Single-table query (returns wrong count)
SELECT COUNT(CAST(
         CASE WHEN FALSE
              THEN c0
              ELSE CAST(c0 AS TIME)
         END AS DATETIME
       )) AS single_cnt
FROM src;

-- Join query (returns correct count)
SELECT COUNT(CAST(
         CASE WHEN FALSE
              THEN r.c0
              ELSE CAST(r.c0 AS TIME)
         END AS DATETIME
       )) AS join_cnt
FROM l JOIN r ON l.id = r.id;