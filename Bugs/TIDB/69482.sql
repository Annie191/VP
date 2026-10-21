DROP DATABASE IF EXISTS repro_tidb626_db3;
CREATE DATABASE repro_tidb626_db3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
USE repro_tidb626_db3;

SET SESSION sql_mode='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION';

CREATE TABLE src (
  vp_rowid BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  c0 TEXT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

INSERT INTO src(c0) VALUES (' '), (''), ('y');

CREATE TABLE l (vp_rowid BIGINT NOT NULL PRIMARY KEY) ENGINE=InnoDB;
CREATE TABLE r (
  vp_rowid BIGINT NOT NULL PRIMARY KEY,
  c0 TEXT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

INSERT INTO l SELECT vp_rowid FROM src;
INSERT INTO r SELECT vp_rowid, c0 FROM src;

CREATE VIEW v AS
SELECT l.vp_rowid, r.c0
FROM l JOIN r ON l.vp_rowid = r.vp_rowid;

-- Single‑table query (wrong: returns space ' ')
SELECT 'single' AS q, CONCAT('[', MIN(c0), ']') AS shown, HEX(MIN(c0)) AS hex, LENGTH(MIN(c0)) AS len
FROM src;

-- View query (correct: returns empty string)
SELECT 'split' AS q, CONCAT('[', MIN(c0), ']') AS shown, HEX(MIN(c0)) AS hex, LENGTH(MIN(c0)) AS len
FROM v;
