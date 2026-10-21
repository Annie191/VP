-- No top-level single-table SELECT found.
DROP DATABASE IF EXISTS repro_mariadb806_db1_min6;
CREATE DATABASE repro_mariadb806_db1_min6
  CHARACTER SET utf8mb4 COLLATE utf8mb4_uca1400_ai_ci;
USE repro_mariadb806_db1_min6;

CREATE TABLE t4 (
  c0 VARCHAR(100) NOT NULL,
  PRIMARY KEY (c0)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

INSERT INTO t4 VALUES ('⳾v*');

CREATE TABLE vp_mariadb_database1_937_source LIKE t4;
ALTER TABLE vp_mariadb_database1_937_source
  ADD COLUMN vp_rowid BIGINT NOT NULL FIRST;
ALTER TABLE vp_mariadb_database1_937_source ENGINE=InnoDB;

INSERT INTO vp_mariadb_database1_937_source (vp_rowid, c0)
SELECT ROW_NUMBER() OVER (ORDER BY c0), c0
FROM t4;

CREATE TABLE vp_mariadb_database1_937_left (
  vp_rowid BIGINT NOT NULL,
  c0 VARCHAR(100)
    CHARACTER SET utf8mb4 COLLATE utf8mb4_uca1400_ai_ci
) ENGINE=InnoDB;

CREATE TABLE vp_mariadb_database1_937_right (
  vp_rowid BIGINT NOT NULL
) ENGINE=InnoDB;

INSERT INTO vp_mariadb_database1_937_left
SELECT vp_rowid, c0 FROM vp_mariadb_database1_937_source;

INSERT INTO vp_mariadb_database1_937_right
SELECT vp_rowid FROM vp_mariadb_database1_937_source;

SET NAMES utf8mb4 COLLATE utf8mb4_uca1400_ai_ci;

-- 单表查询
SELECT *
FROM (
  SELECT TRIM(ORD(c0)) AS vp_ast_col_0,
         -1914373596 AS vp_ast_col_1,
         c0 AS vp_ast_col_2,
         c0 AS vp_ast_col_3,
         '' AS vp_ast_col_4
  FROM vp_mariadb_database1_937_source
) q
WHERE q.vp_ast_col_2 = '⳾v*'
ORDER BY 1, 2, 3, 4, 5;

-- 多表重写查询
SELECT *
FROM (
  SELECT TRIM(ORD(c0)) AS vp_ast_col_0,
         -1914373596 AS vp_ast_col_1,
         c0 AS vp_ast_col_2,
         c0 AS vp_ast_col_3,
         '' AS vp_ast_col_4
  FROM (
    SELECT l.vp_rowid, l.c0
    FROM vp_mariadb_database1_937_left l
    JOIN (
      SELECT vp_rowid, CONCAT('vp:', vp_rowid) AS payload
      FROM vp_mariadb_database1_937_right
    ) p ON p.payload = CONCAT('vp:', l.vp_rowid)
    JOIN vp_mariadb_database1_937_right r
      ON r.vp_rowid = p.vp_rowid
  ) vp_rel
) q
WHERE q.vp_ast_col_2 = '⳾v*'
ORDER BY 1, 2, 3, 4, 5;