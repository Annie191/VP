DROP DATABASE IF EXISTS repro_mysql714_db8;
CREATE DATABASE repro_mysql714_db8;
USE repro_mysql714_db8;
DROP TABLE IF EXISTS vp_s, vp_l, vp_r;
SET SESSION sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';
SET SESSION big_tables = 1;
CREATE TABLE vp_s (
    vp_rowid BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    c0       DECIMAL(65, 30) NULL
) ENGINE = InnoDB;
INSERT INTO vp_s (c0) VALUES (NULL), (0), (0), (1);
ALTER TABLE vp_s ADD KEY i0 (c0 DESC);
CREATE TABLE vp_l (vp_rowid BIGINT NOT NULL PRIMARY KEY, c0 DECIMAL(65,30) NULL) ENGINE = InnoDB;
CREATE TABLE vp_r (vp_rowid BIGINT NOT NULL PRIMARY KEY, c0 DECIMAL(65,30) NULL) ENGINE = InnoDB;
INSERT INTO vp_l SELECT vp_rowid, c0 FROM vp_s;
INSERT INTO vp_r SELECT vp_rowid, c0 FROM vp_s;
CREATE INDEX l_idx ON vp_l(c0);

-- ORIGINAL SINGLE-TABLE QUERY
-- 单表查询（covering index → IFNULL 正确 → c0=0 的行通过）
SELECT COUNT(*) FROM vp_s
WHERE 1 NOT IN (IFNULL(NULL, vp_s.c0), CAST('abc' AS SIGNED));

-- TLP BASE (original query with WHERE removed)
/* 单表查询（covering index → IFNULL 正确 → c0=0 的行通过） */ SELECT COUNT(*) FROM vp_s;

-- TLP UNION ALL
SELECT SUM(agg0) FROM (/* 单表查询（covering index → IFNULL 正确 → c0=0 的行通过） */ SELECT COUNT(*) AS agg0 FROM vp_s WHERE NOT 1 IN (COALESCE(NULL, vp_s.c0), CAST('abc' AS SIGNED)) UNION ALL /* 单表查询（covering index → IFNULL 正确 → c0=0 的行通过） */ SELECT COUNT(*) AS agg0 FROM vp_s WHERE NOT NOT 1 IN (COALESCE(NULL, vp_s.c0), CAST('abc' AS SIGNED)) UNION ALL /* 单表查询（covering index → IFNULL 正确 → c0=0 的行通过） */ SELECT COUNT(*) AS agg0 FROM vp_s WHERE NOT 1 IN (COALESCE(NULL, vp_s.c0), CAST('abc' AS SIGNED)) IS NULL) AS tlp_parts;

-- NoREC optimized
/* 单表查询（covering index → IFNULL 正确 → c0=0 的行通过） */ SELECT COUNT(*) FROM vp_s WHERE NOT 1 IN (COALESCE(NULL, vp_s.c0), CAST('abc' AS SIGNED));

-- NoREC unoptimized
/* 单表查询（covering index → IFNULL 正确 → c0=0 的行通过） */ SELECT SUM(CASE WHEN NOT 1 IN (COALESCE(NULL, vp_s.c0), CAST('abc' AS SIGNED)) THEN 1 ELSE NULL END) FROM vp_s;
