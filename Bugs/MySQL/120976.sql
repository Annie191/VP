DROP DATABASE IF EXISTS repro_mysql714_db38;
CREATE DATABASE repro_mysql714_db38;
USE repro_mysql714_db38;

DROP TABLE IF EXISTS vp_s, vp_l, vp_r;

SET SESSION sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';
SET SESSION big_tables = 0;
SET SESSION internal_tmp_mem_storage_engine = 'MEMORY';

CREATE TABLE vp_s (
    vp_rowid BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    c0       DECIMAL(65, 30) NULL,
    c1       DECIMAL(65, 30) NULL
) ENGINE = InnoDB;

INSERT INTO vp_s (c0, c1) VALUES (NULL, NULL), (NULL, 1611551235), (0, NULL);

ALTER TABLE vp_s ADD UNIQUE KEY i2 (c1, c0);

CREATE TABLE vp_l (vp_rowid BIGINT NOT NULL PRIMARY KEY, c0 DECIMAL(65, 30) NULL) ENGINE = InnoDB;
CREATE TABLE vp_r (vp_rowid BIGINT NOT NULL PRIMARY KEY, c1 DECIMAL(65, 30) NULL) ENGINE = InnoDB;

INSERT INTO vp_l SELECT vp_rowid, c0 FROM vp_s;
INSERT INTO vp_r SELECT vp_rowid, c1 FROM vp_s;

CREATE INDEX l_idx ON vp_l(c0);
CREATE INDEX r_idx ON vp_r(c1);

-- 单表查询（covering index → CAST 正确）
SELECT DISTINCTROW MAX(DISTINCT vp_s.vp_rowid) AS ref0 FROM vp_s
WHERE CAST(IFNULL(vp_s.c0, vp_s.c1) AS SIGNED);

-- LATERAL 重构（JOIN → CAST 异常）
SELECT DISTINCTROW MAX(DISTINCT vp_rowid) AS ref0 FROM (
    SELECT l.vp_rowid AS vp_rowid, l.c0 AS c0, r.c1 AS c1
    FROM vp_l l
    JOIN LATERAL (SELECT * FROM vp_r rr WHERE rr.vp_rowid = l.vp_rowid) r ON TRUE
) vp_source
WHERE CAST(IFNULL(vp_source.c0, vp_source.c1) AS SIGNED);