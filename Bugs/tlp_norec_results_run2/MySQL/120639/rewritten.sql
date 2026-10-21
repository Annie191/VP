DROP DATABASE IF EXISTS test_db2;
CREATE DATABASE test_db2;
USE test_db2;
-- 原始表
CREATE TABLE s (
  id BIGINT NOT NULL,
  KEY (id)
) ENGINE = InnoDB;
INSERT INTO s VALUES (1);
-- VP source：单表形式
CREATE TABLE vp_source (
  vp_rowid BIGINT NOT NULL,
  id BIGINT NOT NULL,
  PRIMARY KEY (vp_rowid),
  KEY (id)
) ENGINE = InnoDB;
INSERT INTO vp_source (vp_rowid, id)
SELECT id, id
FROM s;
-- VP split：多表形式
CREATE TABLE vp_l (
  vp_rowid BIGINT NOT NULL,
  PRIMARY KEY (vp_rowid)
) ENGINE = InnoDB;
CREATE TABLE vp_r (
  vp_rowid BIGINT NOT NULL,
  id BIGINT NOT NULL,
  PRIMARY KEY (vp_rowid),
  KEY (id)
) ENGINE = InnoDB;
INSERT INTO vp_l (vp_rowid)
SELECT id
FROM s;
INSERT INTO vp_r (vp_rowid, id)
SELECT id, id
FROM s;

-- ORIGINAL SINGLE-TABLE QUERY
SELECT COUNT(*) AS cnt
FROM vp_source
WHERE NOT (IFNULL(0.5, 0) NOT IN (id));

-- TLP BASE (original query with WHERE removed)
SELECT COUNT(*) AS cnt FROM vp_source;

-- TLP UNION ALL
SELECT SUM(agg0) FROM (SELECT COUNT(*) AS agg0 FROM vp_source WHERE NOT (NOT COALESCE(0.5, 0) IN (id)) UNION ALL SELECT COUNT(*) AS agg0 FROM vp_source WHERE NOT NOT (NOT COALESCE(0.5, 0) IN (id)) UNION ALL SELECT COUNT(*) AS agg0 FROM vp_source WHERE NOT (NOT COALESCE(0.5, 0) IN (id)) IS NULL) AS tlp_parts;

-- NoREC optimized
SELECT COUNT(*) FROM vp_source WHERE NOT (NOT COALESCE(0.5, 0) IN (id));

-- NoREC unoptimized
SELECT SUM(CASE WHEN NOT (NOT COALESCE(0.5, 0) IN (id)) THEN 1 ELSE NULL END) FROM vp_source;
