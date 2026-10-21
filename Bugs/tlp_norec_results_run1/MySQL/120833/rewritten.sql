DROP DATABASE IF EXISTS repro;
CREATE DATABASE repro;
USE repro;
-- ===========================================
-- MySQL 9.6.0 INDEX LOOKUP DOUBLE→BIGINT 截断
-- ===========================================

-- 源表，带索引
CREATE TABLE src (
  vp_rowid BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  c0 BIGINT NULL
) ENGINE=InnoDB;
INSERT INTO src (c0) VALUES (0), (0), (1);
CREATE INDEX i_src ON src(c0, vp_rowid);
-- 垂直分区 l/r（SINGLE_REPLICATED）
CREATE TABLE l (vp_rowid BIGINT NOT NULL PRIMARY KEY, c0 BIGINT NULL) ENGINE=InnoDB;
CREATE TABLE r (vp_rowid BIGINT NOT NULL PRIMARY KEY, c0 BIGINT NULL) ENGINE=InnoDB;
INSERT INTO l (vp_rowid, c0) SELECT vp_rowid, c0 FROM src;
INSERT INTO r (vp_rowid, c0) SELECT vp_rowid, c0 FROM src;
-- Split 路径：temp table 重建（无 c0 索引）
DROP TEMPORARY TABLE IF EXISTS joined;
CREATE TEMPORARY TABLE joined AS
  SELECT l.vp_rowid, l.c0 FROM l JOIN r ON l.vp_rowid = r.vp_rowid;

-- ORIGINAL SINGLE-TABLE QUERY
-- Single query: 查 src（有索引 → INDEX LOOKUP，0.433 截断为 0 → 错误匹配 2 行）
SELECT 'single' AS query_type, COUNT(*) AS row_count
FROM src
WHERE (IF(NULL, 0.3530399531809283, 0.43307147808600643)) IN (c0);

-- SKIPPED TLP/NoREC: only one top-level aggregate expression is supported
