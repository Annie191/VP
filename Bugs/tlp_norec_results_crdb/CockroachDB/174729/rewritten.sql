DROP DATABASE IF EXISTS repro_crdb803_db8_min;
CREATE DATABASE repro_crdb803_db8_min;
USE repro_crdb803_db8_min;
SET TimeZone = 'UTC';
SET vectorize = 'on';
SET optimizer_use_histograms = 'on';
SET enable_zigzag_join = 'off';
SET distsql = 'auto';
SET opt_split_scan_limit = 2048;
SET serial_normalization = 'rowid';
-- 单表 src
CREATE TABLE src (
  vp_rowid    INT8 PRIMARY KEY,
  c0          FLOAT NULL,
  vp_computed INT8 AS (vp_rowid * 2) STORED
);
-- 垂直拆分：左表 l（c0 + vp_computed）、右表 r（c0）
CREATE TABLE l (
  vp_rowid    INT8 PRIMARY KEY,
  c0          FLOAT NULL,
  vp_computed INT8 AS (vp_rowid * 2) STORED
);
CREATE TABLE r (
  vp_rowid INT8 PRIMARY KEY,
  c0       FLOAT NULL
);
INSERT INTO src (vp_rowid, c0) VALUES
  (1, -1.370533208e+09),
  (2, 0.08560940630775526),
  (3, 0.49184326379793697),
  (4, 0.619883735554361),
  (5, 0.6697518608462573),
  (6, 0.6905402975088976),
  (7, 2.08893773e+08),
  (8, 1.275818667e+09);
INSERT INTO l (vp_rowid, c0) SELECT vp_rowid, c0 FROM src;
INSERT INTO r (vp_rowid, c0) SELECT vp_rowid, c0 FROM src;
CREATE INDEX l_partial ON l(c0) WHERE c0 IS NOT NULL;
CREATE INDEX r_partial ON r(c0) WHERE c0 IS NOT NULL;

-- ORIGINAL SINGLE-TABLE QUERY
-- single（等价于原查询的 GROUP BY 常量分组）
SELECT STDDEV(vp_computed) FROM src GROUP BY CAST(NULL AS INT8);

-- SKIPPED TLP/NoREC: GROUP BY is not handled by this batch rewriter
