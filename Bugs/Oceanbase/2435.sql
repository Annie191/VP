DROP DATABASE IF EXISTS repro_ob730_db13_min;
CREATE DATABASE repro_ob730_db13_min;
USE repro_ob730_db13_min;

CREATE TABLE src (
  vp_rowid BIGINT NOT NULL PRIMARY KEY,
  c0 DOUBLE NULL
) PARTITION BY HASH(vp_rowid) PARTITIONS 8;

CREATE TABLE l (
  vp_rowid BIGINT NOT NULL PRIMARY KEY,
  c0 DOUBLE NULL
) PARTITION BY HASH(vp_rowid) PARTITIONS 8;

CREATE TABLE r (
  vp_rowid BIGINT NOT NULL PRIMARY KEY
) PARTITION BY HASH(vp_rowid) PARTITIONS 8;

INSERT INTO src VALUES (1,-0.999),(2,-0.999);
INSERT INTO src VALUES (3,-0.999),(4,-0.999);
INSERT INTO src VALUES
  (5,-0.999),(6,-0.999),(7,-0.999),(8,-0.999);
INSERT INTO src VALUES
  (9,-0.999),(10,-0.999),(11,-0.999),(12,-0.999),
  (13,-0.999),(14,-0.999),(15,-0.999);

INSERT INTO l SELECT * FROM src;
INSERT INTO r SELECT vp_rowid FROM src;

SET SESSION ob_enable_aggregation_pushdown = OFF;

-- 单表
SELECT /*+ PARALLEL(4) */ STD(c0) AS single_std, VAR_POP(c0) AS single_var
FROM src;

-- 多表重建，保留原日志的 JSON 派生表 JOIN 结构
SELECT /*+ PARALLEL(4) */ STD(c0) AS split_std, VAR_POP(c0) AS split_var
FROM (
  SELECT l.vp_rowid, l.c0
  FROM l
  JOIN (
    SELECT vp_rowid, JSON_OBJECT('vp', vp_rowid) AS payload
    FROM r
  ) jp ON l.vp_rowid = jp.vp_rowid
  JOIN r ON r.vp_rowid = jp.vp_rowid
) AS src;