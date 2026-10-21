DROP DATABASE IF EXISTS repro_databend916_db8_min;
CREATE DATABASE repro_databend916_db8_min;
USE repro_databend916_db8_min;

SET enable_query_result_cache = 0;

-- 单表基准，MEMORY 引擎。
CREATE TABLE vp_source (
    vp_rowid BIGINT NOT NULL
) ENGINE = MEMORY;

INSERT INTO vp_source VALUES (1), (2), (3), (4);

-- 纵向分区左表。分三次 INSERT 是触发条件：
-- 形成 [1,4]、[2]、[3] 三个 FUSE block。
CREATE TABLE vp_left (
    vp_rowid BIGINT NOT NULL
) ENGINE = FUSE;

INSERT INTO vp_left VALUES (1), (4);
INSERT INTO vp_left VALUES (2);
INSERT INTO vp_left VALUES (3);

-- 纵向分区右表。
CREATE TABLE vp_right (
    vp_rowid BIGINT NOT NULL
) ENGINE = FUSE;

INSERT INTO vp_right
SELECT vp_rowid FROM vp_source;

-- 保留原日志的 TEMP_TABLE + 多表 JOIN 重建结构。
CREATE TEMPORARY TABLE vp_joined (
    vp_rowid BIGINT NOT NULL
);

INSERT INTO vp_joined
SELECT l.vp_rowid
FROM vp_left AS l
JOIN vp_right AS r
  ON l.vp_rowid = r.vp_rowid;

-- 单表基准查询。
SELECT vp_rowid
FROM vp_source
ORDER BY vp_rowid
LIMIT 3;

-- 多表重建并物化后的查询。
SELECT vp_rowid
FROM vp_joined
ORDER BY vp_rowid
LIMIT 3;
