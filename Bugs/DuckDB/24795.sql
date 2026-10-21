DROP TABLE IF EXISTS repro_duckdb_database8_min_source;
DROP TABLE IF EXISTS repro_duckdb_database8_min_l;
DROP TABLE IF EXISTS repro_duckdb_database8_min_r;

CREATE TABLE repro_duckdb_database8_min_source (
    vp_rowid BIGINT,
    c0 VARCHAR COLLATE NOCASE
);

INSERT INTO repro_duckdb_database8_min_source
VALUES
    (0, 'A'),
    (1, 'a'),
    (2, 'b');

-- VP 重构后的多表结构
CREATE TABLE repro_duckdb_database8_min_l AS
SELECT vp_rowid, c0
FROM repro_duckdb_database8_min_source;

CREATE TABLE repro_duckdb_database8_min_r AS
SELECT vp_rowid
FROM repro_duckdb_database8_min_source;

-- 查询 1：单表，使用 GROUP BY ALL
SELECT c0, COUNT(*) AS vp_count
FROM repro_duckdb_database8_min_source
GROUP BY ALL;

-- 查询 2：多表，显式 GROUP BY c0
SELECT c0, COUNT(*) AS vp_count
FROM (
    SELECT l.vp_rowid, l.c0
    FROM repro_duckdb_database8_min_l l
    JOIN repro_duckdb_database8_min_r r
      ON l.vp_rowid = r.vp_rowid
) repro_duckdb_database8_min_source
GROUP BY c0;