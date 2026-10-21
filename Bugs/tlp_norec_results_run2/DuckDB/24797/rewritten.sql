DROP TABLE IF EXISTS repro_duckdb_db5_hist_min_source;
DROP TABLE IF EXISTS repro_duckdb_db5_hist_min_l;
DROP TABLE IF EXISTS repro_duckdb_db5_hist_min_r;
CREATE TABLE repro_duckdb_db5_hist_min_source (
    vp_rowid BIGINT,
    c0 DOUBLE,
    c1 BLOB
);
-- 按日志中 source 物化的物理顺序插入
INSERT INTO repro_duckdb_db5_hist_min_source VALUES
    (4, -1e308, NULL),
    (3, '-Infinity'::DOUBLE, NULL),
    (0, 0.0, NULL),
    (1, 'Infinity'::DOUBLE, NULL),
    (2, 'NaN'::DOUBLE, NULL);
CREATE TABLE repro_duckdb_db5_hist_min_l AS
SELECT vp_rowid, c1, c0
FROM repro_duckdb_db5_hist_min_source;
CREATE TABLE repro_duckdb_db5_hist_min_r AS
SELECT vp_rowid, c0
FROM repro_duckdb_db5_hist_min_source;

-- ORIGINAL SINGLE-TABLE QUERY
-- 单表查询
SELECT
    quantile(
        CASE WHEN vp_rowid % 4 = 0 THEN NULL ELSE c0 END,
        0.5
    ) AS median,
    histogram(
        CASE WHEN vp_rowid % 4 = 0 THEN NULL ELSE c0 END
    ) AS hist
FROM repro_duckdb_db5_hist_min_source;

-- SKIPPED TLP/NoREC: only one top-level aggregate expression is supported
