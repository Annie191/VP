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

-- 多表 VP 重构查询（输入值集合相同，但物理顺序不同）
SELECT
    quantile(
        CASE WHEN vp_rowid % 4 = 0 THEN NULL ELSE c0 END,
        0.5
    ) AS median,
    histogram(
        CASE WHEN vp_rowid % 4 = 0 THEN NULL ELSE c0 END
    ) AS hist
FROM (
    WITH l AS (
        SELECT
            ROW_NUMBER() OVER (ORDER BY vp_rowid) AS rn,
            vp_rowid, c1, c0
        FROM repro_duckdb_db5_hist_min_l
    ),
    r AS (
        SELECT
            ROW_NUMBER() OVER (ORDER BY vp_rowid) AS rn,
            vp_rowid, c0
        FROM repro_duckdb_db5_hist_min_r
    )
    SELECT l.vp_rowid, r.c0, l.c1
    FROM l
    JOIN r ON l.rn = r.rn
) reconstructed;