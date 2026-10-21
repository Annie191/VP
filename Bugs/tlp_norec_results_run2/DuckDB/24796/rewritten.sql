CREATE SCHEMA repro_duckdb_db9_min;
SET schema 'repro_duckdb_db9_min';
CREATE TABLE repro_duckdb_db9_min_source(
    vp_rowid BIGINT,
    c0 TIMESTAMP
);
INSERT INTO repro_duckdb_db9_min_source VALUES
    (0, TIMESTAMP '1970-01-01'),
    (1, TIMESTAMP '1969-12-23 16:25:55');

-- ORIGINAL SINGLE-TABLE QUERY
-- 单表版本（正常工作）
SELECT
    COUNT(*) FILTER (WHERE vp_rowid % 2 = 0) AS even_count,
    COUNT(c0) FILTER (WHERE vp_rowid % 2 = 0) AS even_nonnull,
    (
        SELECT COUNT(*)
        FROM (
            SELECT *
            FROM repro_duckdb_db9_min_source
            WHERE FALSE
        ) empty_input
    ) AS empty_count
FROM repro_duckdb_db9_min_source;

-- SKIPPED TLP/NoREC: nested query/window/union is not handled by this batch rewriter
