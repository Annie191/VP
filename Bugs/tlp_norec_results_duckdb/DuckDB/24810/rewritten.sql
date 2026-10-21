DROP TABLE IF EXISTS repro_duckdb_db1_interval_r;
DROP TABLE IF EXISTS repro_duckdb_db1_interval_l;
DROP TABLE IF EXISTS repro_duckdb_db1_interval_source;
CREATE TABLE repro_duckdb_db1_interval_source (
    vp_rowid BIGINT,
    c0 INTERVAL
);
INSERT INTO repro_duckdb_db1_interval_source VALUES
    (0, INTERVAL '580063 hours 44 minutes 54 seconds');
CREATE TABLE repro_duckdb_db1_interval_l (
    vp_rowid BIGINT
);
INSERT INTO repro_duckdb_db1_interval_l
SELECT vp_rowid
FROM repro_duckdb_db1_interval_source;
CREATE TABLE repro_duckdb_db1_interval_r (
    vp_rowid BIGINT,
    c0 INTERVAL
);
INSERT INTO repro_duckdb_db1_interval_r
SELECT vp_rowid, c0
FROM repro_duckdb_db1_interval_source;
COPY repro_duckdb_db1_interval_r
TO 'repro_duckdb_db1_interval_r.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- ORIGINAL SINGLE-TABLE QUERY
SELECT vp_rowid, c0
FROM repro_duckdb_db1_interval_source
ORDER BY vp_rowid;

-- SKIPPED TLP/NoREC: ORDER BY is not handled by this batch rewriter
