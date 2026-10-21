DROP DATABASE IF EXISTS repro_postgres810_db3_min;
CREATE DATABASE repro_postgres810_db3_min;
\c repro_postgres810_db3_min;
-- ============ 会话参数 ============
SET enable_partitionwise_join = on;
SET enable_partition_pruning = on;
SET enable_partitionwise_aggregate = on;
SET enable_parallel_append = on;
SET enable_parallel_hash = on;
SET max_parallel_workers_per_gather = 2;
SET min_parallel_table_scan_size = 0;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
-- ============ 单表 source ============
CREATE TABLE m_src(rowid bigint PRIMARY KEY, tsvec tsvector, tsq tsquery);
INSERT INTO m_src VALUES
 (1, to_tsvector('english','quick brown fox'), to_tsquery('english','alpha & beta')),
 (2, to_tsvector('english','quick brown fox'), to_tsquery('english','alpha & beta')),
 (3, to_tsvector('english','quick brown fox'), to_tsquery('english','alpha & beta'));
-- ============ 左/右分区表（保留原始 l/r 垂直切分结构）============
CREATE TABLE m_l(rowid bigint PRIMARY KEY, tsvec tsvector) PARTITION BY RANGE (rowid);
CREATE TABLE m_l_p1 PARTITION OF m_l FOR VALUES FROM (1) TO (10);
INSERT INTO m_l SELECT rowid, tsvec FROM m_src;
CREATE TABLE m_r(rowid bigint PRIMARY KEY, tsq tsquery) PARTITION BY RANGE (rowid);
CREATE TABLE m_r_p1 PARTITION OF m_r FOR VALUES FROM (1) TO (10);
INSERT INTO m_r SELECT rowid, tsq FROM m_src;
-- 关键：只 ANALYZE 左表，右表保持无统计
ANALYZE m_l;

-- ORIGINAL SINGLE-TABLE QUERY
-- ============ ① 单表查询：正常执行 ============
SELECT ARRAY['', '']::TEXT[] FROM m_src
WHERE NOT (m_src.tsvec @@ m_src.tsq)
GROUP BY ROLLUP (ARRAY['', '']::TEXT[]);

-- SKIPPED TLP/NoREC: GROUP BY is not handled by this batch rewriter
