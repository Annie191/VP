DROP DATABASE IF EXISTS repro_databend917_db5_min;
CREATE DATABASE repro_databend917_db5_min;
USE repro_databend917_db5_min;

SET default_order_by_null = 'nulls_last';
SET disable_join_reorder = 0;
SET enable_bloom_runtime_filter = 1;
SET enable_cbo = 1;
SET enable_join_runtime_filter = 1;
SET enable_query_result_cache = 0;
SET max_threads = 40;
SET numeric_cast_option = 'rounding';
SET sql_dialect = 'PostgreSQL';
SET timezone = 'UTC';
SET force_aggregate_data_spill = 1;
SET force_sort_data_spill = 1;

CREATE TABLE t0 (
    c0date DATE NULL DEFAULT NULL,
    c1decimal DECIMAL(43, 8) NULL DEFAULT 1.00000000,
    c2float FLOAT NULL DEFAULT 0.6591864228248596
);

INSERT INTO t0(c1decimal, c0date) VALUES
(CAST(1 AS DECIMAL(43, 8)), DATE '2000-01-02'),
(NULL, NULL),
(CAST(0 AS DECIMAL(43, 8)), DATE '2000-01-01'),
(CAST(0 AS DECIMAL(43, 8)), DATE '2000-01-01'),
(CAST(0 AS DECIMAL(43, 8)), DATE '2000-01-01'),
(NULL, NULL);

INSERT INTO t0(c2float, c1decimal) VALUES
(0, CAST(0 AS DECIMAL(43, 8))),
(0, NULL);

INSERT INTO t0(c1decimal, c2float) VALUES
(CAST(-1 AS DECIMAL(43, 8)), -1),
(NULL, 0);

INSERT INTO t0(c2float, c1decimal, c0date) VALUES
(0.8981454372406006, CAST(1 AS DECIMAL(43, 8)), DATE '1970-01-10');

INSERT INTO t0(c2float) VALUES (0), (0);

INSERT INTO t0(c2float, c0date) VALUES
(2, DATE '2000-01-03'),
(1, NULL),
(-2, DATE '2000-01-03'),
(NULL, DATE '2000-01-04');

INSERT INTO t0(c2float, c1decimal, c0date) VALUES
(7.821312E8, CAST(1 AS DECIMAL(43, 8)), DATE '1970-01-13'),
(0.8146395087242126, CAST(1 AS DECIMAL(43, 8)), DATE '1970-01-13');

INSERT INTO t0(c0date, c1decimal, c2float) VALUES
(DATE '1970-01-17', CAST(1 AS DECIMAL(43, 8)), 7.821312E8);

INSERT INTO t0(c2float, c0date) VALUES
(0.8556254506111145, DATE '1969-12-15');

INSERT INTO t0(c0date, c1decimal) VALUES
(DATE '2000-01-01', CAST(0 AS DECIMAL(43, 8))),
(NULL, NULL);

INSERT INTO t0(c1decimal, c2float, c0date) VALUES
(CAST(1 AS DECIMAL(43, 8)), 0.469486802816391, DATE '1970-01-10');

INSERT INTO t0(c2float) VALUES (0.23118504881858826);
INSERT INTO t0(c2float) VALUES (0.6223193407058716);
INSERT INTO t0(c2float) VALUES (-3), (0);

INSERT INTO t0(c0date, c2float, c1decimal) VALUES
(DATE '1970-01-11', 0.15438950061798096, CAST(1 AS DECIMAL(43, 8)));

INSERT INTO t0(c1decimal) VALUES
(CAST(0 AS DECIMAL(43, 8))),
(NULL),
(CAST(0 AS DECIMAL(43, 8))),
(CAST(-2 AS DECIMAL(43, 8)));

INSERT INTO t0(c2float, c1decimal, c0date) VALUES
(0, CAST(0 AS DECIMAL(43, 8)), DATE '2000-01-01'),
(3, NULL, NULL);

INSERT INTO t0(c2float) VALUES (0), (3);

INSERT INTO t0(c2float, c0date) VALUES
(-3, DATE '2000-01-04'),
(0, NULL),
(0, DATE '2000-01-01'),
(NULL, DATE '2000-01-01');

INSERT INTO t0(c1decimal, c2float) VALUES
(CAST(1 AS DECIMAL(43, 8)), 1.419568896E9);

CREATE TABLE source (
    id BIGINT NOT NULL,
    d DATE,
    a DECIMAL(43, 8),
    b DOUBLE
) ENGINE = MEMORY;

INSERT INTO source
SELECT ROW_NUMBER() OVER (ORDER BY c0date, c1decimal, c2float),
       c0date, c1decimal, c2float
FROM t0
LIMIT 37;

CREATE TABLE left_part (
    id BIGINT NOT NULL,
    a DECIMAL(43, 8),
    b DOUBLE
) ENGINE = FUSE;

CREATE TABLE right_part (
    id BIGINT NOT NULL,
    d DATE
) ENGINE = MEMORY;

INSERT INTO left_part SELECT id, a, b FROM source;
INSERT INTO right_part SELECT id, d FROM source;

CREATE TEMPORARY TABLE joined (
    id BIGINT NOT NULL,
    d DATE,
    a DECIMAL(43, 8),
    b DOUBLE
);

TRUNCATE TABLE joined;
INSERT INTO joined
SELECT l.id, r.d, l.a, l.b
FROM left_part l JOIN right_part r ON l.id = r.id;

TRUNCATE TABLE joined;
INSERT INTO joined
SELECT l.id, r.d, l.a, l.b
FROM left_part l JOIN right_part r ON l.id = r.id;

TRUNCATE TABLE joined;
INSERT INTO joined
SELECT l.id, r.d, l.a, l.b
FROM left_part l JOIN right_part r ON l.id = r.id;

TRUNCATE TABLE joined;
INSERT INTO joined
SELECT l.id, r.d, l.a, l.b
FROM left_part l JOIN right_part r ON l.id = r.id;

TRUNCATE TABLE joined;
INSERT INTO joined
SELECT l.id, r.d, l.a, l.b
FROM left_part l JOIN right_part r ON l.id = r.id;

-- 单表查询
SELECT d FROM source ORDER BY id LIMIT 8;

-- TEMP_TABLE 重构后的查询
SELECT d FROM joined ORDER BY id LIMIT 8;
