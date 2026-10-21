DROP DATABASE IF EXISTS repro_databend917_13;
CREATE DATABASE repro_databend917_13;
USE repro_databend917_13;
CREATE TABLE src (
  id BIGINT,
  a ARRAY(BIGINT NULL),
  v VARIANT,
  s VARCHAR
) ENGINE=FUSE;
INSERT INTO src VALUES
  (1, [1,2], '{"x":1}', NULL),
  (2, NULL,  '{"x":2}', 'abc'),
  (3, [],    '{"x":3}', '');
CREATE TABLE l (
  id BIGINT,
  s VARCHAR,
  v VARIANT
) ENGINE=MEMORY;
CREATE TABLE r (
  id BIGINT,
  a ARRAY(BIGINT NULL)
) ENGINE=FUSE;
INSERT INTO l SELECT id, s, v FROM src;
INSERT INTO r SELECT id, a FROM src;

-- ORIGINAL SINGLE-TABLE QUERY
SELECT a, s, a
FROM src
WHERE ((s IS NULL) <> (a IS NULL))
  AND ('' LIKE s)
  AND ('' LIKE s);

-- TLP BASE (original query with WHERE removed)
SELECT a, s, a FROM src;

-- TLP UNION ALL
SELECT a, s, a FROM src WHERE ((s IS NULL) <> (a IS NULL)) AND ('' LIKE s) AND ('' LIKE s) UNION ALL SELECT a, s, a FROM src WHERE NOT ((s IS NULL) <> (a IS NULL)) AND ('' LIKE s) AND ('' LIKE s) UNION ALL SELECT a, s, a FROM src WHERE ((s IS NULL) <> (a IS NULL)) AND ('' LIKE s) AND ('' LIKE s) IS NULL;

-- NoREC optimized
SELECT COUNT(*) FROM src WHERE ((s IS NULL) <> (a IS NULL)) AND ('' LIKE s) AND ('' LIKE s);

-- NoREC unoptimized
SELECT SUM(CASE WHEN ((s IS NULL) <> (a IS NULL)) AND ('' LIKE s) AND ('' LIKE s) THEN 1 ELSE NULL END) FROM src;
