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

SELECT a, s, a
FROM src
WHERE ((s IS NULL) <> (a IS NULL))
  AND ('' LIKE s)
  AND ('' LIKE s);

SELECT x.a, x.s, x.a
FROM (
  SELECT l.id, r.a, l.v, l.s
  FROM l
  JOIN (
    SELECT id, JSON_OBJECT('vp', id) AS payload
    FROM r
  ) jp
    ON l.id = CAST(jp.payload:vp AS BIGINT)
  JOIN r
    ON r.id = CAST(jp.payload:vp AS BIGINT)
) x
WHERE ((x.s IS NULL) <> (x.a IS NULL))
  AND ('' LIKE x.s)
  AND ('' LIKE x.s);