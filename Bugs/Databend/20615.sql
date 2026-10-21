DROP DATABASE IF EXISTS repro_databend_db34_tiny;
CREATE DATABASE repro_databend_db34_tiny;
USE repro_databend_db34_tiny;

CREATE TABLE src (
    id INT NOT NULL
) ENGINE = MEMORY;

CREATE TABLE l (
    id INT NOT NULL
) ENGINE = MEMORY;

CREATE TABLE r (
    id INT NOT NULL
) ENGINE = MEMORY;

-- src 分两次写入，形成两个 block
INSERT INTO src VALUES (1);
INSERT INTO src VALUES (2);

-- 分区表各自一次写入
INSERT INTO l VALUES (1), (2);
INSERT INTO r VALUES (1), (2);

SET max_threads = 1;
SET force_aggregate_data_spill = 0;
SET force_sort_data_spill = 0;

SELECT
    NULL AS k,
    CAST(7 AS INT) AS tag
FROM src
GROUP BY
    NULL,
    CAST(7 AS INT);

SELECT
    NULL AS k,
    CAST(7 AS INT) AS tag
FROM (
    SELECT l.id
    FROM l
    JOIN (
        SELECT
            id,
            JSON_OBJECT('vp', id) AS payload
        FROM r
    ) j
        ON l.id = j.id
    JOIN r
        ON r.id = j.id
) reconstructed
GROUP BY
    NULL,
    CAST(7 AS INT);