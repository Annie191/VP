DROP DATABASE IF EXISTS repro_mysql619_11;
CREATE DATABASE repro_mysql619_11;
USE repro_mysql619_11;
CREATE TABLE source (
  vp_rowid BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY
) ENGINE=InnoDB;
INSERT INTO source VALUES ();
CREATE TABLE l (
  vp_rowid BIGINT NOT NULL PRIMARY KEY
) ENGINE=InnoDB;
CREATE TABLE r (
  vp_rowid BIGINT NOT NULL PRIMARY KEY
) ENGINE=InnoDB;
INSERT INTO l SELECT vp_rowid FROM source;
INSERT INTO r SELECT vp_rowid FROM source;

-- ORIGINAL SINGLE-TABLE QUERY
-- Single query: returns 1
SELECT MAX(DISTINCT source.vp_rowid) AS ref0
FROM source
WHERE LEAST('Uk]6T', 0.8087354366787728) IN (source.vp_rowid);

-- TLP BASE (original query with WHERE removed)
/* Single query: returns 1 */ SELECT MAX(DISTINCT source.vp_rowid) AS ref0 FROM source;

-- TLP UNION ALL
SELECT MAX(agg0) FROM (/* Single query: returns 1 */ SELECT MAX(DISTINCT source.vp_rowid) AS agg0 FROM source WHERE LEAST('Uk]6T', 0.8087354366787728) IN (source.vp_rowid) UNION ALL /* Single query: returns 1 */ SELECT MAX(DISTINCT source.vp_rowid) AS agg0 FROM source WHERE NOT LEAST('Uk]6T', 0.8087354366787728) IN (source.vp_rowid) UNION ALL /* Single query: returns 1 */ SELECT MAX(DISTINCT source.vp_rowid) AS agg0 FROM source WHERE LEAST('Uk]6T', 0.8087354366787728) IN (source.vp_rowid) IS NULL) AS tlp_parts;

-- NoREC optimized
/* Single query: returns 1 */ SELECT COUNT(*) FROM source WHERE LEAST('Uk]6T', 0.8087354366787728) IN (source.vp_rowid);

-- NoREC unoptimized
/* Single query: returns 1 */ SELECT SUM(CASE WHEN LEAST('Uk]6T', 0.8087354366787728) IN (source.vp_rowid) THEN 1 ELSE NULL END) FROM source;
