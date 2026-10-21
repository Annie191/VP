DROP DATABASE IF EXISTS repro_db11;
CREATE DATABASE repro_db11 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
USE repro_db11;

SET SESSION sql_mode='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION';

CREATE TABLE s (rowid INT PRIMARY KEY AUTO_INCREMENT, c0 TEXT, c1 TEXT);
INSERT INTO s (c0, c1) VALUES
  (NULL,'a'),(NULL,'b'),(NULL,'c'),(NULL,'d'),
  ('  ',NULL),('',''),('  ','y'),('  ','y'),('  ','y'),(' ','z'),
  ('101','w'),('}{',''),('abc','v'),('F}','u'),('HI',''),('ZV','t');

-- Split (CTE rewrite, using VP split to avoid decorrelation)
CREATE TABLE l (rowid INT PRIMARY KEY, c1 TEXT, c0 TEXT);
CREATE TABLE r (rowid INT PRIMARY KEY, c0 TEXT);
INSERT INTO l SELECT rowid, c1, c0 FROM s;
INSERT INTO r SELECT rowid, c0 FROM s;


-- Single: correlated MIN decorrelated to Cartesian join + HashAgg
SELECT rowid, HEX(c0) AS c0_h,
       HEX((SELECT MIN(c0) FROM s s2 WHERE s2.rowid <= s.rowid)) AS rmin_h
FROM s ORDER BY rowid;



SELECT rowid, HEX(c0) AS c0_h,
       HEX((SELECT MIN(c0) FROM (
         WITH lc AS (SELECT * FROM l), rc AS (SELECT * FROM r)
         SELECT lc.rowid, lc.c0, lc.c1 FROM lc JOIN rc ON lc.rowid = rc.rowid
       ) s2 WHERE s2.rowid <= s.rowid)) AS rmin_h
FROM (WITH lc AS (SELECT * FROM l), rc AS (SELECT * FROM r)
      SELECT lc.rowid, lc.c0, lc.c1 FROM lc JOIN rc ON lc.rowid = rc.rowid) s
ORDER BY rowid;