SELECT rowid, HEX(c0) AS c0_h,
       HEX((SELECT MIN(c0) FROM (
         WITH lc AS (SELECT * FROM l), rc AS (SELECT * FROM r)
         SELECT lc.rowid, lc.c0, lc.c1 FROM lc JOIN rc ON lc.rowid = rc.rowid
       ) s2 WHERE s2.rowid <= s.rowid)) AS rmin_h
FROM (WITH lc AS (SELECT * FROM l), rc AS (SELECT * FROM r)
      SELECT lc.rowid, lc.c0, lc.c1 FROM lc JOIN rc ON lc.rowid = rc.rowid) s
ORDER BY rowid;