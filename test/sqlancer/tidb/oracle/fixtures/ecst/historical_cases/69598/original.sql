-- Single: correlated MIN decorrelated to Cartesian join + HashAgg
SELECT rowid, HEX(c0) AS c0_h,
       HEX((SELECT MIN(c0) FROM s s2 WHERE s2.rowid <= s.rowid)) AS rmin_h
FROM s ORDER BY rowid;