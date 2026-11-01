-- Single‑table query (wrong: returns space ' ')
SELECT 'single' AS q, CONCAT('[', MIN(c0), ']') AS shown, HEX(MIN(c0)) AS hex, LENGTH(MIN(c0)) AS len
FROM src;