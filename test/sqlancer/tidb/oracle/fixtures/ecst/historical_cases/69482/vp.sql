-- View query (correct: returns empty string)
SELECT 'split' AS q, CONCAT('[', MIN(c0), ']') AS shown, HEX(MIN(c0)) AS hex, LENGTH(MIN(c0)) AS len
FROM v;