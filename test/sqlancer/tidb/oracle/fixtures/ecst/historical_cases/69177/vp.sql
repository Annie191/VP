SELECT HEX(MIN(CAST(l.c0 AS BINARY))) AS join_hex,
       MIN(CAST(l.c0 AS BINARY)) AS join_val
FROM l JOIN r ON l.id = r.id;