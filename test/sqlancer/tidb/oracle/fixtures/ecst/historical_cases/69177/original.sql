SELECT HEX(MIN(CAST(c0 AS BINARY))) AS source_hex,
       MIN(CAST(c0 AS BINARY)) AS source_val
FROM src;