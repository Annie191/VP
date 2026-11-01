SELECT
  HEX(MIN(BINARY(
    CASE 'ꉈP/<3|9,-'
      WHEN c0 THEN '2020-11-24'
      WHEN c0 THEN id
      ELSE ']'
    END
  ))) AS source_hex,
  MIN(BINARY(
    CASE 'ꉈP/<3|9,-'
      WHEN c0 THEN '2020-11-24'
      WHEN c0 THEN id
      ELSE ']'
    END
  )) AS source_val
FROM src
ORDER BY id;