SELECT
  HEX(MIN(BINARY(
    CASE 'ꉈP/<3|9,-'
      WHEN c0 THEN '2020-11-24'
      WHEN c0 THEN id
      ELSE ']'
    END
  ))) AS split_hex,
  MIN(BINARY(
    CASE 'ꉈP/<3|9,-'
      WHEN c0 THEN '2020-11-24'
      WHEN c0 THEN id
      ELSE ']'
    END
  )) AS split_val
FROM v
ORDER BY id;