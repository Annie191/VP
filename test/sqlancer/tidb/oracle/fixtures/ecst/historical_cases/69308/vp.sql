-- split/join：返回 64
SELECT MAX(
  BIT_LENGTH(
    CASE v.c0
      WHEN ((BINARY v.id) | (v.id NOT LIKE v.c0)) THEN NULL
      ELSE CAST(0.7772428249950747 AS TIME)
    END
  )
) AS split_val
FROM (
  SELECT l.id, l.c0
  FROM l JOIN r ON l.id = r.id
) v
WHERE 1
ORDER BY v.id;