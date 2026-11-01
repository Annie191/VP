-- Single‑table query: incorrectly returns 3 rows
SELECT id, c1
FROM src
WHERE c1 != (
  CASE (id LIKE c0)
    WHEN '0.5556779972246342' THEN -361629223
    ELSE INET_NTOA(0.1529189515040421)
  END
);