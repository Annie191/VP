-- Split/join query (via correlated subquery): correctly returns empty set
SELECT v.id, v.c1
FROM (
  SELECT l.id, l.c0,
         (SELECT r.c1 FROM r WHERE r.id = l.id) AS c1
  FROM l
  WHERE l.id IN (SELECT r.id FROM r)
) v
WHERE v.c1 != (
  CASE (v.id LIKE v.c0)
    WHEN '0.5556779972246342' THEN -361629223
    ELSE INET_NTOA(0.1529189515040421)
  END
);