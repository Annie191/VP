# OceanBase native plan fixtures

Captured from OceanBase CE 4.4.2.1, MySQL mode, on 2026-11-06 using `EXPLAIN EXTENDED_NOADDR`. Trailing whitespace is normalized.

The data uses `src(vp_rowid BIGINT PRIMARY KEY,c0 BIGINT,c1 BIGINT,c2 VARCHAR(50))`, with `(1,1,2,'001'),(2,NULL,3,'x'),(3,-4,2,'1'),(4,1,2,'001')`. `pl` contains row identity/c0/c2 and `pr` contains row identity/c1, copied from src.

- `scan-filter.plan`: `SELECT vp_rowid FROM (SELECT * FROM src) ecst_outer WHERE c0+c1>0`
- `cross-side-join.plan`: `SELECT ecst_l.vp_rowid FROM pl ecst_l JOIN pr ecst_r ON ecst_l.vp_rowid=ecst_r.vp_rowid WHERE ecst_l.c0+ecst_r.c1>0`
- `retained-view.plan`: `SELECT vp_rowid FROM (SELECT * FROM src LIMIT 18446744073709551615) ecst_outer WHERE c0+c1>0`
- `projection.plan`: `SELECT vp_rowid, CAST(c0 AS DECIMAL(65,30)) AS ecst_value FROM src`
- `index-range.plan`: after `CREATE INDEX ecst_probe_idx ON src(c0)`, `SELECT /*+ INDEX(src ecst_probe_idx) */ vp_rowid FROM src WHERE c0>0`

The index fixture explicitly uses a hint to capture a deterministic index path. The normal ECST transformation generator creates indexes and observes the optimizer without adding this hint. Outline sections remain in the fixtures to check that the parser does not count suggested expressions or hints as operation evidence.
