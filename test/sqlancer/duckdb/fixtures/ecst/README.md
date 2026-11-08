# DuckDB ECST native plans

Captured with DuckDB 1.5.6 using EXPLAIN (ANALYZE,FORMAT JSON), threads=1, and 4096 rows with NULLs and repeated values. Runtime timing/counter fields are removed; native operators, extra_info and children are preserved. src has a single-column ART index on c0.

- `filter.json`: `SELECT vp_rowid FROM src WHERE c0+c1>0`.
- `join.json`: `SELECT l.vp_rowid FROM pl l JOIN pr r ON l.vp_rowid=r.vp_rowid WHERE l.c0+r.c1>0`.
- `join-condition.json`: `SELECT l.vp_rowid FROM pl l JOIN pr r ON l.vp_rowid=r.vp_rowid AND l.c0>r.c1`.
- `cte.json`: `WITH ecst_cte AS MATERIALIZED (SELECT * FROM src) SELECT vp_rowid FROM ecst_cte WHERE c0+c1>0`.
- `projection.json`: `SELECT vp_rowid,c0+c1 AS ecst_value FROM src`.
- `index.json`: `SELECT vp_rowid FROM src WHERE c0=17`.

The executed index fixture reports Type: Index Scan even when operator_name is SEQ_SCAN. CTE consumers identify their producer by CTE Index/Table Index. Projection aliases and #N references alone are not expression evidence.
