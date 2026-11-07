# CockroachDB ECST single-node native fixtures

Captured on CockroachDB CCL v26.2.3 from one isolated `start-single-node` instance, with `distsql=off`. Trailing whitespace is normalized. No range splits, SCATTER commands or gateway rotation were used.

`src(vp_rowid INT8 PRIMARY KEY,c0 INT8,c1 INT8,c2 STRING)` contains 256 generated rows, NULLs and repeated values. `pl` contains row identity/c0/c2 and `pr` row identity/c1. The source has `ecst_idx(c0) STORING(c1,c2)`.

- `filter.plan`: `SELECT vp_rowid FROM (SELECT * FROM src) ecst_outer WHERE c0+c1>0`.
- `join.plan`: `SELECT ecst_l.vp_rowid FROM pl ecst_l JOIN pr ecst_r ON ecst_l.vp_rowid=ecst_r.vp_rowid WHERE ecst_l.c0+ecst_r.c1>0`.
- `cte.plan`: `WITH ecst_cte AS MATERIALIZED (SELECT * FROM src) SELECT vp_rowid FROM ecst_cte WHERE c0+c1>0`.
- `index.plan`: `SELECT vp_rowid FROM src@ecst_idx WHERE c0>0`.
- `projection.plan`: `SELECT vp_rowid,c0+c1 AS ecst_value FROM src`.

All fixtures use `EXPLAIN (VERBOSE)`. The explicit index reference in `index.plan` makes the parser fixture deterministic; production ECST creates indexes and observes the optimizer without forcing an index.
