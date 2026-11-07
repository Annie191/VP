# Native ClickHouse ECST fixtures

Captured from an isolated ClickHouse 26.7.1.1315 single-node server using EXPLAIN PLAN json=1,actions=1,indexes=1 and the fixed query settings in ClickHouseEcstSession. Tables contain 64 rows with repeated numeric values; the live suite additionally tests NULLs and duplicates. JSON whitespace is normalized.

- `filter.json`: `SELECT vp_rowid FROM ecst_probe_ch_20261108.src WHERE greater(plus(c0,c1),0)`.
- `join.json`: `SELECT l.vp_rowid FROM ecst_probe_ch_20261108.pl l ALL INNER JOIN ecst_probe_ch_20261108.pr r ON l.vp_rowid=r.vp_rowid AND greater(plus(l.c0,r.c1),0)`.
- `projection.json`: `SELECT vp_rowid,plus(c0,c1) AS ecst_value FROM ecst_probe_ch_20261108.src`.
- `cte.json`: `WITH z AS MATERIALIZED (SELECT * FROM ecst_probe_ch_20261108.src) SELECT a.vp_rowid FROM z a ALL INNER JOIN z b ON a.vp_rowid=b.vp_rowid WHERE greater(plus(a.c0,a.c1),0)`.
- `pruned.json`: `SELECT vp_rowid FROM ecst_probe_ch_20261108.keyed WHERE greater(c0,0)`.

MaterializingCTE establishes physical materialization. Anonymous ReadFromMemoryStorage consumers do not provide source lineage. PrimaryKey granule counts distinguish actual pruning from a full scan.
