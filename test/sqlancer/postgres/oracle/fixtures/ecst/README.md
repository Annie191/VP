# PostgreSQL native JSON EXPLAIN fixtures

`scan-12.json`, `join-12.json`, `materialized-12.json` were captured from PostgreSQL 12.22. The matching `*-18.json` files were captured from PostgreSQL 18.6. No plan fields were edited. Each capture used an isolated schema, four source rows and ANALYZE; the schema was removed afterwards.

```sql
CREATE TABLE source(c0 bigint,c1 bigint,vp_rowid bigint PRIMARY KEY);
INSERT INTO source VALUES (1,2,1),(3,1,2),(NULL,2,3),(-4,1,4);
CREATE TABLE lhs(c0 bigint,vp_rowid bigint PRIMARY KEY);
CREATE TABLE rhs(c1 bigint,vp_rowid bigint PRIMARY KEY);
INSERT INTO lhs SELECT c0,vp_rowid FROM source;
INSERT INTO rhs SELECT c1,vp_rowid FROM source;
ANALYZE source;
ANALYZE lhs;
ANALYZE rhs;
EXPLAIN (VERBOSE,FORMAT JSON) SELECT vp_rowid FROM source WHERE c0>c1;
EXPLAIN (VERBOSE,FORMAT JSON) SELECT ecst_l.vp_rowid FROM lhs ecst_l
  JOIN rhs ecst_r ON ecst_l.vp_rowid=ecst_r.vp_rowid AND ecst_l.c0>ecst_r.c1;
EXPLAIN (VERBOSE,FORMAT JSON) WITH ecst_cte AS MATERIALIZED
  (SELECT * FROM source) SELECT vp_rowid FROM ecst_cte WHERE c0>c1;
```

The tests explicitly synthesize changed scan node types, duplicate predicates, missing input relations, invalid JSON, and an unrelated materialized scalar subplan. These variants are parser regressions, not claims of captured access-path migrations. Live tests separately verify actual index-scan transitions in both directions without changing optimizer switches.
