# ClickHouse ECST on one node

The ClickHouse adapter preserves the TiDB/MySQL/OceanBase ECST testing contract through controlled methods in `ClickHouseVerticalPartitioningOracle`: fix a source snapshot and logical operation, choose a directional target and legal configuration, transform the relation, validate semantic/data prerequisites, inspect native plans, compare results, update two-level history, and capture independent replay. It is functionally adapted to ClickHouse rather than translated SQL from another engine.

## Running

Use the matching complete SQLancer workspace and a single-node ClickHouse endpoint:

```sh
java -ea -jar target/sqlancer-2.0.0.jar \
  --host 127.0.0.1 --port 8123 --username default --password '' \
  --num-threads 1 --random-seed 37 --num-tries 1 \
  --max-generated-databases 1 --num-queries 1 --max-num-inserts 0 \
  --use-connection-test false --timeout-seconds 600 \
  --database-prefix ecst_ch \
  clickhouse --oracle ECST --ecst-query-pairs 3
```

The matching provider appends the Oracle name to its database prefix. ECST uses the existing connection and local MergeTree helpers. It rejects distributed VP profiles and does not create Distributed/Replicated tables, send ON CLUSTER DDL, stop merges or contact other gateways. Legacy VP remains a separate entry point.

The tested engine is ClickHouse 26.7.1.1315 with JDBC 0.8.6. The adapter requires the analyzer, materialized-CTE support and JSON physical plans with actions and index information. An unavailable setting or SQL feature raises an error; it is not silently replaced by a weaker migration claim.

## Controlled inputs and transformations

The normal seed generator selects numeric dependencies (including nullable integer/float inputs) from local row-preserving Memory/MergeTree/Log-family base tables. Computed/alias source definitions are excluded; unsupported default definitions are rejected. Other payload columns are copied with their native types. Row identities are assigned once while copying the source snapshot, before selecting any target. The existing VP column-definition, population, typed canonicalization and floating-comparison primitives are reused.

Transformations support same-side/cross-side vertical partitions, full-source view representations, JOIN/derived/CTE/view reconstruction, and legal WHERE/SELECT/HAVING, inner WHERE, inner SELECT and JOIN ON placement. A two-column seed has 63 legal configurations across its three original contexts. The deterministic seam also supports scalar String tests.

Same layout/index requirements reuse physical partitions. Rebuilds preserve the source, indexes, original SQL and operation. Invalid or unknown metadata/data prerequisites invalidate reuse. Metadata checks include exact column sets/native types and view output types. Data checks include row counts, rowid uniqueness/completeness and native Values-encoded copied values. MergeTree's primary key does not enforce uniqueness; the adapter proves it from the data before accepting the reconstruction.

Source and helper tables use controlled MergeTree sorting keys with one-row index granularity. This makes sparse-key pruning observable on bounded snapshots without changing the source after target selection. A primary key on a dependency is only a candidate: actual granule reduction in the plan is required for an access-path observation.

## Native evidence

`EXPLAIN PLAN json=1,actions=1,indexes=1` supplies a physical tree and expression DAGs. The adapter matches expression structure against native filter columns and FUNCTION actions, follows physical inputs to relevant tables, and preserves string-literal contents while removing native numeric type annotations.

| Directional dimension | Native states and evidence |
| --- | --- |
| LOCATION | PRE_JOIN / POST_JOIN, from a matched filter/function and physical Join inputs |
| BOUNDARY | MERGED / MATERIALIZED, from relevant physical reads beneath MaterializingCTE |
| ACCESS_PATH | TABLE_SCAN / PRIMARY_PRUNED, requiring PrimaryKey Selected Granules < Initial Granules and a nontrivial condition |

On the tested engine, a cross-table non-equi JOIN ON predicate is executed as a Filter after Join. The adapter records FILTER and POST_JOIN and does not invent a JOIN_CONDITION migration target. Sparse MergeTree pruning is not labeled as a MySQL B-tree index range.

A materialized CTE is referenced twice using a rowid self-join. Checked uniqueness preserves multiplicity and prevents the optimizer's single-reference CTE inlining. A MATERIALIZED keyword, ordinary view or CTE name alone is not boundary evidence. Anonymous ReadFromMemoryStorage consumers lack source identity in this JSON format; their operation position remains UNKNOWN even when the corresponding producer proves materialization. Ambiguous, unsupported or unmapped expressions remain UNKNOWN. Evidence is PLAN_ONLY, with no causal/minimization claim.

## Comparison, settings and replay

Every accepted pair is compared, including target misses and unknown observations. Two-level history learns target/configuration outcomes, coverage and cost; rejected setup/prerequisites do not become migration trials. Mismatches must repeat before reporting. Reproduced mismatches and unexpected failures retain helpers; prepared unexpected failures also attempt a replay bundle. Legacy known-pattern suppression is bypassed by the strict ECST result path.

Comparison checks original output metadata even when both results are empty, preserves duplicate multiplicity, and uses the existing VP float tolerances. JDBC v2 cannot reliably expose String bytes through `getBytes`; ECST first probes metadata and then transports scalar String/FixedString output as native `hex(...)`, preventing UTF-8 replacement from hiding binary differences. Rich payload equality is validated using native Values serialization.

Identical query-scoped settings enable the analyzer/materialized CTEs, select ALL/hash joins and NULL semantics, disable automatic PREWHERE movement and query-condition/index-read caches, and bound parallelism. They are applied to SELECT/EXPLAIN reads rather than relying on persistent sessions across HTTP requests. No server/user setting is changed. The complete profile is in `ClickHouseEcstSession.SETTINGS`.

Replay contains the final layout's unqualified DDL, source rows serialized by ClickHouse `formatRowNoNewline('Values',...)`, partition population, final view DDL and the profile. It preserves native bytes, FixedString padding, arrays, maps, tuples, decimals and DateTime64 precision without the original base table or obsolete retries. Optimizer decisions can differ on replay.

JSONL appears under `logs/clickhouse/ecst-<database>.jsonl`. Records contain original/transformed plans, target/configuration, results, `executionMode: SINGLE_NODE`, query settings, source replay and `replayQueries` with the settings attached. Use those complete queries for independent HTTP requests; interactive SQL clients can also execute the replay's SET statement before the queries.

## Validation

```sh
mvn -o -Djacoco.skip=true \
  -Dtest=ClickHouseExecutionContextAnalyzerTest,ClickHouseEcstIntegrationTest,EcstHistoryTest \
  '-Dclickhouse.ecst.test.url=jdbc:clickhouse://127.0.0.1:8123/default?compress=false' test
```

Set `clickhouse.ecst.test.user/password` when needed. Only an absent endpoint skips live tests; configured connection/SQL errors fail acceptance. Native fixtures are captured from an isolated single-node server. Tests cover all 63 configurations, both materialization directions, NULLs/duplicates/grouping, corruption detection and rebuild, duplicate rowids, empty metadata differences, binary String distinctions, independent rich-type replay, error propagation and full Oracle cleanup on one connection.

This repository is a source export; it needs the matching complete SQLancer framework and build configuration.

### Verified on 2026-11-08

- The exact published VP variant was compiled and packaged in an isolated copy of the matching framework, excluding unrelated pre-existing VP workspace changes. All 41 targeted tests passed: eight native-plan/options tests, nine live integration tests and 24 history tests, with no failures, errors or skips. Live acceptance covered all 63 configurations, real view creation/use and both materialization directions.
- The same packaged artifact completed a three-pair CLI budget on an isolated ClickHouse 26.7.1.1315 single node. All three pairs compared equal: 1/1, 1/1 and 0/0 result rows. Two target observations were UNKNOWN and one missed; comparison still ran for every pair.
- All 437 server-setting values matched the pre-run snapshot. No ECST helpers remained, own test databases were dropped, and the isolated container was stopped and removed. Scoped Checkstyle checks passed. Test artifacts, server data and binaries are excluded from this source export.

Native references: [EXPLAIN JSON/actions/indexes](https://clickhouse.com/docs/reference/statements/explain), [materialized CTE behavior and settings](https://clickhouse.com/docs/reference/statements/select/with).
