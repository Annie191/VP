# DuckDB ECST

DuckDB ECST preserves the testing contract established by the TiDB/MySQL/OceanBase adapters through controlled methods in `DuckDBVerticalPartitioningOracle`: an immutable source snapshot and operation, directional target/configuration selection, native plan observations, semantic/data gates, independent result comparison, two-level feedback and final-state replay. DuckDB's embedded execution, materialized CTEs and ART indexes are adapted to their own semantics.

## Run in the matching complete SQLancer workspace

```sh
java -ea -jar target/sqlancer-2.0.0.jar \
  --num-threads 1 --random-seed 19 --num-tries 1 \
  --max-generated-databases 1 --num-queries 1 --max-num-inserts 20 \
  --use-connection-test false --timeout-seconds 600 \
  --database-prefix ecst_duck \
  duckdb --oracle ECST --ecst-query-pairs 3
```

DuckDB runs embedded, in memory by default; it needs no host, server, username or password. The tested engine is DuckDB 1.5.6 with JDBC 1.5.6.0. `--ecst-query-pairs` counts comparable pairs, including empty results whose output types match. JSONL summaries report the requested/completed budgets and skips/rejections separately.

ECST uses one connection, one DuckDB execution thread and UTC before creating snapshots. It preserves the current optimizer switches, default collation and ART thresholds, and uses the same settings for both sides. The existing VP native-execution lock protects the ECST check and executed-plan reads. ECST does not apply VP's split-only optimizer modes, Parquet/storage-boundary interventions or one-byte checkpoint policy. The legacy VP entry point remains available.

## Controlled VP interfaces

Existing VP descriptors, column definitions, source/partition population, structured value canonicalization and floating-point tolerance are reused. Each seed copies the base values once, assigns BIGINT primary-key row identities, and fixes its source indexes, SQL and operation before choosing a target. Dependencies may be numeric or character columns. Seeds include scalar/multi-column arithmetic, string length, conditional expressions and direct comparisons whose placement can become a native join condition.

The legal matrix contains 63 configurations across WHERE/SELECT/HAVING seeds: same-side/cross-side vertical partitions, full-source views, JOIN/derived/CTE/view reconstruction and legal inner WHERE, inner SELECT and JOIN ON placement. Ordinary views are created and expanded; retained blocks use explicit MATERIALIZED CTEs.

Same layout/index requirements reuse partitions. Different requirements or rejected/unknown prerequisites rebuild them without altering the source. Semantic checks verify column sets/native types, rowid-only primary keys and frozen native table/index/view definitions, including collation and nullability changes. Data checks verify copied values, counts and complete unique row identities. Character values are compared as encoded bytes. Filtered views may discard rows; their predicate semantics belong to result comparison rather than a precondition that could discard a bug.

## Native execution evidence

`EXPLAIN (ANALYZE,FORMAT JSON)` executes the query and returns its physical tree. The adapter reads only native operator fields and resolves CTE_SCAN's CTE Index to the CTE producer's Table Index. Unrelated CTE branches do not establish a boundary for the selected operation.

| Dimension | Supported observations |
| --- | --- |
| LOCATION | PRE_JOIN / POST_JOIN, from a structurally matched filter/condition and physical inputs |
| OPERATOR | FILTER / JOIN_CONDITION; pushed scan predicates are separately recorded as SCAN_FILTER |
| BOUNDARY | MERGED / MATERIALIZED, from actual CTE producer/consumer structure |
| ACCESS_PATH | TABLE_SCAN / INDEX_SCAN, from the executed scan's Type on relevant relations |

A DuckDB operator named SEQ_SCAN can report `Type: Index Scan` at runtime. The adapter uses that executed Type; index existence or a static EXPLAIN estimate is insufficient. Only single-column numeric ART indexes are synthesized as access-path candidates. A sequential scan may still use zonemaps and is not claimed to read every row.

Expression matching normalizes quoted identifiers and equivalent reversed inequalities while preserving literal contents. Duplicate pushed predicates, unresolved `#N` references and projection display aliases do not prove unique operation placement and remain UNKNOWN. The comparison still runs. Recorded migration evidence remains plan-based; no causal intervention or minimization is claimed.

## Results and independent replay

Strict comparison retains duplicate multiplicity, checks column labels/native types even for empty output, preserves strings and BLOB/structured values, and reuses VP's numeric tolerances. It bypasses legacy known-pattern suppression. Unexpected SELECT or EXPLAIN errors propagate. A mismatch must repeat before reporting; reproduced mismatches and unexpected failures retain helpers for inspection. Prepared failures attempt a replay bundle with diagnostics.

Replay contains the captured profile, source DDL and typed values, only the final partition layout/indexes and view setup. Scalars are serialized using their native VARCHAR representation and an explicit original type; UTF-8 text is transported using `decode(from_hex(...))`, preserving quotes, backslashes and NUL characters. JDBC lists, maps and structs are recursively reconstructed with their native type trees. This preserves BLOB bytes, JSON, DECIMAL, nanosecond timestamps, nested nulls and Map entry order without the original base table or external Parquet files. Schema parsing retains JSON and timestamp-resolution/time-zone declarations.

Records appear in `logs/duckdb/ecst-<database>.jsonl`, including original/transformed SQL and plans, target/configuration, result counts, history, final-state replay and failure details. Optimizer decisions can differ during replay; reconstructing data and SQL does not guarantee an identical plan.

## Validation

```sh
mvn -o -Djacoco.skip=true \
  -Dtest=DuckDBExecutionContextAnalyzerTest,DuckDBEcstIntegrationTest,EcstHistoryTest,DuckDBValueCanonicalizerTest,DuckDBVerticalPartitioningOracleTest,DuckDBSchemaTest \
  test
```

On 2026-11-08 the exact source-export variant passed all 60 targeted tests (12 embedded integration, 8 native-plan, 24 history, and 16 legacy VP/schema/canonicalization tests), with no failures or skips. Scoped formatting and Checkstyle passed. The source-export variant excludes unrelated local Parquet work.

The packaged CLI, seed 19 with an insert limit of 20, completed all 3 requested pairs; all were nonempty and equal. All 3 targets succeeded: TABLE_SCAN→INDEX_SCAN, MATERIALIZED→MERGED and MERGED→MATERIALIZED. Captured optimizer/collation/ART settings were identical across pairs.

The integration tests always run against separate embedded databases. They cover all 63 configurations, both materialization directions, executed ART transitions in both directions, filter-to-join-condition placement, NULLs/duplicates/grouping, leading/trailing-character differences, empty output types, metadata/data corruption and recovery, synthetic-rowid exclusion, composite row keys, optimizer-setting preservation, error propagation, full Oracle cleanup and rich typed replay into a fresh connection after dropping the original helpers.

Native fixtures were captured on DuckDB 1.5.6; volatile runtime counters are removed while operator trees, extra_info and executed scan types are preserved. One explicitly synthetic test removes a duplicate producer predicate to isolate CTE consumer-lineage handling.

This repository remains a source export requiring the matching complete SQLancer framework and build configuration.

Native references: [DuckDB 1.5 profiling/JSON plans](https://duckdb.org/docs/1.5/dev/profiling), [ART index eligibility and EXPLAIN ANALYZE](https://duckdb.org/docs/1.5/guides/performance/indexing), [CTE materialization](https://duckdb.org/docs/1.5/sql/query_syntax/with).
