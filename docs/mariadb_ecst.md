# MariaDB ECST

MariaDB ECST implements the TiDB/MySQL testing contract through controlled methods on `MariaDBVerticalPartitioningOracle`: an immutable source snapshot and operation, directional target/configuration selection, native plan observations, semantic/data gates, independent result comparison, two-level feedback, repeated mismatch validation, final-state replay and cleanup. MariaDB's native JSON plans and materialization rules are used directly.

## Run in the matching complete SQLancer workspace

```sh
java -ea -jar target/sqlancer-2.0.0.jar \
  --host 127.0.0.1 --port 23317 --username root --password '' \
  --num-threads 1 --random-seed 19 --num-tries 1 \
  --max-generated-databases 1 --num-queries 1 --max-num-inserts 20 \
  --use-connection-test false --timeout-seconds 600 \
  --database-prefix ecst_maria_cli \
  mariadb --oracle ECST --ecst-query-pairs 3
```

Use the actual MariaDB endpoint and credentials. Acceptance runs use MariaDB 11.8.5 and an isolated data directory. The adapter uses one connection and InnoDB helper tables. It does not create server processes, change global variables or run VP's optimizer-profile/known-pattern suppression paths.

`--ecst-query-pairs` counts comparable pairs, including empty results with compatible output types. Each invocation has a bounded budget of eight seeds per requested pair and four configurations per selected target. JSONL summaries separately report requested/completed counts, generation skips, prerequisite rejections and incomparable results; a zero process exit alone does not prove completion.

The session uses UTC for timestamp values. Incoming SQL mode, character-set/collation, optimizer switches, join-cache and selectivity settings are shared by both queries and captured for replay. Provider preparation skips random SET actions for ECST. The legacy VP entry retains its existing behavior.

## Controlled VP interfaces

The adapter reuses VP Column/Plan/VpTables descriptors, native column definitions, JOIN projection, source-row limit, character/binary canonicalization and numeric tolerance. Each seed snapshots its input once with BIGINT primary-key row identities and fixes its source index, SQL and operation before selecting a target. Queries use numeric expressions, character length and direct cross-column comparisons in WHERE, SELECT and HAVING contexts.

The legal matrix contains 63 configurations: same-side/cross-side partitions, complete-source views, JOIN/derived/CTE/view reconstruction, and legal inner WHERE, inner SELECT or JOIN ON placement. Matching layout/index requirements reuse helper tables. Failed or unknown prerequisites invalidate the cache; retries rebuild transformed helpers while preserving the source.

Semantic gates verify native column types, character sets/collations, exact column sets, sole-rowid primary keys and frozen source/helper/view definitions. Data gates check cardinalities, complete row identities and copied values using null-safe equality; character and binary payloads are compared through native hex. Filtered views may remove rows: their predicate semantics remain subject to result comparison rather than a row-count prerequisite that could hide a bug.

Derived/CTE blocks use a non-restricting LIMIT of 18446744073709551615 to prevent merging. Reconstruction views use ALGORITHM=TEMPTABLE. This works on the tested 11.8 series without assuming MariaDB 12.1 NO_MERGE hints or MySQL TREE syntax. JOIN ON configurations use STRAIGHT_JOIN with the right rowid index ignored to make a native join-buffer condition inducible. The rowid key remains enforced, and the native plan must confirm the actual placement; the syntax alone is insufficient evidence.

## Native plan evidence

The analyzer reads `EXPLAIN FORMAT=JSON`. It builds physical input trees from nested_loop, table, block-nl-join and materialized producer fields, retaining sort/temporary-table wrappers. EXPLAIN failures propagate.

| Dimension | Evidence |
| --- | --- |
| LOCATION | PRE_JOIN / POST_JOIN from a structurally matched operation and its physical inputs |
| OPERATOR | SCAN_FILTER from table conditions, FILTER from post-join/multi-input or HAVING conditions, JOIN_CONDITION from a native block-nl-join condition |
| BOUNDARY | MERGED / MATERIALIZED from the relevant source's actual producer ancestry |
| ACCESS_PATH | ALL → TABLE_SCAN; range → INDEX_RANGE; ref/eq_ref/const → INDEX_LOOKUP; index → INDEX_SCAN |

A multi-input attached condition on a nested-loop table is a post-join filter, not automatically a join-buffer condition. Materialization in an unrelated subquery does not establish the selected operation's boundary. Access evidence is restricted to relevant source/helper inputs; possible_keys or a created index alone cannot establish selected access.

Projection display aliases and unavailable expression details remain UNKNOWN. Duplicate operation mappings, incomplete input relations and unsupported JSON/expressions are also UNKNOWN. Accepted cases still compare results when a migration misses or is unknown. These are optimizer-plan observations, not causal confirmation or a promise of identical runtime plans during replay.

## Results and independent replay

Strict JDBC comparison preserves duplicate multiplicity, native output type/precision/scale/labels even for empty output, character/binary bytes, and VP floating-point tolerances. It bypasses known-pattern and broad expected-error suppression. Unexpected SELECT/EXPLAIN failures propagate; a result mismatch must reproduce before reporting.

Replay contains session settings, executable final table DDL (including source/final helper indexes), literal source rows and only the final view setup. It needs neither the original base table nor an old connection. Text and binary values travel as hex without embedding their contents in SQL string literals. BIT values use exact bit literals; unsigned integers and decimals keep their full native text precision; INET6/UUID values use character conversion; ENUM/SET use ordinal/mask values rather than ambiguous display strings. Native temporal precision, JSON text, NUL/quotes and non-UTF-8 character sets are retained. Numeric session variables are restored with numeric literals. Native schema parsing recognizes binary/blob declarations and FLOAT.

Records in `logs/mariadb/ecst-<database>.jsonl` include target/configuration, fixed operation, both SQL/plans, observed shifts, result counts, feedback history, replay and failure diagnostics. Reproduced mismatches and unexpected failures retain helpers for inspection. Successful/ordinary skipped cases clean up their helpers.

## Validation

```sh
mvn -o -Djacoco.skip=true \
  -Dmariadb.ecst.live=true -Dmariadb.ecst.port=23317 \
  -Dtest=MariaDBEcstIntegrationTest,MariaDBExecutionContextAnalyzerTest,EcstHistoryTest \
  test
```

On 2026-11-08, the exact source-export variant passed all 39 targeted tests: 9 live, 6 native-plan and 24 history tests, with no failures or skips. Compilation, packaging, scoped formatting and Checkstyle also passed. Unrelated local VP changes are excluded from this export.

The packaged CLI completed all 3 requested pairs, each nonempty and equal: 1 migration succeeded and 2 targets missed, and all were compared. All 682 global variables were unchanged; all 10 captured session settings were identical across pairs. No ECST helpers remained.

Live tests opt in against the configured endpoint and use unique databases dropped in cleanup. They cover all 63 configurations; native materialization and range-access shifts in both directions; scan-filter to join-buffer condition placement; NULLs/duplicates/grouping; exact string and JSON-text corruption, weakened keys and metadata drift with recovery; empty output types; unchanged incoming optimizer switches; error propagation; full coordinator cleanup; and rich replay in a fresh database/connection.

Native fixtures retain MariaDB 11.8.5 JSON fields. Unit tests explicitly mutate those fixtures for regular attached-condition and access-type mapping checks; live tests independently validate real optimizer decisions.

Primary references: [MariaDB JSON EXPLAIN](https://mariadb.com/docs/server/reference/sql-statements/administrative-sql-statements/analyze-and-explain-statements/explain-format-json), [derived-table merging](https://mariadb.com/docs/server/ha-and-performance/optimization-and-tuning/query-optimizations/optimizations-for-derived-tables/derived-table-merge-optimization), [versioned table-level hints](https://mariadb.com/docs/server/ha-and-performance/optimization-and-tuning/optimizer-hints/table-level-hints).

This repository remains a source export requiring the matching complete SQLancer framework and build configuration.
