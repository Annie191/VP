# Databend ECST

Databend ECST implements the TiDB/MySQL ECST testing contract through controlled methods on `DatabendVerticalPartitioningOracle`: an immutable snapshot and operation, directional target/configuration selection, physical-plan observations, semantic/data gates, independent result comparison, two-level feedback, repeat validation and final-state replay. It uses Databend's FUSE storage and native SQL semantics.

## Run in the matching complete SQLancer workspace

```sh
java -ea -jar target/sqlancer-2.0.0.jar \
  --host 127.0.0.1 --port 23309 --username root --password '' \
  --num-threads 1 --random-seed 19 --num-tries 1 \
  --max-generated-databases 1 --num-queries 1 --max-num-inserts 20 \
  --use-connection-test false --timeout-seconds 600 \
  --database-prefix ecst_bend_cli \
  databend --oracle ECST --ecst-query-pairs 3
```

Use the actual MySQL-protocol endpoint of the intended Databend server. The implementation uses one connection; acceptance runs on a private Databend 1.2.950 single query node. It creates only its own database and prefixed helpers, without cluster DDL, global settings, gateway rotation, COPY stages or external files. The repository remains a source export requiring the matching complete SQLancer framework and build configuration.

`--ecst-query-pairs` counts comparable pairs, including empty results with compatible output types. The bounded coordinator tries up to eight seeds per requested pair and four configurations per selected target; JSONL summaries distinguish completed pairs, generation skips and rejected prerequisites. A zero process exit alone does not establish that the requested budget was completed.

Both sides use the same session profile, one execution thread and UTC. Incoming optimizer, join-reordering, materialized-CTE and pruning switches are preserved. Captured settings accompany SQL replay. ECST bypasses VP's mixed-engine, mutation, reclustering/compaction and known-bug suppression paths. The legacy VP entry remains available.

## Controlled VP interfaces

The adapter reuses VP column descriptors, native type/DDL handling, source and partition population, JOIN projection, value canonicalization and floating-point comparison. It snapshots an eligible numeric/character source once, assigns BIGINT row identities, and fixes source layout, SQL and operation before choosing a target. Direct cross-column comparisons can move into HashJoin filters; arithmetic, conditional and character-length expressions are also generated.

The legal matrix has 63 configurations across WHERE/SELECT/HAVING: same-side/cross-side partitions, complete-source views, JOIN/derived/CTE/view reconstruction, and legal inner WHERE, inner SELECT or JOIN ON placement. Derived and CTE reconstructions use explicit MATERIALIZED CTEs. View reconstruction uses ordinary views: this Databend build cannot bind a MATERIALIZED CTE in a CREATE VIEW definition. Views are expanded by the optimizer, and are not reported as materialized merely because a view exists.

FUSE does not provide an enforced rowid primary-key constraint here. Data gates explicitly check snapshot cardinality and complete, unique non-null row identities on source and partitions. They verify copied values, including exact hex text/binary values, typed nested JSON representations, bitmap contents and Geometry EWKB. Semantic gates check column sets/native types and frozen native table/view definitions, detecting nullability or layout changes. Failed or unknown prerequisites invalidate the cached partition layout; the next attempt rebuilds it while retaining the original snapshot.

A filtered view may remove rows; its predicate semantics remain the responsibility of result comparison. It is not rejected merely because its row count differs from the unfiltered source.

## Native execution evidence

The analyzer parses the physical operator tree returned by `EXPLAIN`, not SQL text or aliases invented by the harness. EXPLAIN errors propagate.

| Dimension | Evidence and limits |
| --- | --- |
| LOCATION | PRE_JOIN / POST_JOIN from a structurally matched operation and its physical input tree |
| OPERATOR | SCAN_FILTER from TableScan push downs, FILTER from Filter, JOIN_CONDITION from join filters, and PROJECTION from EvalScalar expressions |
| BOUNDARY | MERGED when the actual source relations remain visible; opaque materialized-CTE scans remain UNKNOWN |
| ACCESS_PATH | TABLE_SCAN / PARTITION_PRUNED from native `partitions total` and `partitions scanned` on the actual source relations |

`PARTITION_PRUNED` requires a positive denominator and fewer scanned partitions. `TABLE_SCAN` means no partition reduction was observed; it does not exclude internal filtering or Bloom indexes. The adapter never calls FUSE block pruning a B-tree/index scan. Pruning candidates use FUSE blocks and a dependency cluster key/sorted load; comparison candidates scatter the partition load by a deterministic rowid hash. Engine decisions may still miss the requested target.

Column annotations such as `c0 (#1)` are normalized only outside quoted literals; bare `#N` references and display/output aliases are insufficient evidence. Physical input relations must cover the selected dependencies. Duplicate placements and unrecognized expressions remain UNKNOWN.

On the tested build, MATERIALIZED CTE EXPLAIN displays a temporary TableScan name without the producer's source lineage. The analyzer does not equate that name with a source table and does not claim a confirmed boundary migration. Boundary targets remain directional attempts, whose observations can be UNKNOWN. Result comparison still runs after accepted prerequisites, including target misses and unknown observations.

## Comparison and independent replay

Comparison checks native output type, precision/scale and labels even for empty output; duplicate multiplicity and character values are preserved. Floating-point values use VP tolerances. Binary results use bytes. SELECT failures propagate through the strict JDBC path without legacy error or known-pattern suppression. Mismatches must reproduce before a bug is reported.

Replay includes captured session settings, canonical executable DDL, literal source data, only the final helper layout and deferred view setup. It does not reuse SHOW CREATE's non-executable positional Tuple member labels. Scalars travel through UTF-8 hex with an explicit original type; BINARY uses native hex, ARRAY/MAP/TUPLE values are recursively constructed with native types, VARIANT uses parse_json, Bitmap uses to_bitmap, and Geometry uses EWKB including SRID. Decimal precision, timestamp microseconds, nested nulls, quote/NUL characters and binary bytes are preserved without the original source tables or connection. Native schema parsing now recognizes BINARY distinctly from VARCHAR.

Records in `logs/databend/ecst-<database>.jsonl` include both SQL statements/plans, target/configuration, observed shifts, result counts, history, replay and failure diagnostics. Reproduced mismatches and unexpected failures retain helpers for investigation. Replay restores data and SQL; it does not promise an identical optimizer plan or causal confirmation.

## Validation

```sh
mvn -o -Djacoco.skip=true \
  -Ddatabend.ecst.live=true -Ddatabend.ecst.port=23309 \
  -Dtest=DatabendEcstIntegrationTest,DatabendExecutionContextAnalyzerTest,EcstHistoryTest,DatabendSchemaTest \
  test
```

On 2026-11-08, the exact source-export variant passed 44 targeted tests: 8 live, 7 native-plan, 24 history and 5 schema tests, with no failures or skips. It also passed compilation, packaging, scoped formatting and Checkstyle. The full-workspace legacy VP suite passed 94 tests; unrelated local VP changes are excluded from this export.

The packaged CLI completed 3 requested, nonempty and equal pairs. All 3 boundary observations were UNKNOWN because of the documented opaque CTE scan, and all were compared. All 219 server-default settings were unchanged; all 16 captured session settings were identical across pairs. The private server had one query node and no ECST helpers remained after the CLI.

Live tests are opt-in and use unique databases dropped in cleanup. They cover all 63 configurations, actual FUSE pruning in both directions, scan-filter to join-condition placement, NULLs/duplicates/grouping, data/metadata corruption and retry, empty-output type mismatches, unchanged incoming optimizer settings, unexpected error propagation, full coordinator cleanup and rich typed replay in a new database/connection. Native plan fixtures retain their original fields; the count-validation unit test explicitly uses synthetic partition totals to exercise invalid and reduced denominators.

The reference for native EXPLAIN fields is [Databend EXPLAIN](https://docs.databend.com/sql/sql-commands/explain-cmds/explain); actual version-specific behavior is exercised against the server.
