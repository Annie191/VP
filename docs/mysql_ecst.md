# MySQL ECST

MySQL ECST has its own `--oracle ECST` entry point. It reuses the controlled construction, data validation, multiset comparison, and cleanup methods in `MySQLVerticalPartitioningOracle`. Random VP exploration continues to use its existing entry point.

## Running

Use a MySQL endpoint supporting `EXPLAIN FORMAT=TREE` and CTEs (MySQL 8.0.18 or later). For example, from a built checkout:

```sh
java -jar target/sqlancer-2.0.0.jar \
  --host 127.0.0.1 --port 3306 --username root --password '' \
  --num-threads 1 --random-seed 37 --database-prefix ecst_mysql \
  --max-generated-databases 1 --num-queries 10 \
  mysql --oracle ECST --ecst-query-pairs 4
```

`--ecst-query-pairs` counts comparable query pairs per invocation. Each invocation has a bounded seed-attempt budget; its summary reports the number actually completed. Existing VP source-row limits also apply. The source relation pool is refreshed periodically and bounded to eight base tables. Helper tables use InnoDB so ECST can attribute changes to relation structure rather than a simultaneous engine change.

Logs are JSONL at `logs/mysql/ecst-<database>.jsonl`. They include case SQL, operation, observed shifts, both plans and evidence, comparison counts, reproduction status, source-data replay, and feedback summaries. As with other SQLancer workloads, use a dedicated working directory if existing logs must be retained: SQLancer's normal logger initializes its provider log directory on startup.

## Contract and supported transformations

The source snapshot, source indexes, original SQL, and logical operation identity are fixed before target selection. Controlled transformations can use:

- Vertical partitioning with dependencies placed on the same side or across two sides, preserving a unique non-null `vp_rowid`.
- A full source view as the relation representation.
- A mergeable join reconstruction, a derived-table/CTE reconstruction with an all-rows LIMIT barrier, or a `TEMPTABLE` view reconstruction.
- WHERE, SELECT, and grouped HAVING seeds; legal WHERE-to-JOIN-ON/inner-WHERE and SELECT-to-inner-SELECT embeddings.

The candidate set is filtered by source context, dependency layout, and legal embedding, without a fixed candidate-list truncation. It is passed to the shared two-level feedback selector. Configuration attempts and materialization failures have bounded retry handling; materialization cost does not create a migration trial.

Current seeds exercise numeric and string operations, CAST, COALESCE, and CASE over MySQL's existing five-type schema model. HAVING uses dependency groups, COUNT, and MIN(row identity), preserving duplicate input rows and NULL values. Empty inputs are legal, although constant-folded plans can leave no observable target.

Targets are directional canonical keys, independent of TiDB naming:

| Dimension | States |
| --- | --- |
| LOCATION | PRE_JOIN / POST_JOIN for the matched predicate |
| OPERATOR | FILTER / JOIN_CONDITION for the matched predicate |
| BOUNDARY | MERGED / MATERIALIZED for the operation's input relation |
| ACCESS_PATH | TABLE_SCAN / INDEX_RANGE / INDEX_LOOKUP / INDEX_SCAN |

`DERIVED_TABLE` and `CTE` use `LIMIT 18446744073709551615`, which preserves all input rows while introducing an optimizer boundary. `VIEW` reconstruction explicitly requests `ALGORITHM=TEMPTABLE`. These are recorded construction controls; migration is accepted only from actual plan evidence.

This adapter does not interpret MySQL server/engine placement as TiDB Root/Cop, or conflate materialization with index condition pushdown. It does not currently expose ICP, hash-vs-nested-loop, or evaluation-environment targets. A projection omitted from TREE cannot support an assertion about projection evaluation location. Relation-specific materialization/access evidence can still support those respective dimensions.

## Correctness and feedback

Static and data preconditions validate column sets, types/collations, row identity, multiplicity, and copied values. Metadata/read failures return UNKNOWN and cannot authorize a comparison. Character values are checked bytewise; result comparison preserves string and binary values and uses numeric normalization only for numeric result types. Floating point comparison retains the VP tolerances. Output labels and JDBC types are checked, including for empty results.

The TREE analyzer compares structural expression fingerprints and follows the target's input relations. It distinguishes confirmed success, confirmed failure, and insufficient evidence. Access predicates absorbed into an index range can provide access-path evidence without being treated as an observed predicate relocation. Unknown outcomes never blacklist configurations or become failures. All confirmed auxiliary shifts are recorded independently of the selected target; nonempty coverage is tracked separately.

Every semantically valid materialized pair is compared, including failed/unknown migrations. A mismatch must repeat on the same connection and data before it is reported. Such reports preserve artifacts and include a replay built from the final source/partition/view DDL and literal source values. Retried configurations cannot contribute stale CREATE or INSERT statements. SQL-mode-independent string/byte literals allow replay in an independent database. Minimization and causal intervention are explicitly unperformed; plan differences alone do not establish the cause of a result mismatch.

## Validation

Unit tests cover directional feedback, structural expression matching, ambiguous/missing operations, relation-specific access and materialization, string/binary normalization, and replay literals. Opt-in live acceptance tests cover the legal transformation matrix, NULL/duplicate/group semantics, materialization in both directions, corrupted data/metadata, independent replay after retries, empty input, and the oracle loop with cleanup.

```sh
mvn -Djacoco.skip=true \
  -Dtest=EcstHistoryTest,MySQLExecutionContextAnalyzerTest,MySQLEcstIntegrationTest,MySQLCaseOperatorTest,MySQLToStringVisitorTest,TiDBECSTOracleCandidateSynthesisTest,TiDBExecutionContextAnalyzerTest \
  -Dmysql.ecst.test.url=jdbc:mysql://127.0.0.1:3306/ test
```

The endpoint property is optional for offline runs. When configured, connection/SQL failures fail the integration suite; they are not converted to skipped tests. Use `mysql.ecst.test.user` and `mysql.ecst.test.password` for credentials when required.

Validated on 2026-11-06 against MySQL 9.7.1: all 112 targeted tests passed, including live acceptance over all 63 legal configurations. A packaged CLI smoke run with seed 37 completed 12 comparable/equal query pairs: 4 target successes and 8 target failures, with result comparison performed in both cases. No helper artifacts remained. The independent temporary MySQL instance was shut down after validation.

Scoped Checkstyle passed for the new adapter/analyzer/capability classes and the factory/options. The three existing files with baseline style violations were compared against their pre-change contents: 59 baseline violations versus 57 current violations, with no new violation categories/counts. Full-repository verification was not claimed; the previously observed TiDB historical-fixture test failures are outside this change.
