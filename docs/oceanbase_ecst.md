# OceanBase ECST

OceanBase ECST implements the same testing contract as the TiDB/MySQL adapters, using OceanBase MySQL-compatible SQL and native execution-plan evidence. It has its own `--oracle ECST` entry point and controlled interfaces in `OceanBaseVerticalPartitioningOracle`.

## Running

In the complete matching SQLancer project, build the package and use an existing MySQL-mode tenant:

```sh
java -jar target/sqlancer-2.0.0.jar \
  --host 127.0.0.1 --port 2881 --username root@test --password '' \
  --num-threads 1 --random-seed 37 --database-prefix ecst_ob \
  --max-generated-databases 1 --num-queries 10 \
  oceanbase --oracle ECST --ecst-query-pairs 4
```

The adapter operates in the MySQL-mode `oceanbase` provider, separately from `oceanbaseoracle`. `--ecst-query-pairs` counts comparable query pairs, with bounded seed/configuration retries. Summaries report the actual completed count. Logs and independent replay bundles are written to `logs/oceanbase/ecst-<database>.jsonl`.

## Controlled transformation contract

The source snapshot, source indexes, original SQL and logical operation are fixed before target selection. Subsequent attempts replace only the transformed representation. Configurations cover vertical partitioning with same-side/cross-side dependencies, full source views, JOIN/derived-table/CTE/view reconstruction, and legal WHERE/SELECT/HAVING, JOIN ON, inner WHERE and inner SELECT embeddings.

For the same seed, unchanged partition layouts and index requirements reuse their physical partition tables. Every configuration still runs metadata, data and result checks. Changing the layout/index requirements, switching representation, or rejecting a precondition invalidates that reuse, so a retry rebuilds the partitions rather than inheriting rejected state.

The retained query-block variants use an all-rows LIMIT barrier, including in view definitions. Ordinary view or `ALGORITHM=TEMPTABLE` syntax is not taken as proof of materialization: the installed OceanBase optimizer can merge such views. A `SUBPLAN SCAN` indicates a retained query-block boundary; it does not by itself demonstrate temporary-table materialization.

Source generation uses the existing OceanBase VP column definitions and population primitives. Metadata and data checks verify row identity, multiplicity, column sets, type/collation equality and copied values. Numeric HAVING seeds can group duplicate dependency tuples; character-dependent HAVING seeds include row identity so PAD SPACE/case-insensitive group equivalence cannot choose different representative strings.

Column metadata comes from `information_schema.COLUMNS`: OceanBase's `SHOW FULL COLUMNS` can report empty collations for views even when the resolved columns retain their source collations. Row identity requires a primary key consisting solely of `vp_rowid`; a composite primary key does not prove that property.

The ECST session keeps the current aggregation-pushdown setting. It does not invoke VP's capability probes that toggle this setting, disable aggregation pushdown by default, freeze a tenant, or apply VP's storage/partition/column-store intervention profiles. Session settings are captured for replay.

## OceanBase execution context

The analyzer reads `EXPLAIN EXTENDED_NOADDR`, joining the operator table to per-operator `Outputs & filters`. It matches predicates only in `filter(...)` and join `other_conds(...)`, excludes outline/hint/output-only mentions, follows the target input relations, and converts OceanBase's internal `cast(value, DECIMAL(p,s))` representation into a structural SQL expression fingerprint.

| Dimension | Active target states |
| --- | --- |
| LOCATION | PRE_JOIN / POST_JOIN |
| OPERATOR | FILTER / JOIN_CONDITION |
| BOUNDARY | MERGED / RETAINED_VIEW |
| ACCESS_PATH | TABLE_SCAN / INDEX_RANGE / INDEX_SCAN |

Primary-key gets/ranges and explicit materialization, when observed, have their own context values and are not relabeled as secondary-index access or a retained view. Missing or conflicting operation evidence remains UNKNOWN. A predicate absorbed into an index range can support access-path evidence without proving predicate relocation. Projection routing in an output list alone cannot establish where a scalar operation was evaluated.

The shared structural fingerprint implementation is `EcstExpressionMatcher`; the existing MySQL analyzer uses the same implementation, with its own native plan parser. Directional history keys remain independent of DBMS-specific naming conventions.

## Results, feedback, and replay

Two-level feedback selects targets and configurations using observed transitions and costs. Unknown outcomes never blacklist configurations, and rejected materializations do not create migration trials. Confirmed auxiliary shifts and nonempty coverage are recorded separately from target success.

Every semantically accepted pair is compared even if migration fails or is unknown. ECST result comparison checks output metadata, preserves character/binary values exactly, normalizes numeric result types only, and retains the established floating tolerances. This strict ECST mode is separate from the legacy OceanBase VP result normalization and PAD SPACE compatibility behavior.

Mismatches must repeat on the same connection and data before reporting. Final-object DDL, captured session settings and literal source values produce a standalone replay after configuration retries. A filtered view's predicate correctness belongs to the result oracle, rather than a precondition that could discard the difference being tested. Minimized/causally established bug claims are not made: evidence is explicitly PLAN_ONLY.

## Validation

Offline tests include captured OceanBase 4.4.2.1 scan/join/subplan/index plans, native cast normalization, ambiguous/absent operations, strict result encoding and the shared MySQL fingerprint regression tests. Opt-in integration tests exercise all 63 legal configurations, NULLs, duplicates, grouping, both boundary directions, metadata/data corruption, independent replay after deleting original helpers, empty inputs, cleanup, and unchanged aggregation-pushdown state.

```sh
mvn -Djacoco.skip=true \
  -Dtest=EcstHistoryTest,MySQLExecutionContextAnalyzerTest,OceanBaseExecutionContextAnalyzerTest,OceanBaseEcstIntegrationTest \
  '-Doceanbase.ecst.test.url=jdbc:mysql://127.0.0.1:2881/?useSSL=false&serverTimezone=UTC' \
  -Doceanbase.ecst.test.user=root@test test
```

Use `oceanbase.ecst.test.password` if required. Live tests skip only when no endpoint property is supplied; configured connection and SQL errors fail the suite.

Native syntax reference: [OceanBase EXPLAIN](https://docs.oceanbase.com/oceanbase-database/V4.2.1-explain-mysql-compatible-mode).

## Validation snapshot (2026-11-06)

- Live endpoint: OceanBase CE 4.4.2.1, MySQL mode.
- All 49 targeted tests passed with no failures, errors or skips: 41 offline tests and 8 live integration tests. The live suite also confirmed both retained/merged boundary directions, independent replay after deleting original helpers, corrupted-partition recovery, row-identity constraints, empty results and unchanged aggregation-pushdown settings.
- The live controlled matrix passed all 63 legal WHERE/SELECT/HAVING configurations, including metadata/data checks, immutable original-query/operation checks, repeated result comparisons and helper cleanup.
- Package build: `mvn -q -DskipTests -Djacoco.skip=true package` passed.
- The packaged CLI completed two comparable, equal pairs using seed 37, `--max-num-inserts 0`, `--num-queries 1` and `--ecst-query-pairs 2`. Both targets missed; result comparison still ran, matching 3 and 13 nonempty result rows respectively. The summary recorded no precondition/materialization rejects or incomparable pairs, and no ECST helper objects remained afterward.
