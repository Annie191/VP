# CockroachDB ECST on a single node

CockroachDB ECST implements the TiDB/MySQL/OceanBase testing contract through controlled interfaces in `CockroachDBVerticalPartitioningOracle`. It uses CockroachDB SQL and native physical-plan evidence on one configured connection. The existing VP entry point remains available.

## Run in the matching complete SQLancer project

```sh
java -jar target/sqlancer-2.0.0.jar \
  --host 127.0.0.1 --port 26257 --username root --password '' \
  --num-threads 1 --random-seed 37 --max-num-inserts 0 \
  --max-generated-databases 1 --num-queries 1 --timeout-seconds 1800 \
  --database-prefix ecst_crdb \
  cockroachdb --oracle ECST --ecst-query-pairs 4
```

Use a single-node CockroachDB instance, such as one started with `cockroach start-single-node`, and supply its `--host` and `--port`. No gateway list, live-node minimum, cluster-identity query or internal membership privilege is required. ECST keeps the worker's existing connection for the complete seed/transform/comparison lifecycle.

Before snapshot creation, ECST fixes timezone to UTC, `distsql` to `off`, byte output to hex and tracing to off. Other optimizer/formatting settings remain in the session and are captured with their parameter types for replay. ECST rejects VP distributed-gateway, distributed-profile and row/range-pressure options. The legacy VP entry point retains its existing options.

ECST reuses VP's bounded oracle-run permit and schema-change backpressure. Bootstrap uses `defaultdb`; ECST does not change cluster settings or database GC zones and does not enable VP's time-travel, row-pressure or randomized optimizer intervention profiles.

## Controlled VP contract

Source rows, row identities, source indexes, original SQL and the logical operation are fixed before choosing a target. Transformations cover same-side/cross-side vertical partitions, full source views, JOIN/derived/CTE/view reconstruction and legal WHERE/SELECT/HAVING, JOIN ON, inner WHERE and inner SELECT embeddings: 63 configurations for a two-column seed across the three original contexts.

The source and physical partitions use existing VP column-definition, projection and population primitives. Operations select numeric or string dependencies. Replay-safe payloads include booleans, bits, bytes, arrays, JSON and temporal types; tables containing user-defined enum types (including enum arrays) or computed columns are excluded from the ECST seed pool. Legacy advanced VP generation remains separate.

Unchanged seed/layout/index requirements reuse partition tables. Layout/index changes and switching representation rebuild them; INVALID or UNKNOWN metadata/data preconditions invalidate reuse. Each configuration still checks column sets/types/collations, the rowid-only primary key, source/partition row counts, row identity and copied values. Strings are checked as bytes. A filtered view may remove rows; correctness of that filter is checked by result comparison rather than a precondition that could discard a bug.

`MATERIALIZED` CTEs form retained boundaries in derived, CTE and view reconstruction. The native `buffer` producer and `scan buffer` consumer provide materialization evidence, including when the consumer and producer are separate branches of the plan. Ordinary view syntax alone is not such evidence.

Source and partition relations use ordinary local setup. ECST does not issue range splits or SCATTER commands, collect leaseholder/replica coverage, rotate connections or probe other endpoints. Empty and very small sources remain valid without synthetic amplification.

## Native plan evidence

`EXPLAIN (VERBOSE)` supplies the physical operator tree. Expression structure and relevant physical inputs identify the fixed operation:

| Dimension | Supported target states and evidence |
| --- | --- |
| LOCATION | PRE_JOIN / POST_JOIN, from the matched filter/predicate/render and its inputs |
| OPERATOR | FILTER / JOIN_CONDITION, from `filter` / join `pred` |
| BOUNDARY | MERGED / MATERIALIZED, following buffer producer/consumer lineage |
| ACCESS_PATH | TABLE_SCAN / INDEX_RANGE / INDEX_SCAN, from the relevant scan's table/index and spans |

An explicit `render <column>:` can identify scalar projection evaluation. A `columns:` list, original SQL text or a routing alias cannot. Primary-key range access has its own state and is not relabeled as secondary-index access. Absorbed index predicates can give access-path evidence while operation placement remains UNKNOWN. Ambiguous or unsupported expressions remain UNKNOWN and do not blacklist configurations.

Migration evidence remains `PLAN_ONLY`. The original and transformed native plans are recorded with the fixed operation; the single-node adapter makes no distributed execution or causal-confirmation claim.

## Results, feedback and replay

Two-level history selects directional targets and concrete configurations using outcomes, coverage and cost. Rejected setup/preconditions do not become migration trials. Every accepted pair is compared even when the target shift fails or is unknown. Counters report the current invocation; learned history persists across checks.

ECST checks result metadata even for empty results, compares multisets with multiplicity, preserves strings/bits/bytes and retains the established VP floating tolerances. Its strict mode is separate from legacy VP normalization. Mismatches must repeat before being reported. Unexpected internal SQL/EXPLAIN errors propagate; they are not disguised as unknown observations or known-pattern suppression. Reproduced mismatches and unexpected failures retain source artifacts. Prepared unexpected failures also attempt to capture standalone replay data.

Replay contains session settings, final-object DDL, typed literal source data and no distributed setup commands. It does not accumulate obsolete configuration attempts or depend on the generated base table. Explicit literal types preserve bit lengths, bytes, arrays, temporal precision and NUMERIC NaN. Database qualifications in catalog DDL are removed without editing string literals. Optimizer choices can change on replay; the bundle reconstructs the data/layout and queries, without guaranteeing the original plan.

JSONL records are written to `logs/cockroachdb/ecst-<database>.jsonl`, including `executionMode: SINGLE_NODE`, original/transformed plans and final-state replay.

## Validation

Offline tests use native v26.2.3 physical-plan fixtures, including materialized producer/consumer lineage. Live acceptance covers all 63 configurations, NULLs, duplicates, grouping, character HAVING, empty output, metadata/data corruption and recovery, row-identity constraints, independent rich-type replay after deleting original helpers, both materialization directions and execution on the same single-node connection without distributed setup.

```sh
mvn -Djacoco.skip=true \
  -Dtest=CockroachDBExecutionContextAnalyzerTest,CockroachDBEcstIntegrationTest,EcstHistoryTest,CockroachDBProviderTest \
  '-Dcockroachdb.ecst.test.url=jdbc:postgresql://127.0.0.1:26257/defaultdb?sslmode=disable' test
```

Set `cockroachdb.ecst.test.user/password` when needed. Live tests skip when no endpoint is supplied. Configured connection or SQL errors fail acceptance.

### Verified on 2026-11-07

- CockroachDB v26.2.3 on an isolated `start-single-node` instance with a memory store; test setup confirmed exactly one live node.
- 46 targeted tests passed with no failures, errors or skips: 12 plan/session tests, 24 history tests, one provider test and nine single-node integration tests. The live matrix covered all 63 legal configurations and independent rich-type replay. Five native plan fixtures were recaptured on this single node; the 12 plan/session tests passed again with those fixtures.
- Clean packaging and scoped Checkstyle passed. The packaged artifact contains the single-node session helper and no obsolete ECST cluster helper classes.
- The final packaged CLI, seed 37 and a three-pair budget, completed three comparable, equal, nonempty pairs. One target migration succeeded and two missed. Result sizes were 8/8, 5/5 and 9/9; original and transformed plans all reported local execution.
- The preparation log and replay contained no range splits or SCATTER commands. All 1,074 cluster settings matched the pre-run snapshot, and no ECST helpers remained after the CLI run. Own test databases were removed and the isolated instance was stopped after validation.

These checks used the matching complete SQLancer workspace. This repository remains a source export rather than a standalone build.

Native syntax references: [EXPLAIN](https://www.cockroachlabs.com/docs/stable/explain), [materialized CTEs](https://www.cockroachlabs.com/docs/stable/common-table-expressions).
