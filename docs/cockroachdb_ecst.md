# CockroachDB ECST on three nodes

CockroachDB ECST implements the TiDB/MySQL/OceanBase testing contract through controlled interfaces in `CockroachDBVerticalPartitioningOracle`. It uses CockroachDB SQL, physical-plan evidence, and real same-cluster SQL gateways. The existing VP entry point remains available.

## Run in the matching complete SQLancer project

```sh
java -jar target/sqlancer-2.0.0.jar \
  --host 127.0.0.1 --port 26261 --username root --password '' \
  --num-threads 1 --random-seed 37 --max-num-inserts 0 \
  --max-generated-databases 1 --num-queries 1 --timeout-seconds 1800 \
  --database-prefix ecst_crdb \
  cockroachdb --oracle ECST --ecst-query-pairs 4 \
  --vp-distributed-gateways 127.0.0.1:26261,127.0.0.1:26259,127.0.0.1:26260
```

The port numbers are an example, not node identities. ECST checks `crdb_internal.cluster_id()` and live membership: it requires at least three live nodes and rejects gateways belonging to another cluster. `--vp-distributed-min-live-nodes` can request a stricter minimum. The VP distributed-profile enable flag is not needed for ECST. With no gateway list, the initial gateway is used and three-node membership is still required.

Gateways rotate between seeds, after the preceding seed has been cleaned up. A seed's plan analysis, transformation and result comparisons use one gateway. Session formatting/optimizer settings and their parameter types are preserved when reconnecting and in replay; ECST fixes timezone to UTC, `distsql` to `always`, byte output to hex, and tracing to off before creating each snapshot. It reuses VP's bounded oracle-run permit and schema-change backpressure. ECST database bootstrap uses `defaultdb`, does not change cluster settings or database GC zones, and does not apply VP's time-travel, row-pressure, or randomized optimizer intervention profiles.

## Controlled VP contract

Source rows, row identities, source indexes, original SQL and the logical operation are fixed before choosing a target. Transformations cover same-side/cross-side vertical partitions, full source views, JOIN/derived/CTE/view reconstruction and legal WHERE/SELECT/HAVING, JOIN ON, inner WHERE and inner SELECT embeddings: 63 configurations for a two-column seed across the three original contexts.

The source and physical partitions use existing VP column-definition, projection and population primitives. Operations select numeric or string dependencies. Replay-safe payloads include booleans, bits, bytes, arrays, JSON and temporal types; tables containing user-defined enum types (including enum arrays) or computed columns are excluded from the ECST seed pool. Legacy advanced VP generation remains separate.

Unchanged seed/layout/index requirements reuse partition tables. Layout/index changes and switching representation rebuild them; INVALID or UNKNOWN metadata/data preconditions invalidate reuse. Each configuration still checks column sets/types/collations, the rowid-only primary key, source/partition row counts, row identity and copied values. Strings are checked as bytes. A filtered view may remove rows; correctness of that filter is checked by result comparison rather than a precondition that could discard a bug.

`MATERIALIZED` CTEs form retained boundaries in derived, CTE and view reconstruction. The native `buffer` producer and `scan buffer` consumer provide materialization evidence, including when the consumer and producer are separate branches of the plan. Ordinary view syntax alone is not such evidence.

Each populated physical relation is split at deterministic rowid quartiles and scattered, scoped to the generated tables. Empty and very small sources remain valid without synthetic amplification. Range evidence is read from actual `SHOW RANGES ... WITH DETAILS`; replica and leaseholder coverage are never inferred from the number of configured nodes or requested splits.

## Plan evidence and actual distributed execution

`EXPLAIN (VERBOSE)` supplies the physical operator tree. Expression structure and relevant physical inputs identify the fixed operation:

| Dimension | Supported target states and evidence |
| --- | --- |
| LOCATION | PRE_JOIN / POST_JOIN, from the matched filter/predicate/render and its inputs |
| OPERATOR | FILTER / JOIN_CONDITION, from `filter` / join `pred` |
| BOUNDARY | MERGED / MATERIALIZED, following buffer producer/consumer lineage |
| ACCESS_PATH | TABLE_SCAN / INDEX_RANGE / INDEX_SCAN, from the relevant scan's table/index and spans |

An explicit `render <column>:` can identify scalar projection evaluation. A `columns:` list, original SQL text or a routing alias cannot. Primary-key range access has its own state and is not relabeled as secondary-index access. Absorbed index predicates can give access-path evidence while operation placement remains UNKNOWN. Ambiguous or unsupported expressions remain UNKNOWN and do not blacklist configurations.

After comparison, `EXPLAIN ANALYZE (VERBOSE)` collects execution coverage independently for the original and transformed SQL. It records SQL processor node IDs, KV read node IDs, distribution, gateway and the full runtime plan. A `distribution: full` header alone does not prove that a query used multiple SQL nodes. Multiple replicas or KV nodes do not prove multiple SQL processors. Cluster-wide runtime coverage is not promoted to per-expression evaluation evidence or causal confirmation; migration evidence remains `PLAN_ONLY`.

## Results, feedback and replay

Two-level history selects directional targets and concrete configurations using outcomes, coverage and cost. Rejected setup/preconditions do not become migration trials. Every accepted pair is compared even when the target shift fails or is unknown. Counters report the current invocation; learned history persists across checks.

ECST checks result metadata even for empty results, compares multisets with multiplicity, preserves strings/bits/bytes and retains the established VP floating tolerances. Its strict mode is separate from legacy VP normalization. Mismatches must repeat before being reported. Unexpected internal SQL/EXPLAIN errors propagate; they are not disguised as unknown observations or known-pattern suppression. Reproduced mismatches and unexpected failures retain source artifacts. Prepared unexpected failures also attempt to capture standalone replay data.

Replay contains session settings, final-object DDL, typed literal source data and deterministic split/scatter commands. It does not accumulate obsolete configuration attempts or depend on the generated base table. Explicit literal types preserve bit lengths, bytes, arrays, temporal precision and NUMERIC NaN. Database qualifications in catalog DDL are removed without editing string literals. Range IDs, lease placement and optimizer choices can change on replay; the bundle reconstructs the data/layout and queries, not a causal guarantee of the original plan.

JSONL records are written to `logs/cockroachdb/ecst-<database>.jsonl`, including `clusterCoverage`, original/transformed execution evidence and final-state replay. SQL/node coverage errors are recorded separately from a completed result comparison.

## Validation

Offline tests use native v26.2.3 plan fixtures, including a materialized producer/consumer and a runtime plan that used SQL nodes n1/n3 and KV nodes n1/n2/n3. Live acceptance covers all 63 configurations, NULLs, duplicates, grouping, character HAVING, empty output, metadata/data corruption and recovery, row-identity constraints, independent rich-type replay after deleting original helpers, both materialization directions, same-snapshot comparisons through three gateways, actual remote execution, and rejection of a foreign/single-node cluster.

```sh
mvn -Djacoco.skip=true \
  -Dtest=CockroachDBExecutionContextAnalyzerTest,CockroachDBEcstIntegrationTest,EcstHistoryTest,CockroachDBProviderTest \
  '-Dcockroachdb.ecst.test.url=jdbc:postgresql://127.0.0.1:26261/defaultdb?sslmode=disable' \
  '-Dcockroachdb.ecst.test.gateways=127.0.0.1:26261,127.0.0.1:26259,127.0.0.1:26260' \
  '-Dcockroachdb.ecst.test.foreign.url=jdbc:postgresql://127.0.0.1:26258/defaultdb?sslmode=disable' test
```

Set `cockroachdb.ecst.test.user/password` when needed. The foreign URL must refer to a separate single-node test instance; it is read only. Live tests skip when no endpoint is supplied; foreign/single-node rejection additionally needs that foreign URL. Configured connection or SQL errors fail acceptance.

### Verified on 2026-11-07

- CockroachDB v26.2.3, isolated three-node cluster with memory stores and a separate single-node instance for rejection tests. All three cluster nodes were live and ranges had at least three replicas before acceptance.
- 47 targeted tests passed with zero failures, errors or skips: 12 native-plan/session tests, 24 history tests, one provider test and 10 live integration tests. The live matrix checked all 63 legal configurations. Same-snapshot execution through all three gateways observed actual multi-node SQL processing and KV reads.
- The packaged CLI, seed 37 and a three-pair budget, completed three comparable, equal, nonempty pairs through gateways n1/n3/n2. Source snapshots contained eight rows; result sizes were 8/8, 8/8 and 4/4. All three selected target migrations missed; result comparisons still ran. Two original queries used multiple SQL nodes; transformed queries used one SQL node while reading KV data remotely. The execution records preserve this distinction.
- All 1,074 cluster-setting values matched the pre-run snapshot after tests and CLI execution. No ECST helpers remained after the CLI run; the test databases were removed. The isolated services were stopped after validation.
- Offline packaging and scoped Checkstyle checks passed in the complete matching SQLancer workspace. This source-export repository does not include that workspace's build configuration or all framework dependencies.

Native syntax references: [EXPLAIN](https://www.cockroachlabs.com/docs/stable/explain), [materialized CTEs](https://www.cockroachlabs.com/docs/stable/common-table-expressions), [SQL and KV execution-node statistics](https://www.cockroachlabs.com/docs/stable/explain-analyze).
