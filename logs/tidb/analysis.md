# ECST 5-hour run analysis

Scope: records containing `ecstshort5h_20261103` in shared TiDB JSONL files.

- Duration: 2026-11-02 18:59–23:59 UTC; external 18,000-second timeout.
- Progress: 50,660 generated queries.
- Records: 36,219 cases and 1,533 seed skips.
- Case outcomes: 5,748 successful target migrations, 29,340 target failures, 1,131 UNKNOWN/precondition-invalid cases.
- Result oracle: 22,512 comparable equal results, 12,576 incomparable results, 0 mismatches, 0 bug candidates.
- Root -> Cop TiKV: 12,481 precondition-accepted and EXPLAIN-analyzed attempts, 0 migration successes.
- Candidate synthesis for Root -> Cop: 1,755 seed summaries and 12,285 applicable candidate configurations.
- Cop TiKV -> Root: 1,361 successes / 8,398 failures / 594 unknown.
- Pushdown Cop -> Root: 1,396 successes / 8,374 failures / 533 unknown.
- Selection -> MergeJoin: 2,987 successes / 3 failures.
- IndexReader -> TableFullScan: 4 successes / 84 failures / 4 unknown.
- Observed shifts: Cop->Root location 5,747; Pushdown->NonPushdown boundary 5,747; Selection->MergeJoin 5,739; IndexFullScan->TableFullScan 41; IndexReader->TableFullScan 4.
- HAVING: 2,922 non-unique dependency-group queries and 8,141 row-id-group queries. Seed skips: 1,532 HAVING and 1 WHERE.
- Source rows: 4,068 empty snapshots among case records; 1,300 of 5,748 successful migrations used empty snapshots.
- Materialization failures: 21,804, dominated by VIEW + JOIN_ON configurations (7,910 CTE, 7,324 derived-table, 6,570 view). These candidates were filtered after this run and are not migration evidence.

Conclusion: the new counters prove that Root -> Cop still has real candidates reaching EXPLAIN, but remains completely unreachable with the current SQL shapes. A dedicated single-side Selection/Projection pushdown template is still required before treating this target as productive.
