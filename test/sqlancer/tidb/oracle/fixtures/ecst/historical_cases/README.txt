These fixtures are the former TiDB VP logic-bug reproductions represented in
ECST vocabulary.  Each directory contains the original setup, the single-table
query, and the VP-rewritten query.  TiDB plans captured from the current
v9.0.0-beta.2.pre instance are in ../historical-plans.jsonl.

The mapping from a plan pair to an ECST target/configuration is defined by
TiDBEcstHistoricalCase.  It is evidence for candidate scheduling and rule
coverage; it does not bypass the normal seed applicability, capability, or
row-id checks.
