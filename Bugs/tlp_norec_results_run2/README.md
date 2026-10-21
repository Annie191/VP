# TLP / NoREC 执行结果

每个子目录对应 `Bugs/<数据库>/<文件>.sql`，包含：

- `rewritten.sql`：建表/插入后第一条顶层单表查询，以及 TLP 基线、三分支 UNION ALL、NoREC 优化/非优化查询。
- `results.json`：每条查询的实际输出、错误信息和不一致标记。
- `summary.tsv`：按数据库统计。

TLP 不一致表示去掉谓词的结果与真/假/未知三分支合并结果的多重集不同。NoREC 统计将空集聚合的 `NULL` 与计数语义的 `0` 视为相同；其余值按原样比较。

本次目录中 87 个文件均已尝试。数据库端点不可用、没有可识别的顶层单表查询、或查询形态含当前改写器不支持的 `GROUP BY`/`ORDER BY`/窗口/嵌套结构的文件，保留了 `unavailable`、`no_single_table_query` 或 `skipped` 状态，没有臆造结果。
