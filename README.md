# VP idea 核心源码

本仓库展示基于 SQLancer 的 Vertical Partitioning（VP）实现，包含 12 个数据库或模式的 Oracle 和 5 个直接配套的辅助类。

这些文件按原项目包路径保留，内容与导出时的工作区源码一致。这里只收录核心实现，依赖 SQLancer 框架及对应数据库的 Provider、Options、Schema、AST、Visitor 和表达式生成器，不能独立编译运行。完整运行环境需要原项目中的配套改动，不能只将这些文件放入任意版本的上游 SQLancer。

## VP Oracle

| 数据库或模式 | 源码 |
| --- | --- |
| ClickHouse | [ClickHouseVerticalPartitioningOracle.java](src/sqlancer/clickhouse/oracle/ClickHouseVerticalPartitioningOracle.java) |
| CockroachDB | [CockroachDBVerticalPartitioningOracle.java](src/sqlancer/cockroachdb/oracle/CockroachDBVerticalPartitioningOracle.java) |
| Databend | [DatabendVerticalPartitioningOracle.java](src/sqlancer/databend/oracle/DatabendVerticalPartitioningOracle.java) |
| DuckDB | [DuckDBVerticalPartitioningOracle.java](src/sqlancer/duckdb/DuckDBVerticalPartitioningOracle.java) |
| MariaDB | [MariaDBVerticalPartitioningOracle.java](src/sqlancer/mariadb/oracle/MariaDBVerticalPartitioningOracle.java) |
| MonetDB | [MonetDBVerticalPartitioningOracle.java](src/sqlancer/monetdb/oracle/MonetDBVerticalPartitioningOracle.java) |
| MySQL | [MySQLVerticalPartitioningOracle.java](src/sqlancer/mysql/oracle/MySQLVerticalPartitioningOracle.java) |
| OceanBase | [OceanBaseVerticalPartitioningOracle.java](src/sqlancer/oceanbase/oracle/OceanBaseVerticalPartitioningOracle.java) |
| OceanBase Oracle 模式 | [OceanBaseVerticalPartitioningOracle.java](src/sqlancer/oceanbaseoracle/oracle/OceanBaseVerticalPartitioningOracle.java) |
| PostgreSQL | [PostgresVerticalPartitioningOracle.java](src/sqlancer/postgres/oracle/PostgresVerticalPartitioningOracle.java) |
| SQLite3 | [SQLite3VerticalPartitioningOracle.java](src/sqlancer/sqlite3/oracle/SQLite3VerticalPartitioningOracle.java) |
| TiDB | [TiDBVerticalPartitioningOracle.java](src/sqlancer/tidb/oracle/TiDBVerticalPartitioningOracle.java) |

## 配套辅助类

- [DatabendVpP1.java](src/sqlancer/databend/oracle/DatabendVpP1.java)
- [DatabendVpQueryQuality.java](src/sqlancer/databend/oracle/DatabendVpQueryQuality.java)
- [DuckDBValueCanonicalizer.java](src/sqlancer/duckdb/DuckDBValueCanonicalizer.java)
- [MonetDBMutationFeedback.java](src/sqlancer/monetdb/oracle/MonetDBMutationFeedback.java)
- [MonetDBResultComparison.java](src/sqlancer/monetdb/oracle/MonetDBResultComparison.java)

## 来源与校验

- 基于 [SQLancer](https://github.com/sqlancer/sqlancer)，保留原 MIT 许可证，见 [LICENSE.md](LICENSE.md)。
- 导出来源工作区的 HEAD：`e92cd2d8fa254d095ed6b21d92d0f7c144071e8e`。源码包含该 HEAD 之后工作区中尚未提交的修改，不等同于该提交的内容。
- 导出日期：2026-10-17。
- `MANIFEST.sha256` 记录当前校验清单覆盖文件的 SHA-256，可执行 `sha256sum -c MANIFEST.sha256` 校验。
- 测试、日志、数据库数据、构建产物及旧 Git 历史未包含在本次导出中。

## ECST 扩展

本次导出还包含 TiDB ECST 的反馈驱动骨架：

- [TiDBECSTOracle.java](src/sqlancer/tidb/oracle/TiDBECSTOracle.java)
- [TiDBExecutionContextAnalyzer.java](src/sqlancer/tidb/oracle/TiDBExecutionContextAnalyzer.java)
- [TiDBVerticalPartitioningOracle.java](src/sqlancer/tidb/oracle/TiDBVerticalPartitioningOracle.java) 中的受控 VP/ECST seam
- `src/sqlancer/common/oracle/ecst/` 中的上下文、历史反馈、规则和变换配置类
- [EcstHistoryTest.java](test/sqlancer/common/oracle/ecst/EcstHistoryTest.java)

ECST 与 VP 一样依赖完整 SQLancer 工程中的 TiDB Provider、Options、Schema、AST 和构建配置，当前仓库仍是源码导出，不是独立构建项目。


## MySQL ECST

已加入 MySQL ECST 的完整测试链路，复用 MySQL VP 的受控接口，实现目标选择、关系变换、计划识别、双层反馈、语义前提校验、结果比较和独立回放。

- [MySQLECSTOracle.java](src/sqlancer/mysql/oracle/MySQLECSTOracle.java)：独立 ECST Oracle，迁移未命中或无法确认时仍比较查询结果。
- [MySQLExecutionContextAnalyzer.java](src/sqlancer/mysql/oracle/MySQLExecutionContextAnalyzer.java)：基于表达式结构和输入关系识别 MySQL TREE 计划中的迁移证据。
- [MySQLEcstCapabilities.java](src/sqlancer/mysql/oracle/MySQLEcstCapabilities.java)：MySQL 支持的方向性目标与合法变换配置。
- [MySQLVerticalPartitioningOracle.java](src/sqlancer/mysql/oracle/MySQLVerticalPartitioningOracle.java)：固定种子快照、受控物化、类型感知比较、重试后的最终状态回放及清理。
- MySQL Provider、Options 和 OracleFactory 接入文件，以及 MySQL 单元/实库验收测试。

在完整配套 SQLancer 工程中使用 `mysql --oracle ECST --ecst-query-pairs 4`。具体实现范围、启动参数和验证命令见 [MySQL ECST 说明](docs/mysql_ecst.md)。本仓库仍保留源码导出结构，README 上述完整工程依赖说明同样适用。

完整工作区中已有 112 项针对性测试通过；MySQL 9.7.1 命令行验证完成 12 对等价查询，其中 4 次目标迁移成功、8 次未命中，均执行了结果比较。此次导出未加入构建产物、运行日志或数据库文件。

## OceanBase ECST

已加入 OceanBase MySQL 模式的 ECST，复用 OceanBase VP 的受控接口，实现目标与配置选择、语义和数据前提校验、结果比较、反馈、重试及独立回放。

- [OceanBaseECSTOracle.java](src/sqlancer/oceanbase/oracle/OceanBaseECSTOracle.java)：独立 ECST 入口及测试协调。
- [OceanBaseExecutionContextAnalyzer.java](src/sqlancer/oceanbase/oracle/OceanBaseExecutionContextAnalyzer.java)：解析原生 `EXPLAIN EXTENDED_NOADDR` 的算子树及 `Outputs & filters`，识别谓词位置、算子角色、查询块边界和访问路径。
- [OceanBaseEcstCapabilities.java](src/sqlancer/oceanbase/oracle/OceanBaseEcstCapabilities.java)：方向性目标及合法配置。
- OceanBase VP 受控接口、Provider、Options、OracleFactory、输入表选择，以及单元测试、实库验收测试和原生计划样本。
- [EcstExpressionMatcher.java](src/sqlancer/common/oracle/ecst/EcstExpressionMatcher.java)：MySQL 与 OceanBase 共用的表达式结构匹配。

OceanBase 的 `SUBPLAN SCAN` 表示保留查询块边界；实现将其记为 `RETAINED_VIEW`，仅凭该算子不会宣称发生临时表物化。ECST 保持当前聚合下推设置。

同一种子下相同布局和索引要求的分区表可以复用，仍逐配置校验元数据、数据和查询结果；前提校验拒绝后会重建分区。

在完整配套 SQLancer 工程中使用 `oceanbase --oracle ECST --ecst-query-pairs 4`。运行参数、适配范围和验收命令见 [OceanBase ECST 说明](docs/oceanbase_ecst.md)。当前仓库仍是依赖完整配套工程的源码导出。

完整工作区中 49 项针对性测试通过，包含 OceanBase CE 4.4.2.1 上全部 63 种合法配置及双方向边界迁移验收。打包后的命令行完成 2 对非空且相等的查询，两个迁移目标均未命中，仍执行了结果比较并清理了辅助对象。

## CockroachDB ECST（单节点）

已将 CockroachDB ECST 调整为单节点实现，复用 VP 受控接口，实现固定种子快照、方向性目标与配置选择、变换、语义和数据前提校验、结果比较、双层反馈及独立回放。

- 使用 `--host`/`--port` 指定的一个连接，关闭分布式 SQL 执行，无三节点最低要求、入口轮换、range 拆分或 SCATTER。
- 解析原生 `EXPLAIN (VERBOSE)`，支持 filter/join pred/render、物化 CTE 的 buffer/scan buffer 关联及索引访问证据。
- 保留 63 种合法配置、类型感知的多重集比较、失败复验和最终状态富类型数据回放；不修改集群设置和数据库 GC 配置。

在完整配套 SQLancer 工程中使用 `cockroachdb --oracle ECST --ecst-query-pairs 4`；不传 VP 分布式参数。参数、证据规则和验收命令见 [CockroachDB ECST 说明](docs/cockroachdb_ecst.md)。当前仓库仍是依赖完整配套工程的源码导出。

CockroachDB v26.2.3 隔离单节点上 46 项针对性测试通过，无失败或跳过，覆盖全部 63 种配置及独立回放。最终命令行完成 3 对非空且相等的查询，1 次迁移成功、2 次未命中；输入准备和回放均无拆 range/SCATTER，1,074 项集群设置保持一致，辅助对象已清理。
