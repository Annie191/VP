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
