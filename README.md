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

## ClickHouse ECST（单节点）

已加入 ClickHouse 单节点 ECST，复用 VP 受控接口完成固定种子快照、目标/配置选择、关系变换、语义和数据校验、结果比较、双层反馈及独立回放。

- 使用本地 MergeTree 辅助表和一个连接，无 Distributed 表、ON CLUSTER DDL 或远程入口轮换。
- 解析原生 JSON 物理计划及表达式动作，识别 JOIN 前后位置、实际 CTE 物化，以及稀疏主键的真实 granule 裁剪。
- 非等值 JOIN ON 在实测计划中仍由 Join 后的 Filter 执行，不误报 JOIN_CONDITION；匿名 CTE 内存读取缺少来源信息时，表达式位置保留 UNKNOWN。
- 支持全部 63 种合法变换配置，校验 rowid 唯一性和原生类型/数据；空结果仍比较类型，String/FixedString 以服务端 hex 编码保持字节精度。
- 最终布局回放保留原生二进制、数组、Map、Tuple、Decimal、DateTime64 等数据，不依赖原始输入表或过期重试对象。

在完整配套 SQLancer 工程中使用 `clickhouse --oracle ECST --ecst-query-pairs 3`。启动参数、适配边界和验收命令见 [ClickHouse ECST 说明](docs/clickhouse_ecst.md)。当前仓库仍是依赖完整配套工程的源码导出。

准备发布的源码在配套完整工程中通过 41 项测试，无失败或跳过，包含 ClickHouse 26.7.1.1315 隔离单节点上的 9 项实库验收及全部 63 种配置。

同一构建产物的命令行完成 3 对结果相等的查询（2 对非空、1 对为空）：两个迁移观察为 UNKNOWN，一个未命中，均执行了比较。437 项服务器设置保持一致，测试对象及隔离服务已清理。


## DuckDB ECST（嵌入式单节点）

已加入 DuckDB ECST，复用 VP 受控接口，按功能等价实现固定种子快照、方向性目标/配置选择、关系变换、语义与数据校验、结果比较、双层反馈及独立回放。

- 使用嵌入式数据库和一个连接，支持全部 63 种合法变换配置。
- 解析原生 `EXPLAIN (ANALYZE,FORMAT JSON)`，识别实际 ART 索引扫描、CTE 物化边界和 FILTER/JOIN 条件位置；证据不足时保留 UNKNOWN，仍比较结果。
- 保留重复行、空结果类型以及 BLOB、JSON、Decimal、纳秒时间戳和嵌套值的精度，支持在新连接中独立回放。

在完整配套 SQLancer 工程中使用 `duckdb --oracle ECST --ecst-query-pairs 3`，无需服务器或连接凭证。实现范围与验收命令见 [DuckDB ECST 说明](docs/duckdb_ecst.md)。当前仓库仍是依赖完整配套工程的源码导出。

待发布源码在 DuckDB 1.5.6 / JDBC 1.5.6.0 上通过 60 项针对性测试，无失败或跳过，覆盖全部 63 种配置及 12 项嵌入式实库验收。打包后的命令行完成 3 对非空且相等的查询，3 次迁移均成功；各查询对使用相同优化器、排序规则和 ART 设置。


## Databend ECST

已加入 Databend ECST，复用 VP 受控接口，按功能等价实现固定快照、目标/配置选择、关系变换、原生计划观察、语义与数据校验、结果比较、双层反馈及独立回放。

- 使用一个连接和 FUSE 辅助表，支持全部 63 种合法配置；不使用集群 DDL、全局优化器干预或外部 COPY 文件。
- 识别原生扫描谓词、Filter、HashJoin 条件和 EvalScalar 表达式；访问路径依据实际分区裁剪计数，验证了双向变化。
- 物化 CTE 的原生计划缺少生产者来源时保留 UNKNOWN，仍执行结果比较；普通视图展开不误报为物化。
- 校验唯一行标识、类型及复制值，支持二进制、嵌套类型、Variant、Bitmap、Decimal、时间戳和带 SRID 的 Geometry 独立回放。

在完整配套 SQLancer 工程中使用 `databend --oracle ECST --ecst-query-pairs 3`。运行参数、证据边界和验收命令见 [Databend ECST 说明](docs/databend_ecst.md)。当前仓库仍是依赖完整配套工程的源码导出。

待发布源码在 Databend 1.2.950 隔离单节点上通过 44 项针对性测试，无失败或跳过，包含 8 项实库验收和全部 63 种配置。打包后的命令行完成 3 对非空且相等的查询，3 次边界迁移观察均为 UNKNOWN，均执行了比较。219 项服务器默认设置保持一致，辅助对象已清理。
