# PostgreSQL ECST

PostgreSQL ECST 通过现有 VP 的受控接口实现功能等价迁移。使用 `postgres --oracle ECST`，要求 PostgreSQL 12 或更新版本（使用原生 `AS MATERIALIZED` CTE）。当前仓库仍是源码导出，运行需要 README 所述的完整配套 SQLancer 工程。

## 运行

在完整配套工程中构建后运行，例如：

```bash
java -ea -jar target/sqlancer-2.0.0.jar \
  --host 127.0.0.1 --port 5432 --username postgres --password postgres \
  --num-threads 1 --num-tries 1 --max-generated-databases 1 --num-queries 1 \
  --database-prefix ecst_pg_ \
  postgres --connection-url postgresql://127.0.0.1:5432/postgres \
  --test-tablespaces false --oracle ECST --ecst-query-pairs 4 \
  --vertical-partitioning-rows 64 --ecst-snapshot-rows 256
```

全局连接参数放在 `postgres` 前，数据库专有选项放在后面。连接角色需要创建测试数据库、表、视图和索引的权限。数据库名由 SQLancer 生成；请使用独立测试前缀。

`--ecst-query-pairs` 默认 4，指定一次检查期望完成的可比较查询对数。每个检查最多尝试预算的 8 倍种子，每个种子最多尝试 4 个配置；耗尽后日志会明确给出实际完成数，不保证所有环境都达到预算。`--ecst-snapshot-rows` 默认 256，0 复制全部源行。已有 `--vertical-partitioning-rows` 控制 VP 输入表的大表轮廓，不改变冻结后的种子。

## 复用范围与不变量

复用 VP 的 `VpColumn` 原生目录元数据、`VpSchema`、连接投影、分区填充 SQL、原生表构造、结果规范化和浮点容差多重集比较。新增入口为 `createEcstSeed`、`applyEcstTransformation`、`prepareEcstCase`、语义/数据校验、`compareEcstCase`、失败复验、数据重放及清理。

源快照只创建一次，包含独立 BIGINT 行标识主键。源 SQL、逻辑操作标识、依赖列、源索引及是否保留物化边界在选择目标之前确定；变换重试只重建辅助表示。支持 WHERE、SELECT、HAVING、同侧/跨侧列布局、垂直分区/视图，以及 JOIN、DERIVED_TABLE、CTE、VIEW 重构，共 63 种合法配置（WHERE 30、SELECT 22、HAVING 11）。同一种子的相同布局及索引要求复用分区；任何前提校验拒绝都会使缓存失效。

复制保留原生类型、类型修饰符、排序规则和非空属性；默认表达式、生成列和标识列在快照中保存已经求得的值，不重新求值。行标识必须是唯一的单列主键。物理列、约束和索引定义被冻结并逐配置检查；复制数据使用原生文本输出的 UTF-8 字节进行空值安全比较，避免文本排序规则、JSON 缺少相等操作符或隐式数值转换掩盖数据变化。当前可独立重放 PostgreSQL 内置类型及内置数组；用户自定义枚举、域、复合类型、扩展类型及用户定义排序规则缺少 DDL 依赖时，在创建快照之前跳过该种子。

## 原生计划证据

解析 `EXPLAIN (VERBOSE, FORMAT JSON)`，目标来自 PostgreSQL 原生计划，不根据 SQL 字面写法、索引存在与否或变换配置推断成功。PostgreSQL 官方文档说明了 [JSON EXPLAIN 格式](https://www.postgresql.org/docs/current/sql-explain.html)和[扫描、连接及过滤节点](https://www.postgresql.org/docs/current/using-explain.html)。

- 位置：`PRE_JOIN` ↔ `POST_JOIN`，依据匹配操作节点及其相关物理子树。
- 算子：`SCAN_FILTER`/`FILTER` ↔ `JOIN_CONDITION`，识别 `Filter`、`Index Cond`、`Recheck Cond`、`Hash Cond`、`Merge Cond`、`Join Filter`。Bitmap 的 heap recheck 与 index 条件只计为同一操作的一次证据。
- 边界：`MERGED` ↔ `MATERIALIZED`，需要相关真实输入的 `InitPlan` / `Subplan Name: CTE ...` 生产者证据。普通视图会被展开，`OFFSET 0` 子查询保留边界但不能据此声称物化；这类不充分证据记为 UNKNOWN。无关标量子计划或连接内部缓存不会被当成相关 CTE 边界。
- 访问：`TABLE_SCAN` ↔ `INDEX_SCAN`/`INDEX_ONLY_SCAN`/`BITMAP_SCAN`，依据相关真实输入的实际选中节点；多个输入证据不一致或输入缺失时记为 UNKNOWN。

表达式用结构指纹匹配，并验证相关输入关系；仅消除 PostgreSQL 添加的数值常量类型装饰，保留列和表达式上的显式 CAST。重复匹配、无法解析的表达式、缺失关系以及 SELECT 投影操作的身份不确定时保持 UNKNOWN。`Output` 的重复显示不是执行次数或因果证据。

这里是优化器计划证据，日志标记 `PLAN_ONLY`，不使用 `EXPLAIN ANALYZE`，不宣称因果确认或实际运行时计划已获验证。计划可能随版本、统计信息和设置变化；迁移未命中和 UNKNOWN 都继续进行结果比较。

## 会话、比较与重放

两侧在同一个 JDBC 连接上执行。统一 UTC、ISO 日期、标准字符串、interval/bytea 输出及浮点序列化；保留输入会话的查询规划设置，不对单侧关闭扫描或连接算法。重放捕获 Query Tuning 设置和序列化/资源参数，通过 `pg_catalog.set_config` 恢复，包含正确的 `search_path` 列表语义。ECST 不调用随机 VP 的优化器差异探测或集群配置变更。

结果比较使用严格 JDBC 查询路径，比较多重集及列标签、原生类型、精度和小数位；空结果也比较输出类型。新入口不采用旧 VP 的已知模式压制或宽泛错误白名单。SQLSTATE `22003`、`22012`、`57014` 分别作为数值溢出、除零和取消/超时记录；其他查询和计划错误向上传播。结果不一致会重复执行一次确认。

日志位于 `logs/postgres/ecst-<database>.jsonl`，包含目标、配置、计划、证据、前提校验、比较和汇总。`sourceDataReplay` 包含会话设置、最终源/分区表 DDL 及索引、原生类型输入文字的 UTF-8 十六进制数据、分区填充和最终视图 DDL，不依赖原表或旧连接。复现先执行该列表，再执行 `replayQueries`。异常保留辅助对象和诊断，正常结束清理自己创建的对象。

## 验证

```bash
mvn -o -Djacoco.skip=true \
  -Dpostgres.ecst.live=true -Dpostgres.ecst.host=127.0.0.1 \
  -Dpostgres.ecst.port=5432 -Dpostgres.ecst.user=postgres \
  -Dpostgres.ecst.password=postgres \
  -Dtest=PostgresEcstIntegrationTest,PostgresExecutionContextAnalyzerTest,EcstHistoryTest \
  package
```

实库测试创建独立数据库并在结束后删除。也可通过 `PG_ECST_PASSWORD` 环境变量提供测试密码。计划夹具来自 PostgreSQL 12.22 和 18.6，见测试夹具目录；访问类型和错误输入的合成变体在测试代码中明确构造。

验收包括全部 63 种配置、扫描谓词到连接条件、真实索引访问与物化边界的双向变化、数据/类型/行标识篡改后拒绝及重建恢复、空结果类型比较、原生错误传播、规划设置保持、完整协调器清理，以及文本、BYTEA、BIT、JSON/JSONB、高精度数值、带时区时间、INET、UUID、数组、范围和 interval 的跨连接独立重放。

最终导出版本在 PostgreSQL 12.22 和 18.6 上各通过 39 项针对性测试（10 项实库验收、5 项计划解析、24 项历史反馈），无失败或跳过；两者均覆盖 63 种配置。已有 VP 回归测试另执行 18 项，其中 16 项通过、2 项按原有条件跳过。打包后的 PostgreSQL 12.22 命令行完成 3 对非空且相等的查询：2 次迁移成功、1 次未命中，源/变换行数分别为 64/64、64/64、48/48，每次重放捕获 58 项会话设置，辅助对象剩余 0。18.6 的打包命令行另完成 3 对相等查询，结果行数为 10、10、0，包含 2 次迁移成功和 1 次 UNKNOWN；UNKNOWN 同样执行比较。

测试使用两个独立临时实例；18.6 复用已有镜像中的二进制，使用单独的目录、端口和内存文件系统。临时实例关闭磁盘同步以减少共享磁盘等待；验收关注查询语义，不验证崩溃恢复或持久性。既有 18.6 服务的 399 项全局设置保持一致。
