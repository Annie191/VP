# VP

## idea 思想


在满足无损重建条件的前提下，通过垂直拆分数据并构造多种重建表达，生成等价查询对，检测数据库的逻辑错误。

把一张表按列拆成几张表，再通过行标识把它们重建回来，对原表和重建结果执行同一个查询，结果应该一致。

假设原表 `T(id, a, b, c)` 被拆成：

~~~text
L(id, a)
R(id, b, c)
~~~

用共同的行标识 `id` 重建后，

~~~sql
SELECT ...
FROM L JOIN R ON L.id = R.id
~~~

逻辑上应满足：

~~~text
T ≡ L JOIN R ON L.id = R.id
~~~

因此，对相同的查询 `Q`：

~~~text
Q(T) = Q(reconstruct(L, R))
~~~

## 流程

1. 生成或选择一张基准表，收集列名、类型、可空性、默认值、表引擎等信息，并判断表的大致形状，例如空表、单列或多列。
2. 为每行建立稳定的标识 `vp_rowid`，从而让不同分区中的列能够对应回同一条逻辑记录。
3. 将快照物化到独立的源表中。

~~~sql
INSERT INTO vp_source (vp_rowid, a, b, c)
SELECT ROW_NUMBER() OVER (ORDER BY stable_key), a, b, c
FROM base_table;
~~~

按不同策略把列拆成左右分区表，分区表通常都包含行标识：

~~~text
L(vp_rowid, a)
R(vp_rowid, b, c)
~~~

4. 用连接、子查询、CTE、视图或临时表等方式重建逻辑表。

~~~sql
-- 普通连接
SELECT ...
FROM L l JOIN R r ON l.vp_rowid = r.vp_rowid
~~~

5. 构造两条语义上对应的查询：

~~~text
基准查询：Q(vp_source)
改写查询：Q(reconstruct(L, R))
~~~

6. 在原表和重建关系上运行等价查询（包含投影、过滤、聚合、排序等操作，以覆盖不同执行路径。）将结果转换为可比较的表示：
- 对**无序结果**，比较行的多重集合：行内容和重复次数都要相同，不能因为比较时忽略顺序而丢掉重复行差异。
- 对**有序结果**，除多重集合外，还会检查顺序是否一致。
- 对 `NULL`、复杂类型和数据库驱动返回的不同表示，会做相应规范化。
- 对部分浮点或近似计算结果，可按类型和场景使用容差比较；精确结果仍要求完全一致。

## 拆分左右分区表的具体策略

**多列表拆分策略**

| 策略 | 做法 | 示例 |
|---|---|---|
| **均匀拆分 `EVEN_SPLIT`** | 先随机打乱列，再交替分到左右表，尽量平均。 | `L(id, a, c)`，`R(id, b, d)` |
| **重叠拆分 `OVERLAPPED_SPLIT`** | 先做均匀拆分，再随机挑一列，确保它同时出现在左右表。 | `L(id, a, c)`，`R(id, b, c, d)` |
| **偏斜拆分 `SKEWED_SPLIT`** | 随机挑一列放一边，其余列放另一边，制造不均衡布局。 | `L(id, a)`，`R(id, b, c, d)` |
| **按类型拆分 `TYPE_DIRECTED_SPLIT`** | 数值列放一边，非数值列放另一边；若一边为空，则回退到均匀拆分。 | `L(id, 数值列)`，`R(id, 其他列)` |

**单列表拆分策略**

| 策略 | 左分区 | 右分区 | 含义 |
|---|---|---|---|
| `SINGLE_REPLICATED` | `L(id, a)` | `R(id, a)` | 唯一列复制到左右两边，形成重叠列。 |
| `SINGLE_RIGHT_ROW_ID_ONLY` | `L(id, a)` | `R(id)` | 右侧只有行标识，普通列放左侧。 |
| `SINGLE_LEFT_ROW_ID_ONLY` | `L(id)` | `R(id, a)` | 左侧只有行标识，普通列放右侧。 |

**空表拆分策略**

`EMPTY_SPLIT` 策略：按均匀拆分逻辑分配列。多列时通常先打乱列，再交替放入左右分区。


## 重建逻辑表的具体策略

**共有策略16个**


| 分类 | 代码策略名 | 具体含义 | 示例 |
|---|---|---|---|
| 连接重建 | `JOIN` | `L` 与 `R` 按 `vp_rowid` 做内连接，再从两侧投影原列。 | `SELECT l.vp_rowid, l.a, r.b`<br>`FROM L l JOIN R r ON l.vp_rowid = r.vp_rowid;` |
| 连接重建 | `LEFT_RIGHT_JOIN` | 选择左分区或右分区作为驱动表，测试驱动侧行的保留及未匹配行的 NULL 扩展。连接条件可以基于 VP 分区的 vp_rowid，也可以基于业务键或辅助关系中的连接列。 | `SELECT l.vp_rowid, l.a, r.b`<br>`FROM L l LEFT JOIN R r ON l.vp_rowid = r.vp_rowid;` |
| 连接重建 | `FULL_OUTER_SIMULATION` | 保留左右两侧匹配及未匹配的行。实现可能使用原生 `FULL OUTER JOIN`，也可能用外连接与 `UNION ALL` 模拟，具体形式因数据库而异。 | `SELECT l.vp_rowid, l.a, r.b`<br>`FROM L l LEFT JOIN R r ON l.vp_rowid = r.vp_rowid`<br>`UNION ALL`<br>`SELECT r.vp_rowid, l.a, r.b`<br>`FROM R r LEFT JOIN L l ON l.vp_rowid = r.vp_rowid`<br>`WHERE l.vp_rowid IS NULL;` |
| 连接重建 | `NATURAL_OR_USING` | 使用 USING 或 NATURAL JOIN 表达分区关联。USING 显式指定一个或多个连接列，NATURAL JOIN 根据左右表的所有同名列连接；连接列可以是 vp_rowid 或业务列。 | `-- USING：明确按 vp_rowid 连接`<br>`SELECT vp_rowid, L.a, R.b`<br>`FROM L JOIN R USING (vp_rowid);`<br><br>`-- NATURAL JOIN：匹配所有同名列`<br>`SELECT * FROM L NATURAL JOIN R;` |
| 子查询重建 | `SCALAR_SUBQUERY` | 以一侧分区为驱动，通过关联标量子查询按 `vp_rowid` 查出另一侧的列。 | `SELECT l.vp_rowid, l.a,`<br>`       (SELECT r.b FROM R r WHERE r.vp_rowid = l.vp_rowid) AS b`<br>`FROM L l;` |
| 子查询过滤 | `EXISTS_FILTER` | 以一侧分区为驱动，用相关 `EXISTS` 检查另一侧是否存在相同的 `vp_rowid`；若要取出另一侧的列，还需通过关联子查询等方式读取。 | `SELECT l.vp_rowid, l.a,`<br>`       (SELECT r.b FROM R r WHERE r.vp_rowid = l.vp_rowid) AS b`<br>`FROM L l`<br>`WHERE EXISTS (SELECT 1 FROM R r WHERE r.vp_rowid = l.vp_rowid);` |
| 子查询过滤 | `IN_FILTER` | 以一侧分区为驱动，要求其 `vp_rowid` 出现在另一侧分区的行标识集合中；若要取出另一侧的列，还需额外读取。 | `SELECT l.vp_rowid, l.a,`<br>`       (SELECT r.b FROM R r WHERE r.vp_rowid = l.vp_rowid) AS b`<br>`FROM L l`<br>`WHERE l.vp_rowid IN (SELECT r.vp_rowid FROM R r);` |
| 查询结构包装 | `DERIVED_TABLE` | 先将左右分区分别包装为派生表，再按 `vp_rowid` 连接。 | `SELECT l.vp_rowid, l.a, r.b`<br>`FROM (SELECT * FROM L) l`<br>`JOIN (SELECT * FROM R) r ON l.vp_rowid = r.vp_rowid;` |
| 查询结构包装 | `CTE` | 用 `WITH` 定义左右分区，再在后续查询中按 `vp_rowid` 连接。 | `WITH l AS (SELECT * FROM L),`<br>`     r AS (SELECT * FROM R)`<br>`SELECT l.vp_rowid, l.a, r.b`<br>`FROM l JOIN r ON l.vp_rowid = r.vp_rowid;` |
| 物化重建 | `VIEW` | 创建视图，视图定义为分区表连接；外层查询访问视图。 | `CREATE VIEW reconstructed AS`<br>`SELECT l.vp_rowid, l.a, r.b`<br>`FROM L l JOIN R r ON l.vp_rowid = r.vp_rowid;`<br>`SELECT * FROM reconstructed;` |
| 物化重建 | `TEMP_TABLE` | 先通过分区关联生成重建结果，写入临时表或物化表，再从该表读取。ClickHouse 使用 Memory 引擎表物化结果。 | `CREATE TEMPORARY TABLE reconstructed AS`<br>`SELECT l.vp_rowid, l.a, r.b`<br>`FROM L l JOIN R r ON l.vp_rowid = r.vp_rowid;`<br>`SELECT * FROM reconstructed;` |
| 聚合后合并 | `AGGREGATE_THEN_MERGE` | 先按行标识或分组键对某个分区聚合，再将聚合结果与另一分区关联，构造候选结果。 | `WITH r_agg AS (`<br>`  SELECT vp_rowid, MAX(b) AS b`<br>`  FROM R GROUP BY vp_rowid`<br>`)`<br>`SELECT l.vp_rowid, l.a, r_agg.b`<br>`FROM L l JOIN r_agg ON l.vp_rowid = r_agg.vp_rowid;` |
| 行号对齐 | `ROW_NUMBER_ALIGNMENT` | 左右分区分别按某种排序计算 `ROW_NUMBER()`，再按行号配对，而不是直接按原始 `vp_rowid` 连接。 | `WITH l AS (`<br>`  SELECT *, ROW_NUMBER() OVER (ORDER BY vp_rowid) AS rn FROM L`<br>`), r AS (`<br>`  SELECT *, ROW_NUMBER() OVER (ORDER BY vp_rowid) AS rn FROM R`<br>`)`<br>`SELECT l.vp_rowid, l.a, r.b`<br>`FROM l JOIN r ON l.rn = r.rn;` |
| JSON 打包 | `JSON_PACK` | 将一侧分区的列编码为 JSON，再解析 JSON 取回列值并参与重建。 | `WITH packed AS (`<br>`  SELECT vp_rowid, JSON_OBJECT('b', b) AS payload FROM R`<br>`)`<br>`SELECT l.vp_rowid, l.a, JSON_VALUE(p.payload, '$.b') AS b`<br>`FROM L l JOIN packed p ON l.vp_rowid = p.vp_rowid;` |
| 字符串打包 | `STRING_PACK` | 将一侧分区的列转换成字符串后，再通过字符串表示解析或匹配以重建。 | `WITH packed AS (`<br>`  SELECT CAST(vp_rowid AS VARCHAR) AS key_text,`<br>`         CAST(b AS VARCHAR) AS value_text FROM R`<br>`)`<br>`SELECT l.vp_rowid, l.a, CAST(p.value_text AS INTEGER) AS b`<br>`FROM L l JOIN packed p`<br>`  ON CAST(l.vp_rowid AS VARCHAR) = p.key_text;` |
| 相关派生表重建 | `LATERAL_DERIVED` | 以左分区行作为外层输入，右侧 LATERAL 子查询引用当前左侧 `vp_rowid` 查找对应行，再组合两侧列。它与普通派生表的关键区别是子查询可以引用前面的左侧行。 | `SELECT l.vp_rowid, l.a, r.b`<br>`FROM L l`<br>`JOIN LATERAL (`<br>`  SELECT r.b FROM R r`<br>`  WHERE r.vp_rowid = l.vp_rowid`<br>`) r ON TRUE;` |


子查询过滤的两个例子作用相同：筛选出左分区中，在右分区也有相同 `vp_rowid` 的行。**区别是表达方式不同：

- `EXISTS`：检查子查询是否能找到至少一行匹配记录。
- `IN`：检查左侧的 `vp_rowid` 是否属于子查询返回的值集合。

~~~
-- EXISTS：逐行检查是否存在匹配行
WHERE EXISTS (
  SELECT 1
  FROM R r
  WHERE r.vp_rowid = l.vp_rowid
)

-- IN：检查行标识是否属于右侧返回的集合
WHERE l.vp_rowid IN (
  SELECT r.vp_rowid
  FROM R r
)
~~~

在这个场景中，`vp_rowid` 是非 NULL 的行标识，因此两者通常筛出相同的行。右侧即使有重复的 `vp_rowid`，它们也都只是判断“是否有匹配”，不会像普通 JOIN 那样把左侧行复制多份。

同时尝试两种写法，主要是为了测试数据库对 EXISTS 与 IN 两种查询表达及其优化路径的处理，而不是为了表达不同的匹配规则。

**部分数据库特有策略5个**

| 分类 | 代码策略名 | 具体含义 | 支持的数据库 | 示例 |
|---|---|---|---|---|
| 反连接重建 | `ANTI_JOIN_RECONSTRUCTION` | 检查一侧分区中的行是否不存在于另一侧分区，并将未匹配的行补入重建结果，以覆盖内连接无法保留的分区独有行。 | MariaDB | `SELECT r.vp_rowid, NULL, r.b FROM R r WHERE NOT EXISTS (SELECT 1 FROM L l WHERE l.vp_rowid = r.vp_rowid);` |
| 反存在过滤 | `NOT_EXISTS_FILTER` | 使用 `NOT EXISTS` 或反连接风格的结构，判断是否存在满足指定条件的记录，并根据判断结果过滤候选查询结果。该策略不负责补入分区独有行，可用于检测特定存在性条件下的查询语义。 | MonetDB、PostgreSQL、SQLite、CockroachDB、ClickHouse | `SELECT x.row_number FROM X x WHERE NOT EXISTS (SELECT 1 FROM Y y WHERE y.row_number > x.row_number);` |
| 固定连接顺序 | `STRAIGHT_JOIN` | 按行标识 `vp_rowid` 关联左右分区，但按照 SQL 中指定的表顺序执行连接，而非由优化器自由重排连接顺序。 | MySQL、TiDB、MariaDB、OceanBase | `SELECT l.vp_rowid, l.a, r.b`<br>`FROM L l STRAIGHT_JOIN R r`<br>`  ON l.vp_rowid = r.vp_rowid;` |
| 外连接语义探测 | `OUTER_NULL_EXTENSION` | 使用 `FULL OUTER JOIN` 检测左右分区是否存在未匹配行，并将检测结果作为候选重建查询的门控条件。若存在未匹配行，则过滤掉全部候选结果；若不存在，则返回普通内连接重建结果。该策略用于探测外连接的 NULL 扩展语义，而不是通过全外连接补齐重建结果。 | CockroachDB | `SELECT l.vp_rowid, l.a, r.b`<br>`FROM L l JOIN R r ON l.vp_rowid = r.vp_rowid`<br>`CROSS JOIN (`<br>`  SELECT COALESCE(MAX(CASE`<br>`    WHEN lp.vp_rowid IS NULL OR rp.vp_rowid IS NULL THEN 1`<br>`    ELSE 0 END), 0) AS has_outer_null`<br>`  FROM L lp FULL OUTER JOIN R rp`<br>`    ON lp.vp_rowid = rp.vp_rowid`<br>`) p`<br>`WHERE p.has_outer_null = 0;` |
| 索引与连接执行路径探测 | `ZIGZAG_JOIN` | 要求左右分区各自至少有两个可索引列，否则跳过。为每侧选出的两列分别建立正反顺序的复合索引，然后按重建键连接，并要求两侧各自选中的一列非 `NULL`，以测试索引配置、过滤条件与连接查询的交互。 | CockroachDB | `-- 左分区：两种列顺序`<br>`CREATE INDEX ... ON L (l1, l2, vp_rowid);`<br>`CREATE INDEX ... ON L (l2, l1, vp_rowid);`<br>`-- 右分区：两种列顺序`<br>`CREATE INDEX ... ON R (r1, r2, vp_rowid);`<br>`CREATE INDEX ... ON R (r2, r1, vp_rowid);`<br>`-- VP 重建查询`<br>`SELECT l.vp_rowid, l.a, r.b`<br>`FROM L l JOIN R r ON l.vp_rowid = r.vp_rowid`<br>`WHERE l.l1 IS NOT NULL AND r.r1 IS NOT NULL;` |

## 实验结果

| DBMS | New Bug | Duplicate | 上报时Fixed | 总计 |
|---|---:|---:|---:|---:|
| TiDB | 18 | 0 | 0 | **18** |
| MySQL | 19 | 4 | 2 | **25** |
| MariaDB | 12 | 1 | 1 | **14** |
| Databend | 11 | 0 | 3 | **14** |
| OceanBase | 7 | 0 | 0 | **7** |
| CockroachDB | 5 | 0 | 0 | **5** |
| DuckDB | 3 | 0 | 2 | **5** |
| PostgreSQL | 1 | 0 | 0 | **1** |
| ClickHouse | 0 | 1 | 0 | **1** |
| **总计** | **77** | **6** | **8** | **91** |