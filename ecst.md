# ECST

现有等价查询测试方法通常通过构造语义等价的 SQL 查询并比较其执行结果来检测数据库逻辑错误。然而，语义等价的查询可能经过不同的优化规则和执行路径，使相同的逻辑操作在不同的执行上下文中被处理，包括表达式求值位置、谓词下推边界、关系算子组合及数据访问路径等。不同执行上下文之间的实现差异可能导致 SQL 语义不一致，从而产生难以检测的逻辑错误。现有基于等价查询变换的测试方法主要关注查询结果的一致性，缺乏对执行上下文迁移的显式建模与系统化探索，因而难以有针对性地触发和检测跨执行边界的逻辑错误。

基于这一观察，本文提出 ECST（Execution-Context Shift Testing），一种基于等价关系表示的执行上下文迁移测试方法。ECST 将等价关系改写视为一种可控的执行上下文探测手段，通过构造具有不同关系结构和算子依赖的语义等价查询，引导相同的逻辑操作进入不同的优化与执行环境。

具体而言，ECST 首先结合关系纵向分区、视图封装、物理复制等构造等价查询，并根据目标操作的列依赖、表达式算子和关系重构边界，选择能够诱导相应迁移的变换策略；随后，ECST 分析生成查询的执行计划，识别执行上下文中表达式求值位置、谓词下推边界、连接执行路径及数据访问方式的变化，并利用迁移覆盖信息反馈指导后续变换选择，从而系统探索不同优化与执行边界；最后，ECST 通过比较等价查询的结果检测语义不一致，并结合执行路径干预与查询最小化分析语义不一致的触发条件。

## 2 概念

### Definition 1：Equivalent Relational Representation（等价关系表示）

给定原始关系 \(R\)，通过语义保持的关系变换 \(T\)，将原始关系 \(R\) 转换为新的关系表示 \(R'\)。

如果对于所有满足约束 \(\Gamma\) 的数据库状态D、D'，以及任意允许的 SQL 查询语句 \(C \in \mathcal{C}\)，在 \(R\) 和 \(R'\) 执行的查询均产生相同的 SQL 语义结果，则称 \(R\) 与 \(R'\) 为**等价关系表示（Equivalent Relational Representations）**。

**形式化定义**

关系变换：

$$
R' = T(R)
$$

关系表示等价性：

$$
\llbracket C[R] \rrbracket_D
=
\llbracket C[R'] \rrbracket_{D'}

$$

\(D\) 和 \(D'\) 不要求是同一个物理数据库状态。例如，纵向分区和完整列物理复制会改变底层关系的存储形式，但必须保证相应逻辑数据之间存在满足约束的映射关系。

允许的查询上下文集合 \(\mathcal{C}\) 应排除无法保证语义保持的情形，例如依赖未定义行顺序的查询、不受约束的非确定性函数，以及可能观察到物理表身份差异的操作。


### Definition 2：Execution Context（执行上下文）

执行上下文关注特定表达式或关系算子在 DBMS 内部的具体执行行为，包括其执行位置、所属物理算子、所处优化边界、访问与执行路径以及求值环境。

对于查询 \(Q\) 中的目标逻辑操作 \(o\)，定义其执行上下文为：

$$
\boxed{
\kappa(o,Q,D,S)
=
\langle L,O,B,A,E\rangle
}
$$

**符号说明**

| 符号 | 名称 | 含义 |
|---|---|---|
| \(\kappa\) | Execution Context Function | 目标逻辑操作的执行上下文描述函数 |
| \(o\) | Target Logical Operation | 查询中的目标表达式或关系算子 |
| \(Q\) | SQL Query | 包含目标操作 \(o\) 的 SQL 查询 |
| \(D\) | Database State | 数据库状态，包括数据、Schema、索引和统计信息 |
| \(S\) | Session Configuration | 会话状态及优化器配置 |
| \(L\) | Execution Location | 目标操作的执行位置 |
| \(O\) | Operator Context | 目标操作所属的物理执行算子 |
| \(B\) | Optimization Boundary | 目标操作所处的优化变换边界 |
| \(A\) | Access/Execution Path | 目标操作关联的数据访问与物理执行路径 |
| \(E\) | Evaluation Environment | 目标操作的求值环境 |

- \(L\)例如：
  - Root：数据库计算层执行。
  - Coprocessor：存储执行层执行。
  - Remote Worker：分布式远端执行节点。
  - Local Executor：本地执行器。

- \(O\)例如：
  - Selection：过滤算子。
  - Projection：投影算子。
  - Join：连接算子。
  - Aggregation：聚合算子。
  - Sort：排序算子。

- \(B\)例如：
  - Predicate Pushdown：谓词下推边界。
  - Join Reordering：连接重排边界。
  - Subquery Decorrelation：子查询去关联边界。
  - Derived Table Merge：派生表合并边界。
  - Materialization：中间结果物化边界。

- \(A\)例如：
  - Table Scan：全表扫描。
  - Index Scan：索引扫描。
  - Index Lookup：索引回表。
  - Hash Join：哈希连接。
  - Nested Loop Join：嵌套循环连接。
  - Merge Join：归并连接。

- \(E\)例如：
  - SQLMode：SQL 模式。
  - Collation：字符串排序与比较规则。
  - Character Set：字符集。
  - Type Conversion：类型转换规则及其实现。
  - Expression Evaluation：表达式求值实现。

### Definition 3：Execution-Context Shift（执行上下文迁移）

给定两个语义等价的 SQL 查询 \(Q\) 和 \(Q'\)，以及二者之间具有逻辑语义对应关系的目标操作 \(o\) 和 \(o'\)。

如果两个目标操作在 DBMS 内部所处的执行上下文发生变化，则称该等价查询变换诱导了一次**执行上下文迁移（Execution-Context Shift）**。

**形式化定义**

设：

$$
Q \equiv Q'
$$

表示两个查询在规定的数据状态对应关系及 SQL 语义约束下等价。

设：

$$
o \simeq o'
$$

表示两个查询中的目标操作具有逻辑语义对应关系。

定义执行上下文迁移：

$$
\boxed{
\begin{aligned}
&\operatorname{Shift}(Q,Q',o,o')\\
&\iff
(Q\equiv Q')
\land
(o\simeq o')
\land
\left(
\kappa(o,Q,D,S)
\neq
\kappa(o',Q',D',S)
\right)
\end{aligned}
}
$$

**执行上下文差异**

设原始查询中目标操作的执行上下文为：

$$
\kappa(o,Q,D,S)=
\langle L,O,B,A,E\rangle
$$

变换查询中对应操作的执行上下文为：

$$
\kappa(o',Q',D',S)=
\langle L',O',B',A',E'\rangle
$$

定义执行上下文差异：

$$
\boxed{
\Delta\kappa
=
\operatorname{Diff}
\left(
\kappa(o,Q,D,S),
\kappa(o',Q',D',S)
\right)
}
$$

其中，\(\operatorname{Diff}\) 表示提取两个执行上下文之间发生变化的维度及其迁移方向。

对于能够进行逐维比较的上下文，可以进一步表示为：

$$
\boxed{
\Delta\kappa
=
\langle
\Delta L,
\Delta O,
\Delta B,
\Delta A,
\Delta E
\rangle
}
$$

### Definition 4：Context-Shift Target（目标执行上下文迁移）

目标执行上下文迁移（Context-Shift Target）是指测试过程中预先指定、希望通过语义等价的关系变换诱导的执行上下文变化类型。

#### 1. 形式化定义

设 ECST 预定义的目标执行上下文迁移集合为：

$$
\boxed{
\mathcal{S}=\{s_1,s_2,\ldots,s_m\}
}
$$

其中，每个目标迁移定义为：

$$
\boxed{
s_i=\langle d_i,v_i,v_i'\rangle
}
$$

具体而言，目标迁移 \(s_i\) 描述目标逻辑操作在执行上下文维度 \(d_i\) 上，从初始状态 \(v_i\) 向目标状态 \(v_i'\) 的预期变化。其中，目标维度 \(d_i\) 从 Definition 2 定义的执行上下文维度中选择

设原始查询中的目标操作为 \(o\)，变换查询中的对应操作为 \(o'\)，则期望：

$$
\boxed{
\operatorname{Match}
\left(
\Delta\kappa(Q,Q',o,o'),
s_i
\right)
=\mathrm{True}
}
$$

其中：

- \(\Delta\kappa(Q,Q',o,o')\)：两个对应逻辑操作之间实际观察到的执行上下文变化。
- \(s_i\)：预先指定的目标迁移。
- \(\operatorname{Match}(\cdot)\)：判断实际迁移是否包含目标迁移要求的维度、初始状态和目标状态。

# 3 方法：执行上下文迁移测试（ECST）

## 3.1 方法概述

本文提出 **Execution-Context Shift Testing（ECST）**，一种面向执行上下文迁移的数据库语义一致性测试方法。ECST 的基本思想是：在保持 SQL 语义等价的前提下，通过改变关系表示、属性依赖与关系重构方式，引导同一逻辑操作进入不同的优化和执行环境；随后依据执行计划与执行行为识别实际发生的迁移，并通过等价查询的结果一致性检查发现潜在逻辑错误。

前文已定义等价关系表示（Definition 1）、执行上下文（Definition 2）、执行上下文迁移（Definition 3）和目标执行上下文迁移（Definition 4）。与仅在生成查询之后比较结果的测试流程不同，ECST 在生成之前显式选择待探索的执行边界，并在生成之后检查该边界是否真正发生变化。实际迁移结果又反过来指导后续关系变换的选择，形成目标导向的闭环探索。

给定目标 DBMS、测试预算 $B$、目标迁移集合 $\mathcal S$ 与语义保持的变换规则集合 $\mathcal T$，ECST 在预算内持续构造等价查询对，输出候选语义错误集合 $\mathcal B$。其主要过程由六个模块组成：

1. **种子查询与目标操作生成（Seed Query and Target Operation Generation）**：生成测试数据库、种子查询及待追踪的逻辑操作。
2. **目标迁移选择（Context-Shift Target Selection）**：选择当前适用且尚未充分探索的执行上下文迁移。
3. **目标导向的关系变换（Context-Shift-Oriented Relational Transformation）**：联合选择关系表示、重构策略和表达式上下文，生成语义等价查询。
4. **执行上下文分析（Execution-Context Analysis）**：追踪对应操作，提取并比较实际执行上下文。
5. **反馈引导探索（Feedback-Guided Context Exploration）**：记录迁移成功率和覆盖情况，更新后续选择策略。
6. **语义一致性检查（Semantic Consistency Checking）**：比较查询结果，对不一致进行最小化和受控验证。

上述模块的关键关系可概括为：

$$
\underbrace{s}_{\text{目标迁移}}
\longrightarrow
\underbrace{\theta=(\pi,\rho,\phi)}_{\text{变换配置}}
\longrightarrow
\underbrace{(Q',D')}_{\text{等价测试实例}}
\longrightarrow
\underbrace{\Delta\kappa}_{\text{实际迁移}}
\longrightarrow
\underbrace{H}_{\text{反馈更新}}.
$$

其中，$\pi$ 为关系表示配置，$\rho$ 为关系重构策略，$\phi$ 为表达式与算子配置。需要区分的是，**$\kappa$ 表示实际观测到的执行上下文，并非生成器可以直接设置的参数**。ECST 控制变换输入，通过 DBMS 的优化和执行行为间接探索 $\kappa$。

## 3.2 种子查询与目标操作生成

ECST 首先构造数据库状态 $D$、种子查询 $Q$ 和目标逻辑操作 $o$：

$$
(D,Q,o)
$$

测试数据库包含具有不同数据类型、NULL 值、重复值、边界值和索引配置的基础关系。查询生成器在满足目标 DBMS 语法与类型约束的条件下，组合过滤、投影、连接、聚合、子查询等 SQL 结构，并从生成的查询中选取一个或多个可追踪的目标操作。目标操作既可以是 `CAST`、`CASE`、比较表达式、日期与字符串表达式等标量表达式，也可以是过滤、连接或聚合等关系操作。

对于目标操作 $o$，生成器保存其规范化表达式、类型信息、所在 SQL 子句及属性依赖集合：

$$
\operatorname{Dep}(o)=\{a_1,a_2,\ldots,a_k\}.
$$

这些信息用于后续两个步骤：首先判断某种迁移是否适用于当前目标；其次在关系变换之后建立 $o$ 与对应操作 $o'$ 的语义映射。

## 3.3 目标驱动的等价关系变换

ECST 不对所有关系表示与重构策略进行无约束枚举，而是首先选择目标迁移，再根据其适用条件合成能够诱导该迁移的候选查询。

### 3.3.1 变换配置空间

定义参数化变换：

$$
\boxed{\mathcal T_{\theta}(Q,D)=(Q',D'),\qquad\theta=\langle\pi,\rho,\phi\rangle.}
$$

其中，$D'$ 可以包含为测试建立的纵向分区、视图或物理副本。变换必须满足前文 Definition 1 的状态映射与 SQL 语义约束：

$$
\boxed{\llbracket Q\rrbracket_D=\llbracket Q'\rrbracket_{D'}.}
$$

三类参数各自承担不同的控制作用。

| 参数 | 名称 | 控制内容 | 对执行环境的潜在影响 |
|---|---|---|---|
| $\pi$ | Representation Configuration | 关系表示类型、纵向列位置、复制列、行标识、物理副本及索引布局 | 改变目标操作的输入可用性与物理访问条件 |
| $\rho$ | Reconstruction Strategy | JOIN、OUTER JOIN、VIEW、CTE、派生表、标量子查询及合法的反连接重构 | 引入或改变优化与算子边界 |
| $\phi$ | Expression/Operator Configuration | 表达式类型、依赖列、WHERE/SELECT/HAVING/ON 等上下文 | 影响目标操作的可下推性和算子归属 |

**ECST 使用四种基本关系表示：**

$$
\boxed{
\mathcal{R} = \{R_0, R_1, R_2, R_3\}
}
$$

| 符号 | 关系表示 | 定义 | 可能影响的优化与执行行为 |
|---|---|---|---|
| \(R_0\) | Original Relation | 直接使用原始数据表 | 提供原始执行基线 |
| \(R_1\) | Vertically Partitioned Relation | 将原始关系按列拆分，并通过关系运算重构 | JOIN 边界、表达式求值位置、谓词下推 |
| \(R_2\) | View-Based Relation | 通过完整源表视图封装原始关系 | 视图合并、谓词下推、算子位置 |
| \(R_3\) | Replicated Physical Relation | 将原始关系完整复制到多张新的物理表 | 访问路径、索引选择、统计信息相关优化 |

特别说明，$R_2$ 与采用 `VIEW` 的 $R_1$ 重构并不相同：前者包装完整源表，后者包装分区重构关系。

在满足相应语义保持约束的前提下，四种关系表示满足：

$$
\boxed{
R_0 \equiv_{\Gamma,\mathcal{C}} R_1
\equiv_{\Gamma,\mathcal{C}} R_2
\equiv_{\Gamma,\mathcal{C}} R_3
}
$$

虽然不同关系表示具有相同的预期 SQL 语义，但其关系结构、属性分布和算子依赖可能不同，从而影响 DBMS 的查询优化决策。

因此，关系表示之间的语义等价性并不保证其物理执行计划相同：

$$
\boxed{
R_i \equiv_{\Gamma,\mathcal{C}} R_j
\;\not\Rightarrow\;
P(C[R_i]) = P(C[R_j])
}
$$

其中，\(P(Q)\) 表示 DBMS 为查询 \(Q\) 生成的物理执行计划。

设 $\Pi,\mathcal P_{\rho},\Phi$ 分别为三类配置空间，则候选组合为 $\Pi\times\mathcal P_{\rho}\times\Phi$。然而，并非所有组合都满足语义等价与目标适用性。对于给定种子查询和目标迁移 $s$，仅保留：

$$
\boxed{
\Theta_s(Q,D)=\left\{
\theta\in\Pi\times\mathcal P_{\rho}\times\Phi
\;\middle|\;
\operatorname{Legal}(\theta,Q,D,\Gamma)
\land
\operatorname{Applicable}(s,\theta,Q,o)
\right\}.
}
$$

`Legal` 表示变换满足类型、行映射、NULL、重复行、外连接、排序与确定性等语义约束；`Applicable` 表示变换的结构具有诱导指定迁移的条件。

### 3.3.2 目标迁移规则库

ECST 为各类执行上下文迁移建立规则：

$$
\boxed{G_s=\langle\operatorname{Pre}_s,\Pi_s,\mathcal P_s,\Phi_s,\operatorname{Verify}_s\rangle.}
$$

其中，$\operatorname{Pre}_s$ 给出 DBMS 与操作的适用条件，$\Pi_s,\mathcal P_s,\Phi_s$ 限定候选变换，$\operatorname{Verify}_s$ 则描述执行计划中确认迁移的条件。表 1 汇总主要规则。

**表 1：目标迁移与候选关系变换的对应规则**

| 目标迁移 | 适用前提 | $\pi$：表示与列配置 | $\rho$：重构策略 | $\phi$：表达式及上下文 | 计划验收条件 |
|---|---|---|---|---|---|
| Root $\leftrightarrow$ Cop | DBMS 具有多层表达式执行能力 | $R_0/R_1/R_2$；单侧或跨侧列依赖 | VIEW、DERIVED TABLE、JOIN、SCALAR SUBQUERY | CAST、CASE、日期或字符串表达式；过滤/投影 | 对应表达式实际由 Root 与 Cop 两种位置执行 |
| Pre-Join $\leftrightarrow$ Post-Join | 目标表达式具有可调整的属性依赖 | $R_1$；同侧与跨侧列分配 | INNER JOIN；受语义约束的 OUTER JOIN | 涉及多个属性的谓词或表达式 | 表达式在连接输入侧与输出侧的物理位置不同 |
| Pushdown $\leftrightarrow$ Non-Pushdown | 存在可识别的下推候选 | $R_1/R_2$；单侧/跨侧依赖与封装 | JOIN、VIEW、CTE、DERIVED TABLE | WHERE、ON、派生谓词 | 谓词跨越目标下推边界 |
| Merge $\leftrightarrow$ Materialization | 存在可合并或物化的关系输入 | $R_2$ 或经过封装的 $R_1$ | VIEW、CTE、DERIVED TABLE | Projection、WHERE、Aggregation | 计划中确认合并与物化两类执行路径 |
| Table Scan $\leftrightarrow$ Index Scan | 可建立可用索引 | $R_0/R_3$；控制副本、索引布局 | 直接关系访问、等价子查询 | 比较、范围及可索引表达式 | 访问路径实际发生切换 |
| Join Algorithm Shift | DBMS 支持多种连接算法 | $R_1$；连接键、索引及关系结构 | JOIN、连接顺序变换 | 等值或适用的非等值连接条件 | 物理 JOIN 算法不同 |
| Selection $\leftrightarrow$ Projection | 表达式能够合法构造为过滤或派生计算 | $R_1/R_2$ | VIEW、DERIVED TABLE、SCALAR SUBQUERY | 可安全改写的表达式与过滤条件 | 对应表达式归属的物理算子变化 |

规则库不是对 DBMS 物理计划的静态预测。它描述哪些变换**可能影响目标边界**，实际是否发生迁移由 §3.4 确认。对于 TiDB 等系统，Root/Coprocessor 是具体迁移目标；对于不具有这种架构的 DBMS，则使用其适用的本地/远端执行位置或其他边界，避免将某一产品的执行层次强加给所有系统。

实现时必须区分抽象配置空间与 DBMS 能力边界。TiDB 当前代码明确实现的是 **TiDB ECST prototype capability set**：纵向分区（`VERTICAL_PARTITIONED`）、`JOIN`/`DERIVED_TABLE` 重构，以及 `WHERE`/`SELECT`/`HAVING` 表达式上下文。其余配置仍会在抽象空间中枚举，并由语义适用性和能力检查过滤；它们不应被描述为 TiDB 已经支持的完整 $(\pi,\rho,\phi)$ 空间。候选集合不会按固定上限截断，完整可用集合交给反馈历史选择，确保每个可用配置都有进入历史的机会。

### 3.3.3 基于属性依赖的连接边界诱导

纵向分区能够直接控制目标操作的输入属性位于何处，是 ECST 具有辨识度的核心变换机制。

设目标表达式 $o$ 的依赖集合为：

$$
\operatorname{Dep}(o)=\{a,b\}.
$$

对于以唯一行标识对齐的左右分区，ECST 构造两种布局：

$$
\pi_{\mathrm{same}}:\quad
\operatorname{loc}(a)=L,\qquad \operatorname{loc}(b)=L;
$$

$$
\pi_{\mathrm{cross}}:\quad
\operatorname{loc}(a)=L,\qquad \operatorname{loc}(b)=R.
$$

若全部依赖列位于左侧，则完整表达式具有在左输入上提前求值的条件；若其依赖列分布在两侧，则在未进行进一步代数分解的情况下，完整表达式通常需要读取两侧属性后才能求值。两种布局因而改变了优化器可用的合法算子位置。

例如，原始查询为：

```sql
SELECT id
FROM src
WHERE a + b > 10;
```

同侧布局中，左分区持有 `id, a, b`，右分区至少持有对应的 `id`：

```sql
SELECT l.id
FROM part_l AS l
JOIN part_r AS r ON l.id = r.id
WHERE l.a + l.b > 10;
```

跨侧布局中，左分区持有 `id, a`，右分区持有 `id, b`：

```sql
SELECT l.id
FROM part_l AS l
JOIN part_r AS r ON l.id = r.id
WHERE l.a + r.b > 10;
```

在两种数据布局均通过唯一行标识与原表形成一一对应、且保留相关类型和 NULL 语义的条件下，上述查询与原始查询等价。其潜在区别在于表达式的输入依赖结构。需要强调：**同侧布局提供提前求值的机会，并不保证优化器一定提前求值**；跨侧布局也可能被进一步改写。ECST 只有在实际计划显示目标操作跨越连接边界时，才记录迁移成功。

### 3.3.4 基于关系封装的下推与物化边界诱导

对于 Root/Cop、Pushdown 和 Merge/Materialization 等目标，ECST 优先改变关系封装与谓词/投影的相对位置。考虑：

```sql
SELECT id
FROM src
WHERE CAST(a AS SIGNED) > 0;
```

以及在表达式确定且满足相应 SQL 类型语义时的等价派生表形式：

```sql
SELECT id
FROM (
    SELECT id, CAST(a AS SIGNED) AS v
    FROM src
) AS t
WHERE v > 0;
```

第二种形式引入额外关系边界与派生列，可能使表达式的投影、过滤及下推位置变化。优化器也可能将派生表完全合并，导致没有任何目标迁移。类似地，VIEW、CTE 与派生表的合并/物化由 DBMS 的具体规则决定，不能仅以 SQL 语法结构判断。

在允许的情况下，ECST 还可以将索引、统计信息或语义保持的优化器提示作为**辅助执行约束**，提高特定物理路径的可达性。这些控制因素单独记录，不并入 $\kappa$：$\kappa$ 始终表示实际执行上下文。采用提示产生的迁移也应与纯关系变换产生的迁移分开归因。

### 3.3.5 成对变换与语义约束

为减小不相关因素的影响，ECST 优先构造围绕同一目标的**对照配置**：

$$
(\theta^{-},\theta^{+}),
$$

例如同侧/跨侧列分布、直接关系/视图封装、可合并/可能物化的关系表达式。两种配置应尽量仅改变与目标迁移有关的一个控制因素；每种配置均须独立满足语义保持约束。对于无法只改变一个因素的场景，完整记录伴随发生的结构变化，并在后续分析中避免将全部计划差异归因于单一因素。

语义有效性由规则的前置条件约束，包括：唯一行标识与完整行重建、正确的多重集语义、NULL 与外连接空扩展、函数确定性、字符集与排序规则、类型转换、聚合与排序要求等。尤其不能任意将外连接 `WHERE` 谓词移入 `ON` 子句，或将带有未定义行顺序的 `LIMIT` 查询认定为普遍等价。

## 3.4 执行上下文识别与迁移验证

ECST 分别获取原始查询与变换查询的物理计划：

$$
P=P(Q,D,S),\qquad P'=P(Q',D',S).
$$

由于目标表达式可能被复制、化简、上提或下推，ECST 不直接使用 SQL 字符串匹配物理节点，而是综合 AST 来源标识、规范化表达式、属性血缘与物理计划中的表达式信息，建立逻辑操作对应关系 $o\simeq o'$。对于一个逻辑操作对应多个物理执行实例的情形，记录其实例集合及各自上下文，避免强行映射为单一节点。

随后提取前文 Definition 2 中的五维上下文：

$$
\kappa(o,Q,D,S)=\langle L,O,B,A,E\rangle,
$$

$$
\kappa(o',Q',D',S)=\langle L',O',B',A',E'\rangle.
$$

计算实际迁移：

$$
\boxed{\Delta\kappa=\operatorname{Diff}(\kappa,\kappa').}
$$

对于目标 $s=\langle d,v,v'\rangle$，通过比较对应维度是否由 $v$ 变为 $v'$ 判断迁移是否成功：

$$
\boxed{\operatorname{Match}(\Delta\kappa,s)=\mathrm{True}.}
$$

系统区分三种观察状态：**Success**（确认发生目标迁移）、**Failure**（确认目标迁移未发生）以及 **Unknown**（计划信息不足或操作映射不可靠）。不能将未出现在 `EXPLAIN` 中的求值环境差异直接判定为“不变”；对于 SQLMode、Collation 与表达式实现等维度，必要时结合执行 Trace、会话状态和受控干预补充证据。一次查询变换还可能同时触发多个迁移维度，ECST 会记录完整的已确认差异，而不仅是当前目标。

## 3.5 反馈引导的上下文探索（Feedback-Guided Context Exploration）

尽管 ECST 可以通过目标驱动的关系变换构造具有不同关系结构和算子依赖的等价查询，但这些变换能否成功诱导目标执行上下文迁移，仍取决于 DBMS 的具体优化规则、执行架构、数据分布及查询特征。因此，预先定义的变换规则只能提供潜在的迁移诱导能力，不能保证目标迁移一定发生。如果仅随机选择关系表示和重构策略，测试过程可能反复生成相同的执行路径，难以有效探索尚未覆盖的优化与执行边界。

为此，ECST 设计了一种**反馈引导的执行上下文探索机制（Feedback-Guided Context Exploration）**。该机制通过分析每次等价查询变换实际诱导的执行上下文变化，持续学习不同变换配置与目标迁移之间的关联，并利用这些信息指导后续测试生成。

具体而言，ECST 采用两级反馈选择机制：

1. **Target Selection**：根据执行上下文迁移的覆盖情况与历史可达性，选择下一轮测试目标。
2. **Transformation Selection**：根据候选变换对目标迁移的历史成功率及探索程度，选择具体的关系表示、重构策略和表达式配置。

通过这一机制，ECST 在利用已知有效变换与探索尚未覆盖的执行边界之间取得平衡。

### 3.5.1 执行上下文反馈信息

对于每次测试，ECST 不仅记录等价查询的执行结果，还记录目标迁移、变换配置及实际观察到的执行上下文变化。

设第 \(t\) 次测试选择目标迁移 \(s_t\)，并采用变换配置：

$$
\theta_t=\langle\pi_t,\rho_t,\phi_t\rangle
$$

生成等价查询：

$$
(Q_t',D_t')=\mathcal{T}_{\theta_t}(Q_t,D_t)
$$

通过执行计划分析，获得实际执行上下文变化：

$$
\Delta\kappa_t
=
\operatorname{Diff}
\left(
\kappa(o_t,Q_t,D_t,S),
\kappa(o_t',Q_t',D_t',S)
\right)
$$

根据 Definition 4 中的目标迁移匹配规则，形式化地定义目标迁移的验证结果：

$$
z_t=
\begin{cases}
\mathrm{Success},
&
\operatorname{Match}(\Delta\kappa_t,s_t)=\mathrm{True}
\\[4pt]
\mathrm{Failure},
&
\operatorname{Match}(\Delta\kappa_t,s_t)=\mathrm{False}
\\[4pt]
\mathrm{Unknown},
&
\operatorname{Match}(\Delta\kappa_t,s_t)
\text{ 无法确定}
\end{cases}
$$

需要注意，Failure 表示未发生**预期目标迁移**，并不意味着执行上下文完全没有变化。

例如，ECST 试图诱导 Root → Coprocessor 迁移，但实际执行计划显示 Table Scan → Index Scan。此时目标迁移判定为 Failure，但观察到的 Access Path Shift 仍应被记录。

因此，ECST 同时维护：

- **Target-Shift Feedback**：目标迁移是否成功。
- **Observed-Shift Feedback**：实际观察到了哪些执行上下文迁移。

这使算法能够利用非预期迁移扩展执行上下文覆盖，而不是只保留目标迁移成功的查询。

ECST 将本次测试的反馈记录定义为：

$$
\boxed{
h_t=
\langle
s_t,\theta_t,f_t,\Delta\kappa_t,z_t,c_t
\rangle
}
$$

其中，查询特征 \(f_t\) 包括目标表达式类型、属性依赖、SQL 子句、关系结构、索引条件及目标 DBMS 等信息；\(c_t\) 记录本次变换、计划分析及执行所消耗的测试成本



### 3.5.2 变换—迁移关联统计

为了学习不同变换配置的迁移诱导能力，ECST 维护三个计数器：


| 符号 | 含义 |
|---|---|
| \(N_{\theta,s}^{+}\) | 使用变换 \(\theta\) 成功触发目标迁移 $s$ 的次数 |
| \(N_{\theta,s}^{-}\) | 使用变换 \(\theta\) 未触发目标迁移 $s$ 的次数 |
| \(N_{\theta,s}^{?}\) | 使用变换 \(\theta\) 无法判断目标迁移 $s$ 是否发生的次数 |



对于已有观察的组合，定义在当前测试分布与 DBMS 环境下，采用变换配置 \(\theta\) 时，目标迁移 \(s\)的经验迁移成功率：

$$
\boxed{
\widetilde{p}(s\mid\theta)
=
\frac{
N_{\theta,s}^{+}+a
}{
N_{\theta,s}^{+}
+
N_{\theta,s}^{-}
+
a+b
}
}
$$

其中，\(a,b>0\) 为平滑参数。

例如，当 \(a=b=1\) 时，尚未尝试的配置具有中性的初始估计，而不会因为缺少历史数据被错误地认为迁移成功率为零。

需要注意，变换诱导能力与 DBMS、表达式类别、属性分布及查询结构有关。因此，ECST 按 DBMS 和相关查询特征维护统计，并在某个具体组合缺少足够样本时，参考同一变换类别的历史信息。


### 3.5.3 第一级反馈：目标执行上下文迁移选择

ECST 的第一级反馈用于决定下一轮优先探索哪一种执行上下文迁移。

目标迁移可达性定义为：

$$
\boxed{
\operatorname{Reachability}(s)
=
\max_{\theta\in\Theta_s}
\widetilde{p}(s\mid\theta)
}
$$

其中，\(\Theta_s\) 为当前目标迁移对应的有效候选变换集合。



目标迁移的覆盖不足度定义为：

$$
\boxed{
\operatorname{Novelty}(s)
=
\frac{1}{
\sqrt{1+N_s^{\mathrm{cov}}}
}
}
$$

目标迁移的探索不足度定义为：

\[
\boxed{
\operatorname{Explore}(s)
=
\sqrt{
\frac{2\ln(2+N_{\mathrm{total}})}
{1+N_s^{\mathrm{trial}}}
}
}
\]

其中：

- \(N_{\mathrm{total}}\) 为当前 DBMS 下累计的目标迁移尝试次数。
- \(N_s^{\mathrm{trial}}\) 为目标迁移 \(s\) 被选择并实际尝试的次数。
- \(N_s^{\mathrm{cov}}\) 为目标迁移 \(s\) 被成功观察到的累计次数，包括非预期迁移。

目标迁移选择评分定义为：

$$
\boxed{
\operatorname{TargetScore}(s)
=
\lambda_1\operatorname{Novelty}(s)
+
\lambda_2\operatorname{Reachability}(s)
+
\lambda_3\operatorname{Explore}(s)
}
$$



目标选择为：

$$
\boxed{
s^*
=
\arg\max_{s\in\mathcal{S}_{\mathrm{valid}}}
\operatorname{TargetScore}(s)
}
$$

其中，\(\mathcal{S}_{\mathrm{valid}}\) 是根据当前 DBMS、种子查询及目标逻辑操作过滤后的适用迁移集合。

因此，第一级选择同时兼顾三个目标：

1. 优先探索尚未覆盖的执行边界。
2. 利用已有变换经验提高目标迁移的成功概率。
3. 为尚未充分测试的迁移保留探索机会。

### 3.5.4 第二级反馈：目标迁移导向的变换配置选择

选定目标迁移 \(s^*\) 后，ECST 根据 §3.3 的变换规则库生成满足语义约束的候选配置：

$$
\Theta_{s^*}
=
\left\{
\theta
\mid
\operatorname{Legal}(\theta)
\land
\operatorname{Applicable}(s^*,\theta)
\right\}
$$

每个候选配置包含：

$$
\theta=\langle\pi,\rho,\phi\rangle
$$

第二级反馈的目标是在候选集合中选择既具有较高迁移成功概率、又能够提供新探索机会的变换配置。

首先，对于候选配置 \(\theta\)，定义探索奖励：

$$
\boxed{
U(\theta,s)
=
\sqrt{
\frac{
2\ln(2+N_s^{\mathrm{trial}})
}{
1+N_{\theta,s}^{\mathrm{+}}+N_{\theta,s}^{\mathrm{-}}
}
}
}
$$

该奖励使历史测试次数较少的配置获得更多探索机会，避免算法始终选择少数早期表现较好的变换。

其次，定义变换成本：

$$
\operatorname{Cost}(\theta)
$$

该成本可以根据变换生成时间、数据库准备成本、执行计划获取时间以及查询执行时间进行估计，并进行归一化。

最终，变换选择评分定义为：

$$
\boxed{
\operatorname{TransformScore}(\theta,s)
=
\alpha\widetilde{p}(s\mid\theta)
+
\beta U(\theta,s)
-
\gamma\operatorname{Cost}(\theta)
}
$$

对于目标迁移 \(s^*\)，ECST 选择变换：

$$
\boxed{
\theta^*
=
\arg\max_{\theta\in\Theta_{s^*}}
\operatorname{TransformScore}(\theta,s^*)
}
$$

随后应用：

$$
(Q',D')
=
\mathcal{T}_{\theta^*}(Q,D)
$$

生成新的等价测试查询。

这一过程实现了从目标迁移到具体关系变换的反馈引导选择。

**需要强调，目标选择与变换选择是两个不同的决策过程。**

- 第一级决定“下一轮测试哪一种执行边界”。
- 第二级决定“采用什么关系表示和重构策略来触发该边界”。

### 3.5.5 反馈更新过程

每完成一次测试，ECST 根据实际迁移结果更新历史统计。

假设当前选择：

$$
s_t=
\langle
B,\mathrm{PreJoin},\mathrm{PostJoin}
\rangle
$$

对应变换配置：

$$
\theta_t=
\langle
\pi_{\mathrm{cross}},
\rho_{\mathrm{join}},
\phi_{\mathrm{predicate}}
\rangle
$$

即通过跨侧属性分布与 JOIN 重构，尝试诱导目标谓词由 JOIN 前迁移至 JOIN 后。

执行计划分析完成后，可能出现以下情况。

**情况一：目标迁移成功**

若实际计划确认目标表达式位于预期的 JOIN 后执行位置，则：

$$
N_{\theta_t,s_t}^{+}
\leftarrow
N_{\theta_t,s_t}^{+}+1
$$

同时：

$$
N_{s_t}^{\mathrm{cov}}
\leftarrow
N_{s_t}^{\mathrm{cov}}+1
$$

并重新计算该变换配置的目标迁移成功率。

**情况二：目标迁移未发生**

若执行计划确认目标表达式仍处于原来的执行边界，则：

$$
N_{\theta_t,s_t}^{-}
\leftarrow
N_{\theta_t,s_t}^{-}+1
$$

这将降低该变换在对应目标上的历史成功率估计。

但算法不会立即禁用该配置，因为不同查询特征、数据分布和优化器状态可能影响其执行行为。

**情况三：无法判断目标迁移**

如果目标操作无法可靠映射，或者执行计划缺少必要信息，则：

$$
N_{\theta_t,s_t}^{?}
\leftarrow
N_{\theta_t,s_t}^{?}+1
$$

该结果不计入成功率的分子或分母，避免将分析能力不足误判为变换无效。

**情况四：观察到非目标迁移**

例如，当前目标是 Join Boundary Shift，但实际发生了 Index Scan → Table Scan。

此时，目标迁移按照实际证据记录为 Failure 或 Unknown，同时将确认发生的 Access Path Shift 加入执行上下文覆盖。

因此，单次测试可以更新多个实际迁移类型的覆盖统计，但不会将非目标迁移错误计入当前目标的成功次数。

### 3.5.6 反馈引导示例

考虑目标迁移：

$$
s=
\langle
B,\mathrm{NonPushdown},\mathrm{Pushdown}
\rangle
$$

ECST 根据规则库生成以下候选配置：

| 配置 | 关系表示 | 重构策略 | 表达式上下文 |
|---|---|---|---|
| \(\theta_1\) | R1：纵向分区 | INNER JOIN | 单侧 WHERE 谓词 |
| \(\theta_2\) | R1：纵向分区 | INNER JOIN | 跨侧 WHERE 谓词 |
| \(\theta_3\) | R2：完整源表视图 | VIEW | 外层 WHERE 谓词 |
| \(\theta_4\) | R2：完整源表视图 | DERIVED TABLE | 派生列过滤 |

假设经过若干轮测试，得到以下历史统计（仅用于说明算法机制）：

| 配置 | Success | Failure | Unknown | 平滑成功率 |
|---|---:|---:|---:|---:|
| \(\theta_1\) | 8 | 2 | 0 | 9/12 |
| \(\theta_2\) | 1 | 7 | 1 | 2/10 |
| \(\theta_3\) | 5 | 3 | 0 | 6/10 |
| \(\theta_4\) | 0 | 1 | 2 | 1/3 |

这里使用 \(a=b=1\) 的平滑估计。

从历史数据看，\(\theta_1\) 具有较高的目标迁移成功率，而 \(\theta_4\) 的可判定测试次数较少。

因此：

- 当算法需要较高概率触发目标迁移时，可能优先选择 \(\theta_1\)。
- 当算法需要探索不确定的候选配置时，\(\theta_4\) 可以获得更高的探索奖励。
- 如果当前 Pushdown 迁移已经被充分覆盖，第一级反馈可能选择其他尚未覆盖的迁移类型。

随着测试持续进行，ECST 不断更新变换—迁移关联统计，从而逐渐学习哪些配置更适合探索特定的执行上下文边界。

## 3.6 语义一致性 Oracle 与错误验证

执行上下文反馈用于优化测试生成过程，但不直接决定查询是否存在语义错误。

ECST 将两个目标分离：

**执行上下文探索目标：**

$$
\max\operatorname{ShiftCoverage}
$$

**语义错误检测目标：**

$$
\operatorname{Result}(Q,D,S)
\overset{?}{=}
\operatorname{Result}(Q',D',S)
$$

即使某次变换未能触发预期执行上下文迁移，只要查询对满足语义保持约束，ECST 仍然执行结果一致性检查，并把迁移状态作为独立元数据记录。

同样，执行上下文迁移成功也不意味着存在逻辑错误。只有结果出现不一致时，测试 Oracle 才报告潜在 Bug，并通过后续验证分析其与执行上下文迁移之间的关系。

因此，反馈机制主要以**执行上下文迁移成功率与迁移覆盖率**为生成指导，而不是直接利用 Bug 数量调整变换概率。这种设计避免了由于 Bug 信号稀疏或重复缺陷过多，导致生成器过度集中于少数已知错误触发路径。

对候选错误，ECST 首先在相同数据与会话条件下重复执行，随后尝试减少表、列、行、表达式和多余 SQL 结构，同时保留最初的结果不一致。对于能够观察到明确迁移的案例，进一步通过禁止/允许特定下推、切换物理算子或调整优化配置进行受控 A/B/A 干预，判断结果不一致是否随目标执行边界的切换可逆变化。该过程将“观察到迁移”和“证明迁移与错误有关”区分开来；仅因两条计划不同而结果不一致，并不足以断言具体根因。

每条报告保存原始与变换 SQL、数据库状态生成方式、$\pi/\rho/\phi$ 配置、目标 $s$、实测 $\Delta\kappa$、计划、结果差异以及最小化和干预记录，以支持复现与后续根因分析。

## 3.7 ECST 主算法

三个算法采用统一调用关系：**Algorithm 1 → Algorithm 3 → Algorithm 2**。Algorithm 1 执行主测试循环和语义一致性 Oracle；Algorithm 2 负责合成满足约束的候选变换；Algorithm 3 负责目标选择、变换选择、迁移分析和反馈更新。

**Algorithm 1：Execution-Context Shift Testing（ECST 主流程）**

```sql
Input:
    DBMS: target database system
    S_D: context-shift targets supported by DBMS
    G: target-driven transformation rule library
    B: testing budget
Output:
    Bugs: candidate semantic inconsistencies

 1  Initialize history H, coverage set C_shift, counts N_cov
 2  Bugs ← ∅
 3  while budget B remains do
 4      (D, Q, o) ← GenerateSeed(DBMS)
 5      Γ ← SemanticConstraints(DBMS, D, Q, o)
 6      S_valid ← ApplicableTargets(S_D, D, Q, o)
 7      if S_valid = ∅ then continue
 8      result ← FeedbackGuidedExplore(
                    S_valid, G, Q, D, o, Γ, H, C_shift, N_cov)  // Algorithm 3
 9      if result = ⊥ then continue
10      (Q', D', s*, θ*, Δκ, z, H, C_shift, N_cov) ← result
        // Oracle runs regardless of target-shift success.
11      Y ← Execute(Q, D)
12      Y' ← Execute(Q', D')
13      UpdateExecutionCost(H, θ*, Q, D, Y, Y')
14      if not SemanticallyEqual(Y, Y') then
15          bug ← ValidateAndMinimize(Q, Q', D, D', s*, θ*, Δκ, z)
16          Bugs ← Bugs ∪ {bug}
17  return Bugs
```

**Algorithm 2：Target-Driven Transformation Synthesis（候选变换合成）**

```sql
Input:
    G: transformation rule library
    s: target context shift
    Q, D, o: seed query, database state and target operation
    Γ: semantic-preservation and integrity constraints
Output:
    Θ_s: valid candidate configurations

 1  Θ_s ← ∅
 2  Rules ← SelectApplicableRules(G, s, Q, D, o)
 3  for each rule g in Rules do
 4      Π_g ← GenerateRepresentationConfigurations(g, D, o)
 5      for each π in Π_g do
 6          P_g ← ApplicableReconstructionStrategies(g, π)
 7          for each ρ in P_g do
 8              Φ_g ← ApplicableExpressionConfigurations(g, π, ρ, o)
 9              for each φ in Φ_g do
10                  θ ← ⟨π, ρ, φ⟩
11                  if Legal(θ, Q, D, Γ) and Applicable(s, θ, Q, o) then
12                      Θ_s ← Θ_s ∪ {θ}
13  return Θ_s
```

**Algorithm 3：Feedback-Guided Context Exploration（反馈引导探索）**

```sql
Input:
    S_valid: targets applicable to (D, Q, o)
    G: transformation rule library
    Q, D, o: seed query, database state and target operation
    Γ: semantic-preservation and integrity constraints
    H: history including N^+, N^-, N^?, N_trial and N_total
    C_shift, N_cov: confirmed shift set and coverage counts
Output:
    (Q', D', s*, θ*, Δκ, z, H, C_shift, N_cov), or ⊥

    // 1. Generate current candidate sets before computing reachability.
 1  CandidateTargets ← ∅
 2  for each s in S_valid do
 3      Θ_s ← SynthesizeValidConfigurations(G, s, Q, D, o, Γ)
                // Algorithm 2
 4      if Θ_s ≠ ∅ then CandidateTargets ← CandidateTargets ∪ {s}
 5  if CandidateTargets = ∅ then return ⊥

    // 2. Select the target according to Section 3.5.3.
 6  for each s in CandidateTargets do
 7      novelty ← 1 / sqrt(1 + N_s^cov)
 8      reachability ← max_{θ∈Θ_s} p_tilde(s | θ; H)
 9      exploration ← sqrt(2*ln(2+N_total)/(1+N_s^trial))
10      TargetScore[s] ← λ1*novelty + λ2*reachability
                            + λ3*exploration
11  s* ← argmax_{s∈CandidateTargets} TargetScore[s]

    // 3. Select transformation according to Section 3.5.4.
12  for each θ in Θ_s* do
13      success_rate ← p_tilde(s* | θ; H)
14      bonus ← sqrt(2*ln(2+N_s*^trial)/
                       (1+N_{θ,s*}^+ + N_{θ,s*}^-))
15      cost ← EstimateCost(θ, H)
16      TransformScore[θ] ← α*success_rate + β*bonus - γ*cost
17  θ* ← argmax_{θ∈Θ_s*} TransformScore[θ]

    // 4. Apply transformation and verify concrete constraints.
18  (Q', D') ← ApplyTransformation(Q, D, θ*)
19  if not CheckSemanticPreconditions(Q, D, Q', D', θ*) then
20      return ⊥

    // 5. Analyze actual execution-context shifts.
21  P ← Explain(Q, D)
22  P' ← Explain(Q', D')
23  mapping ← MatchLogicalOperations(o, Q, Q', P, P')
24  Δκ ← AnalyzeContextShift(mapping, P, P')
25  z ← MatchTarget(Δκ, s*)   // Success, Failure or Unknown

    // 6. Update intended-target feedback and all observed coverage.
26  N_s*^trial ← N_s*^trial + 1
27  N_total ← N_total + 1
28  UpdateTargetHistory(H, θ*, s*, z)
        // Increment exactly one of N_{θ*,s*}^+, N_{θ*,s*}^-, N_{θ*,s*}^?.
29  Observed ← ConfirmedShifts(Δκ)
30  for each s in Observed do
31      N_s^cov ← N_s^cov + 1  // At most once per test for each shift.
32      C_shift ← C_shift ∪ {s}
33  UpdateFeatureStatisticsAndCost(H, θ*, Q, D, o)
34  return (Q', D', s*, θ*, Δκ, z, H, C_shift, N_cov)

```

## 3.8 方法小结

ECST 将等价关系变换从单纯的查询生成技术扩展为一种执行上下文探索机制。其首先指定希望探索的优化或执行边界，再通过 $\pi,\rho,\phi$ 联合控制关系表示及查询结构，在语义保持约束下合成候选测试，最后通过计划分析确认迁移，并利用覆盖反馈指导后续生成。结果一致性 Oracle 独立运行，确保无论迁移是否成功都不会遗漏有效变换产生的潜在错误。由此，ECST 能够以目标驱动、受约束且可反馈的方式，系统探索 DBMS 在不同优化与执行上下文之间保持 SQL 语义一致性的能力。
