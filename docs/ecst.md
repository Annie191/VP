# 4 方法：执行上下文迁移测试（ECST）

## 4.1 方法概述

本文提出 **Execution-Context Shift Testing（ECST）**，一种面向执行上下文迁移的数据库等价测试方法。ECST 的基本思想是：在保持 SQL 语义等价的前提下，通过关系变换，引导同一逻辑操作进入不同的优化和执行环境，随后依据执行计划与执行行为识别实际发生的迁移，并通过等价查询的结果一致性检查发现潜在逻辑错误。与仅在生成等价查询之后比较结果的测试流程不同，ECST 在生成之前显式选择待探索的执行边界，并在生成之后检查该边界是否真正发生变化，实际迁移结果又反过来指导后续关系变换的选择，形成目标导向的闭环测试。

$$
\underbrace{s}_{\text{目标迁移}}
\longrightarrow
\underbrace{\theta=(\pi,\rho,\phi)}_{\text{变换配置}}
\longrightarrow
\underbrace{(Q,Q')}_{\text{等价测试实例}}
\longrightarrow
\underbrace{\Delta\kappa}_{\text{实际迁移}}
\longrightarrow
\underbrace{H}_{\text{反馈更新}}.
$$

其中，$\pi$ 为关系表示配置，$\rho$ 为关系重构策略，$\phi$ 为目标操作在等价查询中的嵌入位置。

## 4.2 种子查询与目标操作生成

ECST 首先构造数据库状态 $D$、种子查询 $Q$ 和目标逻辑操作 $o$：

$$
(D,Q,o)
$$

测试数据库包含具有不同数据类型、NULL 值、重复值、边界值和索引配置的基础关系。查询生成器在满足目标 DBMS 语法与类型约束的条件下，组合过滤、投影、连接、聚合、子查询等 SQL 结构，并从生成的查询中选取可识别且可追踪的目标逻辑操作。目标操作包括用于值计算的表达式，如 CAST、CASE、算术运算及日期和字符串函数，以及用于条件判断的谓词，如比较谓词、IS NULL、IN 和复合布尔谓词。这些操作可以出现在 SELECT、WHERE、ON 和 HAVING 等不同逻辑位置，并可能在查询优化后由不同的物理算子执行。对于选定的目标操作 $o$，ECST 记录其规范化表示、所在查询位置及属性依赖集合，为后续目标迁移的适用性判断、关系变换合成和执行上下文分析提供依据。

## 4.3 目标迁移建模与关系变换合成

给定种子查询 \(Q\)、数据库状态 \(D\) 和目标逻辑操作 \(o\)，**目标迁移 \(s\)** 描述希望该操作在某个执行上下文维度上发生的预期变化。**关系变换 \(\theta\)** 则用于对原始查询 \(Q\) 构造可能诱导该变化的等价查询 \(Q'\)。

### 4.3.1 目标迁移建模

根据 Definition 3，目标迁移被表示为：

$$
s=\langle d,v,v'\rangle,
$$

其中，\(d\) 是执行上下文 \(\kappa\) 的一个维度，\(v\) 和 \(v'\) 分别表示该维度上的迁移前状态与预期迁移后状态。

设 \(\mathcal{S}\) 为 ECST 定义的目标迁移类型集合，\(\mathcal{S}_D\) 为当前 DBMS 支持的目标迁移集合。对于当前种子查询 \(Q\) 和目标逻辑操作 \(o\)，定义当前测试条件下目标迁移 \(s\) 的所有可选集合\(\mathcal{S}_{\mathrm{valid}}\)，则有：

$$
s\in\mathcal{S}_{\mathrm{valid}}\subseteq\mathcal{S}_D\subseteq\mathcal{S}
$$

**表 1：目标迁移规则表（待完善）**

| 目标迁移 | 适用前提 | 计划验收条件 |
|---|---|---|
| Root $\leftrightarrow$ Cop | DBMS 具有多层表达式执行能力| 对应表达式实际由 Root 与 Cop 两种位置执行 |
| Pre-Join $\leftrightarrow$ Post-Join | 目标表达式具有可调整的属性依赖 | 表达式在连接输入侧与输出侧的物理位置不同 |
| Pushdown $\leftrightarrow$ Non-Pushdown | 存在可识别的下推候选 | 谓词跨越目标下推边界 |
| Merge $\leftrightarrow$ Materialization | 存在可合并或物化的关系输入 | 计划中确认合并与物化两类执行路径 |
| Table Scan $\leftrightarrow$ Index Scan | 可建立可用索引 | 访问路径实际发生切换 |
| Join Algorithm Shift | DBMS 支持多种连接算法 | $R_1$；连接键、索引及关系结构 | 物理 JOIN 算法不同 |
| Selection $\leftrightarrow$ Projection | 表达式能够合法构造为过滤或派生计算 | 对应表达式归属的物理算子变化 |


### 4.3.2 变换配置空间

定义参数化变换：

$$
\boxed{\mathcal T_{\theta}(Q)=Q',\qquad\theta=\langle\pi,\rho,\phi\rangle.}
$$


三类参数各自承担不同的控制作用。

| 参数 | 控制对象 | 回答的问题 | 示例 |
|---|---|---|---|
| $\pi$ | 底层关系表示和属性分布 | 数据来自什么关系？ | 原始数据表\(\pi_0\)、原始表被按列拆分的多张表\(\pi_1\)、视图封装后的原始表\(\pi_2\)、原始表被完整复制的多个物理副本\(\pi_3\)；以及属性的同侧或跨侧分布 |
| $\rho$ | 关系重构与封装结构 | 这些关系如何组织？ | 直接访问、INNER JOIN、OUTER JOIN、CTE、派生表、VIEW、标量子查询，以及 JOIN + CTE 等组合策略 |
| $\phi$ | \(o\)的逻辑嵌入位置 | 目标表达式如何嵌入重构查询？ | 目标谓词配置于 WHERE、INNER JOIN ON、CTE 内层或外层等合法位置；目标表达式配置于 SELECT 投影、WHERE 过滤等合法上下文 |



设 $\Pi,\mathcal P,\Phi$ 分别为三类配置空间，则候选组合为 $\Pi\times\mathcal P\times\Phi$。然而，并非所有组合都满足语义等价与目标适用性。对于给定种子查询 \(Q\) 和目标迁移 $s$，仅保留：

$$
\boxed{
\Theta_s(Q,D)=\left\{
\theta\in\Pi\times\mathcal P \times\Phi
\;\middle|\;
\operatorname{Legal}(\theta,Q,D)
\land
\operatorname{Applicable}(s,\theta,Q,o)
\right\}.
}
$$

`Legal` 表示变换满足类型、行映射、NULL、重复行、外连接、排序与确定性等语义约束；`Applicable` 表示变换的结构具有诱导指定迁移的条件。

### 4.3.3 关系变换合成

考虑原始关系 `src(id, a, b, c, d, e, f, g, h)`，其中 `id` 为唯一且非空的行标识。种子查询 $Q$ 为：

```sql
SELECT a, b, d * c AS x
FROM src
WHERE f = 1
  AND c BETWEEN 1 AND 20
  AND d + e > 100;
```

ECST 选择算术比较谓词作为目标逻辑操作：

$$
o=d+e>100
\qquad
$$

在原始查询中，目标谓词的全部依赖属性均来自单个基础关系 `src`。如果 DBMS 支持该表达式的下推，优化器可以将其与其他过滤条件一起下推至基础表扫描侧。假设该表存储于 TiDB 数据库， 执行计划确认原始目标谓词被下推至扫描侧 `cop[tikv]` 的 Selection 算子中计算，而非在 Root 层执行。ECST 希望通过关系变换，使目标谓词不再能够直接下推至单个基础关系的扫描侧，选择目标迁移：

$$
s=\langle
\mathrm{PredicatePushdown},
\mathrm{Pushed},
\mathrm{NotPushed}
\rangle

$$

**关系表示和属性分布$\pi$。** ECST 选择纵向分区表示 $\pi_1$，将原始关系拆分为：

$$
\begin{aligned}
L &= (id,a,c,d,f,h),\\
R &= (id,b,e,g).
\end{aligned}
$$

左右关系均保留行标识 `id`，并保持原始属性值与行对应关系。通过该配置，目标谓词依赖的两个属性分布在不同关系中：

$$
\operatorname{loc}(d)=L,
\qquad
\operatorname{loc}(e)=R.
$$

**关系重构与封装结构 $\rho$。** ECST 选择 `CTE + INNER JOIN` 作为重构策略，通过 `id` 连接左右分区，并使用 CTE 封装重构关系，为优化器提供不同的关系合并和下推边界。

**目标谓词的逻辑嵌入位置 $\phi$。** ECST 根据表达式的属性依赖将依赖左右分区的目标谓词附着于 `INNER JOIN` 的 `ON`。

最终得到变换配置：

$$
\boxed{
\theta=
\langle
\pi_{\mathrm{1}},
\rho_{\mathrm{cte+inner\_join}},
\phi_{\mathrm{join\_on}}
\rangle.
}
$$

应用该配置，得到变换查询 $Q'$：

```sql
WITH t AS (
    SELECT l.a, r.b, l.c, l.d, r.e, l.f
    FROM part_l AS l
    JOIN part_r AS r
      ON l.id = r.id
     AND l.d + r.e > 100
)
SELECT a, b, d * c AS x
FROM t
WHERE f = 1
  AND c BETWEEN 1 AND 20;
```

在左右分区能够通过唯一行标识完整重构原始关系，并保持数据类型、属性值及 SQL 语义的前提下，两个查询满足：

$$
\boxed{
\llbracket Q\rrbracket_D
=
\llbracket Q'\rrbracket_{D}.
}
$$

变换前，目标谓词的全部输入来自单个基础关系，具备扫描侧整体下推的条件。变换后，属性 $d$ 和 $e$ 分别来自 JOIN 的左右输入，完整目标谓词通常需要获取两侧属性后才能求值，因而不再具备直接下推至任意单个分区扫描算子的条件。

## 4.4 执行上下文识别与迁移验证

§4.3 中的目标迁移 \(s\) 只规定了希望观察到的上下文迁移，不能仅凭变换配置 \(\theta\) 推断该行为已经发生。本节根据原始查询与变换查询的实际执行计划，验证目标逻辑操作的执行上下文迁移是否按预期发生。该步骤回答的是“DBMS 实际执行时发生了什么”，而不是“查询结果是否正确”；结果正确性由 §4.6 中独立的语义一致性 Oracle 判断。

ECST 分别获取原始查询与变换查询的物理计划：

$$
P=P(Q,D,S),\qquad P'=P(Q',D,S).
$$

由于查询优化使目标操作在物理执行计划中的表示形式和位置可能与原始 SQL 不同，ECST 不仅仅依赖 SQL 文本匹配来识别目标操作。为此，ECST 在查询生成阶段记录原始查询 \(Q\) 与变换查询 \(Q'\) 中目标操作的对应关系，并通过表达式规范化消除别名、属性限定及语法表示差异。同时，ECST 维护目标操作的属性血缘信息，记录目标操作的依赖属性在关系变换前后的来源及映射关系。结合这些信息，ECST 分析物理执行计划中的过滤条件、投影表达式和连接条件等信息，提取原始查询和变换查询的执行上下文：

$$
\kappa(o,Q,D,S)=\langle L,O,B,A,E\rangle,
$$

$$
\kappa(o,Q',D,S)=\langle L',O',B',A',E'\rangle.
$$

计算实际迁移：

$$
\boxed{\Delta\kappa=\operatorname{Diff}(\kappa(o,Q,D,S),\kappa(o,Q',D,S)).}
$$

对于目标 $s=\langle d,v,v'\rangle$，通过比较对应维度是否由 $v$ 变为 $v'$ 判断迁移是否成功：

$$
\boxed{\operatorname{Match}(\Delta\kappa,s)=\mathrm{True}.}
$$

根据 Definition 3 中的目标迁移匹配规则，形式化地定义目标迁移的验证结果：

$$
z=
\begin{cases}
\mathrm{Success},
&
\operatorname{Match}(\Delta\kappa,s)=\mathrm{True}
\\[4pt]
\mathrm{Failure},
&
\operatorname{Match}(\Delta\kappa,s)=\mathrm{False}
\\[4pt]
\mathrm{Unknown},
&
\operatorname{Match}(\Delta\kappa,s)
\text{ 无法确定}
\end{cases}
$$

ECST 区分三种观察状态：

- **Success**：有足够证据确认实际上下文变化满足目标迁移 \(s\)。
- **Failure**：有足够证据确认目标迁移未发生；这只表示预期目标未命中，不表示执行上下文完全没有变化。
- **Unknown**：因优化改写而无法被可靠识别，或存在多个无法区分的匹配位置，难以判断目标迁移是否发生。

一次查询变换还可能同时触发多个迁移维度，因此 ECST 不仅判断当前目标是否命中，还记录实际观察到的所有迁移类型。对于第 \(t\) 次测试，用 \(\Delta\kappa_t\) 表示实际上下文差异，用 \(\operatorname{ObservedShifts}_t\) 表示有证据确认发生的迁移类型集合，则：

$$
\operatorname{ObservedShifts}_t
=
\left\{
s\in\mathcal{S}_D
\mid
\operatorname{Match}(\Delta\kappa_t,s)=\mathrm{True}
\right\}.
$$

例如，ECST 试图诱导 Root → Coprocessor 迁移，但实际执行计划显示 Table Scan → Index Scan。此时目标迁移判定为 Failure，但观察到的 Access Path Shift 仍应被记录。

## 4.5 反馈引导的上下文探索（Feedback-Guided Context Exploration）

§4.4 为每次测试提供两类信息：一类是预定目标迁移的验证状态 \(z_t\)，另一类是实际观察到的迁移集合 \(\operatorname{ObservedShifts}_t\)。前者用于估计特定变换对指定目标的诱导能力；后者用于统计测试实际覆盖了哪些执行边界，包括并非本轮预定目标的迁移。ECST 将这两类反馈与查询特征及测试成本关联起来，更新历史统计，并据此指导后续测试生成。

反馈机制包含两个决策：

1. **Target Selection**：根据目标迁移的覆盖不足度、历史可达性和探索不足度，选择下一轮优先探索的执行边界。
2. **Transformation Selection**：在所选目标的有效候选变换配置集合中，根据历史迁移成功率、配置探索奖励和变换成本，选择具体的 \(\theta=\langle\pi,\rho,\phi\rangle\)。

### 4.5.1 执行上下文反馈信息

对于每次测试，ECST 不仅记录等价查询的执行结果，还记录目标迁移、变换配置及实际观察到的执行上下文变化。

设第 \(t\) 次测试选择目标迁移 \(s_t\)，并采用变换配置：

$$
\theta_t=\langle\pi_t,\rho_t,\phi_t\rangle
$$

生成等价查询：

$$
(Q_t')=\mathcal{T}_{\theta_t}(Q_t)
$$

通过执行计划分析，获得实际执行上下文变化：

$$
\Delta\kappa_t
=
\operatorname{Diff}
\left(
\kappa(o_t,Q_t,D_t,S),
\kappa(o_t',Q_t',D_t,S)
\right)
$$

ECST 将本次测试的反馈记录定义为：

$$
\boxed{
h_t=
\langle
s_t,\theta_t,f_t,\Delta\kappa_t,z_t,\operatorname{ObservedShifts}_t,c_t
\rangle
}
$$

其中，查询特征 \(f_t\) 包括目标逻辑操作、属性依赖、关系结构、索引可用性、数据规模、具体访问路径条件及 DBMS 等信息；\(c_t\) 记录本次 seed 生成、候选合成、关系物化、计划分析及执行所消耗的测试成本。物化拒绝单独反馈配置可执行性和物化成本，不计入迁移 Success/Failure/Unknown 统计。



### 4.5.2 变换—迁移关联统计

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

其中，\(a,b>0\) 为平滑参数。例如，当 \(a=b=1\) 时，尚未尝试的配置具有中性的初始估计，而不会因为缺少历史数据被错误地认为迁移成功率为零。

需要注意，变换诱导能力与 DBMS、表达式类别、属性分布及查询结构有关。因此，ECST 按 DBMS 和相关查询特征维护统计，并在某个具体组合缺少足够样本时，参考同一变换类别的历史信息。


### 4.5.3 第一级反馈：目标执行上下文迁移选择

ECST 的第一级反馈用于决定下一轮优先探索哪一种执行上下文迁移。对于当前查询条件下的每个候选目标 \(s\)，ECST 依据 §4.3 的迁移规则与合法性约束确定其候选配置空间 \(\Theta_s\)，再结合这些配置的历史迁移表现估计该目标的可达性。若某个目标在当前条件下不存在合法候选配置，则不将其作为本轮可选目标。这样，目标评分既不会把规则上不适用的迁移当作可选项，也不会仅凭 SQL 语法结构假定迁移必然发生。

目标迁移可达性定义为：

$$
\boxed{
\operatorname{Reachability}(s)
=
\max_{\theta\in\Theta_s}
\widetilde{p}(s\mid\theta)
}
$$

其中，\(\widetilde{p}(s\mid\theta)\) 采用 §4.5.2 中的迁移成功率估计。

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

- \(N_{\mathrm{total}}\) 为当前 DBMS 下累计的目标迁移尝试次数，即 \(\sum_{s\in\mathcal{S}_D}N_s^{\mathrm{trial}}\)。
- \(N_s^{\mathrm{trial}}\) 为目标迁移 \(s\) 被选择并尝试的累计次数，Success、Failure 和 Unknown 均计为一次尝试。
- \(N_s^{\mathrm{cov}}\) 为迁移 \(s\) 在所有测试中被实际观察到的累计次数，不限于 \(s\) 是该轮的预定目标；同一次测试中，同一迁移类型最多计数一次。

因此，Novelty 衡量某种迁移被观察到的频次有多低，而 Explore 衡量该迁移被尝试的频次有多低。二者并不等价：一个迁移可能已经被尝试多次却始终未发生，也可能偶然发生过但尚未被充分尝试。

定义已覆盖的迁移集合与全局迁移覆盖率：

$$
\mathcal{C}_{\mathrm{shift}}
=
\left\{
s\in\mathcal{S}_D
\mid
N_s^{\mathrm{cov}}>0
\right\},
$$

$$
\boxed{
\operatorname{ShiftCoverage}
=
\frac{
|\mathcal{C}_{\mathrm{shift}}\cap\mathcal{S}_D|
}{
|\mathcal{S}_D|
}
}.
$$

\(\operatorname{ShiftCoverage}\) 衡量当前 DBMS 支持的目标迁移类型中，已经通过实际执行证据确认发生过的比例。

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

TiDB 实现保留上面的 Novelty 基础项
\(1/\sqrt{1+N_s^{\mathrm{cov}}}\)，并在工程调度器中额外加入失败惩罚、候选可达性和新查询特征探索项。这些项用于处理 DBMS 特定的不可达配置，不改变 Novelty 的方法定义；实现统计中将它们作为独立的调度启发式记录。

因此，第一级选择同时兼顾三个目标：

1. 优先探索尚未覆盖的执行边界。
2. 优先选择当前候选配置较有可能触发的目标迁移。
3. 为尚未充分测试的迁移保留探索机会。

### 4.5.4 第二级反馈：目标迁移导向的变换配置选择

选定目标迁移 \(s^*\) 后，ECST 使用 §4.3 中定义的目标条件候选空间 \(\Theta_{s^*}(Q,D,o)\)，筛选并枚举当前轮次可执行的合法配置。每个候选配置包含：

$$
\theta=\langle\pi,\rho,\phi\rangle
$$

对于候选配置 \(\theta\)，定义该配置对目标 \(s\) 的总尝试次数：

$$
N_{\theta,s}^{\mathrm{trial}}
=
N_{\theta,s}^{+}
+
N_{\theta,s}^{-}
+
N_{\theta,s}^{?}.
$$

探索奖励 $U(\theta,s)$ 期望尝试次数较少的配置获得更多探索机会，避免算法始终选择少数早期表现较好的变换：

$$
\boxed
{
U(\theta,s)
=
\sqrt
{
\frac
{
2\ln(2+N_s^{\mathrm{trial}})
}
{
1+N_{\theta,s}^{\mathrm{trial}}
}
}
}
$$

其次，考虑变换生成时间、数据库准备成本、执行计划获取时间以及查询执行时间，定义变换成本：

$$
\operatorname{Cost}(\theta)
$$

变换选择评分定义为：

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

其中，$\operatorname{Cost}(\theta)$ 为变换成本，包括变换生成时间、数据库准备成本、执行计划获取时间以及查询执行时间。

对于目标迁移 \(s^*\)，在兼顾测试成本的前提下，ECST 优先选择既有较高概率诱导目标迁移，又具有一定探索价值的变换配置：

$$
\boxed{
\theta^*
=
\arg\max_{\theta\in\Theta_{s^*}}
\operatorname{TransformScore}(\theta,s^*)
}
$$

随后生成新的等价测试查询：

$$
(Q')
=
\mathcal{T}_{\theta^*}(Q)
$$

### 4.5.5 反馈更新过程

每次完成实际迁移验证后，ECST 将目标命中情况与实际迁移覆盖情况分开更新。

首先，据每次测试的目标迁移 \(s_t\) 及所采用的变换配置 \(\theta_t\)，更新相应的累计尝试次数，该统计不受迁移验证结果的影响：

$$
N_{s_t}^{\mathrm{trial}}
\leftarrow
N_{s_t}^{\mathrm{trial}}+1,
\qquad
N_{\theta_t,s_t}^{\mathrm{trial}}
\leftarrow
N_{\theta_t,s_t}^{\mathrm{trial}}+1.
$$

随后，根据 §3.4 的验证状态 \(z_t\) 更新目标迁移与变换配置的关联统计：

- **Success**：确认目标迁移发生，更新
$$
  N_{\theta_t,s_t}^{+}
  \leftarrow
  N_{\theta_t,s_t}^{+}+1.
$$
- **Failure**：有足够证据确认目标迁移未发生，更新
$$
  N_{\theta_t,s_t}^{-}
  \leftarrow
  N_{\theta_t,s_t}^{-}+1.
$$
- **Unknown**：无法可靠判断目标迁移是否发生，更新
$$
  N_{\theta_t,s_t}^{?}
  \leftarrow
  N_{\theta_t,s_t}^{?}+1.
$$


最后，ECST 根据实际观察到的迁移集合更新覆盖统计。对于每一种 \(\hat{s}\in\operatorname{ObservedShifts}_t\)，执行：

$$
N_{\hat{s}}^{\mathrm{cov}}
\leftarrow
N_{\hat{s}}^{\mathrm{cov}}+1.
$$

同一次测试中，同一迁移类型最多更新一次。该更新与 \(z_t\) 独立：若目标迁移成功，则 \(s_t\) 会作为实际观察到的迁移被计入覆盖；若目标迁移失败但实际发生了其他迁移，则只将这些已确认的非目标迁移加入对应覆盖统计。若某种变化无法得到可靠证据，则不将其计为已覆盖。

完成上述更新后，ECST 重新计算相关的 \(\widetilde{p}(s\mid\theta)\)、\(\operatorname{Novelty}(s)\)、\(\operatorname{Explore}(s)\) 和全局 \(\operatorname{ShiftCoverage}\)，并将更新后的统计用于下一轮目标与变换选择。

### 4.5.6 反馈引导示例

考虑目标迁移：

$$
s=\langle
\mathrm{PredicatePushdown},
\mathrm{Pushed},
\mathrm{NotPushed}
\rangle

$$

ECST 根据规则库生成以下候选配置：

| 配置 | 关系表示 | 重构策略 | 表达式位置 |
|---|---|---|---|
| \(\theta_1\) | \(\pi_1\) | INNER JOIN | 单侧 WHERE 谓词 |
| \(\theta_2\) | \(\pi_1\) | INNER JOIN | 内层 ON 谓词 |
| \(\theta_3\) | \(\pi_2\) | VIEW | 外层 WHERE 谓词 |
| \(\theta_4\) | \(\pi_2\) | DERIVED TABLE | 派生列过滤 |

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

随着测试持续进行，ECST 不断更新目标迁移和变换配置的关联统计，从而逐渐学习哪些配置更适合探索特定的执行上下文边界。

## 4.6 Oracle 语义一致性验证

执行上下文迁移验证回答“DBMS 是否发生了预期的执行上下文变化”，并为后续测试生成提供反馈；Oracle 语义一致性验证回答“语义等价的查询是否产生了不一致结果”，负责检测潜在逻辑错误。后者不以目标迁移成功为执行前提，只要查询对满足语义保持约束，ECST 就比较其执行结果，即使迁移状态为 Failure 或 Unknown 也不跳过结果检查。

对于原始查询与变换查询，语义一致性 Oracle 比较：

$$
\operatorname{Result}(Q,D,S)
\overset{?}{=}
\operatorname{Result}(Q',D,S).
$$

这里的结果比较遵循相应 SQL 语义，包括结果多重集以及查询中明确规定的顺序要求。

若结果不一致，ECST 首先在相同数据与会话条件下重复执行，以排除不稳定结果；随后尝试减少表、列、行、表达式和多余 SQL 结构，同时保留最初的结果不一致。对于能够观察到明确迁移的案例，进一步通过禁止/允许特定下推、切换物理算子或调整优化配置进行受控 A/B/A 干预，判断结果不一致是否随目标执行边界的切换可逆变化。该过程区分了执行上下文迁移的观察与错误触发原因的验证，避免仅凭执行计划差异就将特定迁移认定为逻辑错误的根因。

每条报告保存原始与变换 SQL、数据库状态生成方式、\(\pi/\rho/\phi\) 配置、目标 \(s\)、实测 \(\Delta\kappa\)、迁移验证状态、执行计划、结果差异以及最小化和干预记录，以支持复现与后续根因分析。

## 4.7 ECST 主算法

ECST 采用三个算法组织测试流程：Algorithm 1 为主测试循环，Algorithm 2 根据预定迁移目标合成变换配置，Algorithm 3 依据历史反馈选择目标及配置、验证实际迁移并更新统计。调用关系为 Algorithm 1 → Algorithm 3 → Algorithm 2。

**Algorithm 1：Execution-Context Shift Testing（ECST 主流程）**

```sql
Input:
    DBMS: target database system
    S_D: context-shift targets supported by DBMS
    G: target-driven transformation rule library
    B: testing budget
Output:
    Bugs: validated candidate semantic inconsistencies

 1  Initialize history H, coverage C_shift ← ∅,
       N_s^cov ← 0, N_s^trial ← 0 (∀s ∈ S_D), N_total ← 0
 2  Bugs ← ∅
 3  while testing budget B remains do
 4      (D, Q, o) ← GenerateSeed(DBMS)
 5      S ← ConfigureSession(DBMS)
 6      Γ ← SemanticConstraints(DBMS, D, Q, o, S)
 7      P_seed ← Explain(Q, D, S)
 8      κ_seed ← ExtractContext(o, Q, D, S, P_seed)
 9      S_valid ← ApplicableTargets(S_D, G, Q, D, o, κ_seed)
10      if S_valid = ∅ then continue
11      result ← FeedbackGuidedExplore(
                   S_valid, S_D, G, Q, D, o, Γ, S,
                   H, C_shift, N_cov, N_trial, N_total)  // Algorithm 3
12      if result = ⊥ then continue
13      (Q', s*, θ*, Δκ, z, Observed, c_pre) ← result
          // The required relations have been prepared within D.
14      Y  ← Execute(Q,  D, S)
15      Y' ← Execute(Q', D, S)
16      c ← c_pre + ExecutionCost(Y, Y')
17      FinalizeFeedbackCost(H, θ*, s*, c)
          // Do not update migration counters a second time.
18      if Comparable(Y, Y') and not SemanticallyEqual(Y, Y') then
19          bug ← ValidateAndMinimize(Q, Q', D, S, s*, θ*, Δκ, z)
20          if bug is verified then Bugs ← Bugs ∪ {bug}
21      CleanupPreparedRelations(D, θ*)  // If required by test isolation.
22  return Bugs
```

**Algorithm 2：Target-Driven Transformation Synthesis（候选变换合成）**

```sql
Input:
    G: target-driven transformation rule library
    s: target context shift
    Q, D, o: seed query, same database, target logical operation
    Γ: semantic-preservation constraints
Output:
    Θ_s(Q, D, o): valid candidate configurations

 1  Θ_s ← ∅
 2  Rules ← SelectApplicableRules(G, s, Q, D, o)
 3  Dep ← AttributeDependencies(o)
 4  for each rule g in Rules do
 5      Π_g ← GenerateRepresentationConfigurations(g, s, D, Dep)
          // Each π specifies relation layout and its in-D preparation.
 6      for each π in Π_g do
 7          P_g ← GenerateReconstructionConfigurations(g, s, π, Q)
 8          for each ρ in P_g do
 9              Φ_g ← GenerateTargetEmbeddings(g, s, π, ρ, Q, o)
                  // φ controls the logical embedding of the selected o.
10              for each φ in Φ_g do
11                  θ ← ⟨π, ρ, φ⟩
12                  if Legal(θ, Q, D, Γ)
                        and Applicable(s, θ, Q, o)
                        and PreservesTargetCorrespondence(o, θ) then
13                      Θ_s ← Θ_s ∪ {θ}
14  return Θ_s
```

**Algorithm 3：Feedback-Guided Context Exploration（反馈引导探索）**

```sql
Input:
    S_valid, S_D: currently applicable and DBMS-supported targets
    G: transformation rule library
    Q, D, o, Γ: seed query, same database, operation and constraints
    S: session configuration
    H, C_shift, N_cov, N_trial, N_total: exploration feedback
Output:
    (Q', s*, θ*, Δκ, z, Observed, c_pre), or ⊥

 1  f ← QueryFeatures(DBMS, Q, D, o)
 2  CandidateTargets ← ∅
 3  for each s in S_valid do
 4      Θ_s ← SynthesizeValidConfigurations(G, s, Q, D, o, Γ)
              // Algorithm 2
 5      if Θ_s ≠ ∅ then CandidateTargets ← CandidateTargets ∪ {s}
 6  while CandidateTargets ≠ ∅ do
        // Stage 1: target selection, using Sections 3.5.2–3.5.3.
 7      for each s in CandidateTargets do
 8          novelty ← 1 / sqrt(1 + N_s^cov)
 9          reachability ← max_{θ∈Θ_s} p_tilde(s | θ; H, f)
10          exploration ← sqrt(2*ln(2 + N_total)/(1 + N_s^trial))
11          TargetScore[s] ← λ1*novelty + λ2*reachability
                                 + λ3*exploration
12      s* ← argmax_{s∈CandidateTargets} TargetScore[s]

        // Stage 2: transformation selection, using Section 3.5.4.
13      while Θ_s* ≠ ∅ do
14          for each θ in Θ_s* do
15              success ← p_tilde(s* | θ; H, f)
16              bonus ← sqrt(2*ln(2 + N_s*^trial) /
                                  (1 + N_{θ,s*}^trial))
17              cost ← EstimateCost(θ, H, f)
18              TransformScore[θ] ← α*success + β*bonus - γ*cost
19          θ* ← argmax_{θ∈Θ_s*} TransformScore[θ]

            // Apply θ within the SAME database D.
20          Q' ← ApplyTransformation(Q, D, θ*)
              // Prepare π's relations in D, then apply ρ and φ.
21          if not CheckSemanticPreconditions(Q, Q', D, θ*, Γ) then
22              CleanupPreparedRelations(D, θ*)
23              Θ_s* ← Θ_s* \ {θ*}
24              continue

            // Both plans are obtained AFTER preparation in D.
25          P  ← Explain(Q,  D, S)
26          P' ← Explain(Q', D, S)
27          mapping ← MatchLogicalOperations(o, Q, Q', P, P')
28          Δκ ← AnalyzeContextShift(mapping, P, P', S)
29          z ← MatchTarget(Δκ, s*)  // Success / Failure / Unknown
30          Observed ← {r ∈ S_D : Match(Δκ, r) = True}

            // Target feedback and observed coverage are independent.
31          N_s*^trial ← N_s*^trial + 1
32          N_total ← N_total + 1
33          UpdateTargetHistory(H, f, θ*, s*, z)
              // Increment exactly one of N_{θ*,s*}^+, N_{θ*,s*}^-, N_{θ*,s*}^?.
              // N_{θ*,s*}^trial is their sum; do not increment twice.
34          for each r in Observed do
35              N_r^cov ← N_r^cov + 1  // At most once per type and test.
36              C_shift ← C_shift ∪ {r}
37          c_pre ← MeasuredPreparationSynthesisAndPlanCost(D, θ*, P, P')
38          return (Q', s*, θ*, Δκ, z, Observed, c_pre)
39      CandidateTargets ← CandidateTargets \ {s*}
40  return ⊥

```

## 4.8 方法小结

ECST 将等价关系变换从单纯的查询生成技术扩展为一种执行上下文探索机制。其首先指定希望探索的优化或执行边界，再通过 $\pi,\rho,\phi$ 联合控制关系表示及查询语句结构，在语义保持约束下合成候选测试，最后通过计划分析验证迁移是否发生，并利用覆盖反馈指导后续选择。结果一致性 Oracle 独立运行，确保无论迁移是否成功都不会遗漏有效变换发现的的潜在错误。由此，ECST 能够以目标驱动、受约束且可反馈的方式，系统探索 DBMS 在不同优化与执行上下文之间保持 SQL 语义一致性的能力。
