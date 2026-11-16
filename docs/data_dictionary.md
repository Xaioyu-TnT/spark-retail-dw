# 数据字典与指标口径

本文件是数仓的「口径说明书」：字段含义、分层产物、指标计算规则。
维护它的目的是**避免两个人算出两个 GMV**——口径不一致是数据团队最常见的事故来源。

> 所有行数为 2026-11-15 全量统计（90 天数据，2026-01-01 ~ 2026-03-31）。

## 1. 源表（ODS 输入）

| 文件 | 行数 | 字段 | 说明 |
|---|---|---|---|
| `orders.csv` | 100,000 | order_id, user_id, order_status, order_date | 订单主表；状态三值：`paid` 60,200 / `cancel` 19,975 / `refund` 19,825 |
| `order_items.csv` | 200,008 | order_id, item_id, product_id, quantity, unit_price, order_date | 订单明细；一单多行 |
| `users.csv` | 20,000 | user_id, user_name, gender, age, region_id, reg_date | 用户维度 |
| `products.csv` | 200 | product_id, product_name, category, price | 商品维度；`category` 共 6 类 |
| `region.csv` | 6 | region_id, region_name | 地区维度 |

**字段约定**

- 金额：源数据为 `double`，入仓后统一转 `decimal(12,2)`，避免浮点累加误差
- 日期：全部为 `yyyy-MM-dd` 字符串，入仓转 `date`
- 主键：订单明细的主键是 `(order_id, item_id)`，不是 `order_id`

## 2. 分层产物

### 2.1 ODS（贴源，分区 = order_date）

| 表 | 行数 | 分区 | 字段 |
|---|---|---|---|
| `ods_orders` | 100,000 | 90 | order_id, user_id, order_status, order_date |
| `ods_order_items` | 200,008 | 90 | order_id, item_id, product_id, quantity, unit_price, order_date |
| `ods_users` | 20,000 | 0 | user_id, user_name, gender, age, region_id, reg_date |
| `ods_products` | 200 | 0 | product_id, product_name, category, price |
| `ods_region` | 6 | 0 | region_id, region_name |

> 维表（users / products / region）是小表，全量覆盖不分区；事实表按天分区，支持按日增量与分区裁剪。

### 2.2 DWD（明细宽表，分区 = order_date）

| 表 | 行数 | 分区 | 说明 |
|---|---|---|---|
| `dwd_order_detail` | 120,322 | 90 | 5 张源表 join 后的明细宽表，一行 = 一个订单商品；**已过滤为仅 paid**（60,200 个订单） |

| 字段 | 类型 | 含义 | 来源 |
|---|---|---|---|
| order_id | int | 订单号 | orders |
| item_id | int | 明细行号 | order_items |
| user_id | int | 用户号 | orders |
| product_id | int | 商品号 | order_items |
| category | string | 品类（6 类） | products |
| region_name | string | 地区 | region |
| order_status | string | 订单状态 | orders |
| quantity | int | 数量 | order_items |
| unit_price | double | 单价 | order_items |
| **amount** | **decimal(12,2)** | 金额 = quantity × unit_price | 计算列 |
| **age_group** | **string** | 年龄段 18-25 / 26-35 / 36-45 / 46+ | users.age 派生 |
| order_date | date | 订单日期（**分区键**） | orders |

### 2.3 DWS（汇总层，分区 = order_date）

| 表 | 行数 | 分区 | 粒度 | 字段 |
|---|---|---|---|---|
| `dws_category_day` | 540 | 90 | 品类 × 日 | category, order_cnt, qty, amount |
| `dws_user_day` | 59,251 | 90 | 用户 × 日 | user_id, order_cnt, amount |
| `dws_age_day` | 360 | 90 | 年龄段 × 日 | age_group, order_cnt, amount |

### 2.4 ADS（应用层，不分区）

| 表 | 行数 | 用途 | 字段 |
|---|---|---|---|
| `ads_daily_gmv` | 90 | 每日经营看板 | order_date, order_cnt, uv, gmv |
| `ads_category_rank` | 6 | 品类排行与占比 | category, gmv, ratio_pct |
| `ads_age_dist` | 4 | 年龄分布 | age_group, gmv, order_cnt |

## 3. 指标口径

| 指标 | 计算公式 | 排除条件 | 备注 |
|---|---|---|---|
| **GMV** | Σ(quantity × unit_price)，四舍五入 2 位 | 仅 `order_status = 'paid'` | 全量 90 天合计 931,323,579.76；DWD 层已过滤 paid |
| **UV** | COUNT(DISTINCT user_id) | 无 | 按日去重 |
| **订单数** | COUNT(DISTINCT order_id) | 仅 paid | 实测 60,200；不要用明细行数 120,322 |
| **客单价** | GMV / 订单数 | 仅 paid | 931,323,579.76 / 60,200 ≈ 15,470 元 |
| **取消率** | cancel 订单数 / 全部订单数 | 全部 | 19,975 / 100,000 = 19.98% |
| **退款率** | refund 订单数 / 全部订单数 | 全部 | 19,825 / 100,000 = 19.83% |
| **复购率** | 下单 ≥2 次的用户 / 全部下单用户 | 统计全量 90 天 | 实测 84.43%（16,042 / 19,001） |
| **D1 留存** | 首单次日仍下单的用户 / 当日新用户 | 以首单日为新用户日 | 实测 3.1% ~ 4.5% |
| **品类占比** | 品类 GMV / 总 GMV | 仅 paid | 电脑 18.97% / 服饰 18.95% / 手机 18.70% |
| **RFM 分层** | R=最近购买距今天数，F=订单数，M=金额；按分位数打分后组合 | 仅 paid | 高价值活跃用户 1,464 人，贡献 91% 收入 |

**口径陷阱（面试常问）**

1. **取消单要不要算 GMV**：不算。题目若未说明，需主动确认——这是「口径对齐」的典型场景。
2. **明细行数 ≠ 订单数**：`dwd_order_detail` 是明细粒度，统计订单数必须 `COUNT(DISTINCT order_id)`。
3. **留存的分母**：新用户按「首单日」定义，不是「注册日」——本项目数据没有注册行为表，只能用首单日。
4. **金额精度**：全程 `decimal(12,2)`，只在展示时格式化；用 `double` 累加会出现 0.01 级误差。

## 4. 分区与增量约定

| 项 | 约定 |
|---|---|
| 事实表分区键 | `order_date`（date 类型），目录名形如 `order_date=2026-01-15` |
| 写入模式 | `partitionOverwriteMode=dynamic`，只覆盖本次写入的日期分区 |
| 重跑 | 同一天重跑安全（幂等），因为按分区覆盖而非追加 |
| 分区数校验 | 90 天数据应产生 90 个分区，缺失即视为数据不完整（见 `jobs/check_quality.py`） |
| 空分区处理 | 质量校验会统计空分区数，当前为 0 |

## 5. 数据质量校验对照

`jobs/check_quality.py` 的 5 条断言与本文档的对应关系：

| 断言 | 校验内容 | 与本文档的对应 |
|---|---|---|
| 完整性 | ODS 行数 = 源文件行数 | 第 1 节行数 |
| 唯一性 | 明细主键不重复 | 第 1 节「主键是 (order_id, item_id)」 |
| 有效性 | 金额 > 0 且 = 数量 × 单价 | 第 2.2 节 amount 计算列 |
| 时效性 | 90 个分区且非空 | 第 4 节分区约定 |
| 一致性 | 总行数与基线偏差 ≤1% | 第 2.2 节行数 |

---

**维护约定**：新增表或修改指标口径时，同步更新本文件并在 PR 描述里写明「口径变化」——这是数据团队的基本纪律。
