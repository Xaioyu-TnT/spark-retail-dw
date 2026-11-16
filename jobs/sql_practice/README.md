# SQL 练习（基于自己的数仓）

不依赖刷题网站的样例表，直接在 `warehouse_v2` 的 12 万行明细上练，
写出来的 SQL 能对上真实指标（比如复购率应与 ADS 的 84.43% 一致）。

## 用法

```bash
source ~/sparkenv/bin/activate
python jobs/sql_practice/run_sql.py retention_d1        # 跑某个 SQL
python jobs/sql_practice/run_sql.py category_share -n 10
python jobs/sql_practice/run_sql.py user_repeat --save ads_user_repeat   # 结果落盘
```

执行器会把数仓目录注册成临时视图，SQL 里直接写表名即可：

| 视图 | 说明 |
|---|---|
| `dwd_order_detail` | 12 万行明细（分区 order_date） |
| `dws_category_day` / `dws_user_day` / `dws_age_day` | 汇总层 |
| `ads_daily_gmv` / `ads_category_rank` / `ads_age_dist` | 应用层 |

## 已有示例

| 文件 | 考点 |
|---|---|
| `retention_d1.sql` | 次日留存：CTE + LEFT JOIN + 日期函数 |
| `user_repeat.sql` | 复购率：子查询 + CASE WHEN 聚合 |
| `category_share.sql` | 品类排行与占比：窗口函数 ROW_NUMBER + 累计分母 |

## 八道高频题（已全部跑通）

| 文件 | 题目 | 考点 | 结果 |
|---|---|---|---|
| `q1_group_topn.sql` | 每个品类销售额前三的商品 | `ROW_NUMBER` + `PARTITION BY` | 18 行（6 品类 × 3） |
| `q2_continuous_days.sql` | 连续下单 ≥3 天的用户 | 日期 − 行号 分组 | 15 个用户，最长 4 天 |
| `q3_row_to_col.sql` | 品类变成列（行转列） | `CASE WHEN` + 聚合 | 90 行 |
| `q4_col_to_row.sql` | 指标拆成行（列转行） | `UNION ALL` | 12 行 |
| `q5_cumulative_sum.sql` | 每天的累计 GMV | `SUM() OVER (ORDER BY)` | 90 行 |
| `q6_month_over_month.sql` | 月度环比 | `LAG()` | 2 月 −8.73%，3 月 +9.21% |
| `q7_latest_order.sql` | 每个用户最新订单 | `ROW_NUMBER` 过滤 rn=1 | 19,001 用户 |
| `q8_fill_missing_dates.sql` | 缺失日期补 0 | 日期序列 + `LEFT JOIN` + `COALESCE` | 90 天、缺失 0 天、GMV 9.31 亿 |

全部结果都做过多重交叉验证：

- 复购率 84.43% 与 ADS 层一致
- 品类占比 18.97 / 18.95 / 18.70% 与 ADS 层一致
- 累计 GMV 终值 9.31 亿与「六个品类加总」一致

## 还能再练的

- 中位数：`percentile_approx(amount, 0.5)`
- 漏斗转化：曝光→点击→下单（需要多张表，本项目没有行为表）
- 窗口函数区别：`ROW_NUMBER` / `RANK` / `DENSE_RANK` 在并列时的不同

> 面试前把 `q2`（连续登录）和 `q6`（环比）默写一遍，这两题最容易卡。
