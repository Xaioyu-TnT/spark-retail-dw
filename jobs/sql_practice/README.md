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

## 待补的题型

按面试出现频率，建议依次补：

1. 分组 topN（每品类销售额前三）——已用窗口函数版本，可写第二种解法（`RANK` vs `ROW_NUMBER` 区别）
2. 连续登录 ≥3 天（日期差 + 分组）
3. 行转列 / 列转行（`CASE WHEN` + 聚合 / `LATERAL VIEW explode`）
4. 累计求和（`SUM OVER ORDER BY`）
5. 同比环比（`LAG`）
6. 去重取最新（`ROW_NUMBER` 或 `MAX + JOIN`）
7. 缺失日期补齐（日期表 left join）
8. 中位数（`percentile_approx`）

> 每写完一个就提交一次，面试时可以直接给仓库链接。
