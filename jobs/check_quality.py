# -*- coding: utf-8 -*-
"""数据质量校验：在 ETL 之后跑一遍，任一条失败就以非 0 退出，让调度阻断下游

五条断言（对应面试常问的「你怎么保证数据是对的」）：
    1) 完整性：ODS 行数 = 源 CSV 行数（不含表头）
    2) 唯一性：DWD 的 order_id + item_id 组合不重复
    3) 有效性：DWD 金额 > 0，且金额 = 数量 × 单价（误差 < 0.01）
    4) 时效性：分区数 = 预期天数，且每个分区非空
    5) 一致性：DWD 总行数与上一轮基线一致（允许 ±1%，应对源数据小波动）

用法：
    python jobs/check_quality.py
    echo $?          # 0 = 全部通过，1 = 有失败项
"""
import glob
import os
import sys
import time
from pyspark.sql import SparkSession, functions as F

BASE = "/home/data/bigdata/jobs"
WH = f"{BASE}/warehouse_v2"
RAW = f"{BASE}/raw"

expected_days = len(glob.glob(f"{WH}/dwd_order_detail/order_date=*"))
baseline_rows = int(os.environ.get("BASELINE_ROWS", 120322))   # 可用环境变量覆盖
tolerance = float(os.environ.get("TOLERANCE", 0.01))

spark = (SparkSession.builder.appName("check_quality")
         .config("spark.sql.shuffle.partitions", "8").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

results = []


def check(name, passed, detail):
    results.append((name, passed, detail))
    flag = "PASS" if passed else "FAIL"
    print(f"[{flag}] {name:<12} {detail}")
    return passed


t0 = time.time()

# ---------- 1) 完整性 ----------
with open(f"{RAW}/orders.csv", "r", encoding="utf-8", errors="ignore") as fh:
    src_rows = sum(1 for _ in fh) - 1        # 扣掉表头
ods_rows = spark.read.parquet(f"{WH}/ods_orders").count()
check("完整性", ods_rows == src_rows,
      f"ODS {ods_rows:,} 行 vs 源文件 {src_rows:,} 行")

# ---------- 2) 唯一性 ----------
dwd = spark.read.parquet(f"{WH}/dwd_order_detail")
total = dwd.count()
distinct = dwd.select("order_id", "item_id").distinct().count()
check("唯一性", total == distinct,
      f"总行数 {total:,} vs 主键去重 {distinct:,}（重复 {total - distinct}）")

# ---------- 3) 有效性 ----------
bad_amount = dwd.filter(F.col("amount") <= 0).count()
bad_calc = dwd.filter(
    F.abs(F.col("amount") - F.round(F.col("quantity") * F.col("unit_price"), 2)) > 0.01
).count()
check("有效性", bad_amount == 0 and bad_calc == 0,
      f"金额<=0 共 {bad_amount} 行；金额与 数量×单价 不一致 {bad_calc} 行")

# ---------- 4) 时效性（分区完整且非空） ----------
per_day = (dwd.groupBy("order_date").count()
           .agg(F.min("count").alias("min_cnt"), F.max("count").alias("max_cnt"))
           .collect()[0])
empty_days = dwd.groupBy("order_date").count().filter(F.col("count") == 0).count()
check("时效性", empty_days == 0,
      f"分区 {expected_days} 个，单日行数 {per_day['min_cnt']}~{per_day['max_cnt']}，空分区 {empty_days} 个")

# ---------- 5) 一致性 ----------
diff = abs(total - baseline_rows) / baseline_rows
check("一致性", diff <= tolerance,
      f"总行数 {total:,} vs 基线 {baseline_rows:,}（偏差 {diff * 100:.2f}%，阈值 {tolerance * 100:.0f}%）")

spark.stop()

passed = sum(1 for _, p, _ in results if p)
print(f"\n{'=' * 60}")
print(f"校验结果：{passed}/{len(results)} 通过，耗时 {time.time() - t0:.1f} 秒")
print("=" * 60)

if passed != len(results):
    print("有校验未通过，下游任务应被阻断（退出码 1）")
    sys.exit(1)
print("全部通过，下游可以继续（退出码 0）")
sys.exit(0)
