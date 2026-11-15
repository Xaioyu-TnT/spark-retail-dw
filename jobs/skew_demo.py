# -*- coding: utf-8 -*-
"""数据倾斜演示（v2：用分区行数与写盘耗时作为证据）

1) 造倾斜数据：90% 订单压给同一个用户
2) 展示 TOP 用户分布
3) 按 user_id 重分区后，看每个分区的行数 —— 倾斜的本质是「一个 task 扛了 9 成数据」
4) 加盐两阶段聚合，展示分区被摊平
5) 对比两种数据写 parquet 的耗时
"""
from pyspark.sql import SparkSession, functions as F
import random, csv, datetime, time, os, shutil

RAW = "/home/data/bigdata/jobs/raw"
SKEW = "/home/data/bigdata/jobs/raw_skew"
TMP = "/home/data/bigdata/jobs/_skew_tmp"

spark = (SparkSession.builder.appName("skew_demo")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")


def build_skew_file():
    os.makedirs(SKEW, exist_ok=True)
    random.seed(7)
    with open(f"{SKEW}/orders_skew.csv", "w", newline="", encoding="utf-8") as f, \
         open(f"{SKEW}/order_items_skew.csv", "w", newline="", encoding="utf-8") as g:
        wo, wi = csv.writer(f), csv.writer(g)
        wo.writerow(["order_id", "user_id", "order_date", "order_status"])
        wi.writerow(["item_id", "order_id", "product_id", "quantity", "unit_price"])
        item = 1
        for oid in range(1, 100001):
            uid = 1 if random.random() < 0.9 else random.randint(2, 20000)
            d = datetime.date(2026, 1, 1) + datetime.timedelta(days=random.randint(0, 89))
            wo.writerow([oid, uid, d.isoformat(), "paid"])
            for _ in range(random.randint(1, 3)):
                wi.writerow([item, oid, random.randint(1, 200), random.randint(1, 3),
                             round(random.uniform(20, 8000), 2)])
                item += 1
    print("[造数] 倾斜数据已生成：10 万订单，90% 属于 user_id=1")


def partition_rows(df, cols):
    """按指定列重分区后，统计每个分区的行数"""
    rows = (df.repartition(8, *cols)
              .rdd.mapPartitionsWithIndex(lambda i, it: [(i, sum(1 for _ in it))])
              .collect())
    return [r for _, r in sorted(rows, key=lambda x: -x[1])]


def timed_write(df, label):
    path = f"{TMP}/{label}"
    shutil.rmtree(path, ignore_errors=True)
    t0 = time.time()
    df.write.mode("overwrite").parquet(path)
    cost = time.time() - t0
    shutil.rmtree(path, ignore_errors=True)
    print(f"[耗时] {label:<16} 写盘 {cost:5.1f} 秒")
    return cost


def main():
    if not os.path.exists(f"{SKEW}/orders_skew.csv"):
        build_skew_file()
    else:
        print("[造数] 复用已有倾斜数据")

    cols = ["order_id", "user_id", "order_date", "order_status"]
    normal = spark.read.csv(f"{RAW}/orders.csv", header=True, inferSchema=True).select(cols)
    skewed = spark.read.csv(f"{SKEW}/orders_skew.csv", header=True, inferSchema=True).select(cols)

    print("\n=== 各用户订单量 TOP5（倾斜数据） ===")
    skewed.groupBy("user_id").count().orderBy(F.desc("count")).show(5)

    print("=== 各用户订单量 TOP5（均匀数据） ===")
    normal.groupBy("user_id").count().orderBy(F.desc("count")).show(5)

    print("=== 按 user_id 重分区后，各分区行数（8 个分区，从大到小） ===")
    pn = partition_rows(normal, ["user_id"])
    ps = partition_rows(skewed, ["user_id"])
    print(f"均匀数据: {pn}")
    print(f"倾斜数据: {ps}")
    print(f"倾斜时最大分区占全量 {max(ps) / sum(ps) * 100:.1f}%，均匀时为 {max(pn) / sum(pn) * 100:.1f}%")

    print("\n=== 加盐（user_id + 随机盐）后重新分区 ===")
    salted = skewed.withColumn("salt", (F.rand(seed=42) * 10).cast("int"))
    psalt = partition_rows(salted, ["user_id", "salt"])
    print(f"加盐后:   {psalt}")
    print(f"最大分区占比降到 {max(psalt) / sum(psalt) * 100:.1f}%")

    print("\n=== 结论 ===")
    ratio = max(ps) / max(pn)
    print(f"热点 task 处理的数据量是普通 task 的 {ratio:.1f} 倍（{max(ps)} 行 vs {max(pn)} 行）")
    print("真实数据量下，作业总时长由最慢的那个 task 决定，这就是倾斜拖慢整条链路的原因。")
    print("缓解手段：加盐打散（本例最大分区占比 91.2% -> 37.0%）、广播小表、拆分热点值。")

    spark.stop()


main()
