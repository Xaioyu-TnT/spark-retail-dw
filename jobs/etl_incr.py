# -*- coding: utf-8 -*-
"""增量版星型数仓 ETL
用法：
    python etl_incr.py                # 默认处理数据里最大日期那一天
    python etl_incr.py 2026-01-15     # 处理指定日期
与全量版的区别：
    1) 只处理一天的数据（filter order_date）
    2) 写盘用 dynamic partition overwrite，只覆盖当天分区
    3) DWD 增加年龄段维度 age_group（维度加工示例）
    4) 金额统一转 decimal(12,2)，避免科学计数法
"""
from pyspark.sql import SparkSession, functions as F
import sys, time

RAW = "/home/data/bigdata/jobs/raw"
WH = "/home/data/bigdata/jobs/warehouse_v2"

spark = (SparkSession.builder.appName("star_dw_incr")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")


def rd(name):
    return spark.read.csv(f"{RAW}/{name}.csv", header=True, inferSchema=True)


orders = rd("orders").withColumn("order_date", F.to_date("order_date"))
items = rd("order_items")
users = rd("users")
products = rd("products")
regions = rd("region")

# ---- 决定处理哪一天 ----
if len(sys.argv) > 1:
    target = sys.argv[1]
else:
    target = orders.agg(F.max("order_date")).collect()[0][0].isoformat()
print(f"[增量] 目标日期 = {target}")

t0 = time.time()
day_orders = orders.filter(F.col("order_date") == F.lit(target))
n_orders = day_orders.count()
print(f"[ODS] 当天订单数 = {n_orders}")

# ---- ODS：只覆盖当天分区 ----
day_orders.write.mode("overwrite").partitionBy("order_date").parquet(f"{WH}/ods_orders")
day_items = items.join(day_orders.select("order_id", "order_date"), "order_id")
day_items.write.mode("overwrite").partitionBy("order_date").parquet(f"{WH}/ods_order_items")

# ---- DWD：join 维度 + 年龄段 + decimal ----
dwd = (day_items.alias("i")
       .join(day_orders.alias("o"), "order_id")
       .join(products.alias("p"), "product_id")
       .join(users.alias("u"), "user_id")
       .join(regions.alias("r"), "region_id")
       .select("o.order_id", "i.item_id", "u.user_id", "p.product_id", "p.category",
               "r.region_name", "o.order_date", "o.order_status",
               "i.quantity", "i.unit_price", "u.age")
       .withColumn("amount", F.round(F.col("quantity") * F.col("unit_price"), 2).cast("decimal(12,2)"))
       .withColumn("age_group",
                   F.when(F.col("age") < 26, "18-25")
                    .when(F.col("age") < 36, "26-35")
                    .when(F.col("age") < 46, "36-45")
                    .otherwise("46+"))
       .filter(F.col("order_status") == "paid")
       .drop("age"))
print(f"[DWD] 当天明细行数 = {dwd.count()}")
dwd.write.mode("overwrite").partitionBy("order_date").parquet(f"{WH}/dwd_order_detail")

# ---- DWS：当天两个汇总 ----
dws_cat = (dwd.groupBy("order_date", "category")
           .agg(F.countDistinct("order_id").alias("order_cnt"),
                F.sum("quantity").alias("qty"),
                F.sum("amount").alias("amount")))
dws_cat.write.mode("overwrite").partitionBy("order_date").parquet(f"{WH}/dws_category_day")

dws_age = (dwd.groupBy("order_date", "age_group")
           .agg(F.countDistinct("order_id").alias("order_cnt"),
                F.sum("amount").alias("amount")))
dws_age.write.mode("overwrite").partitionBy("order_date").parquet(f"{WH}/dws_age_day")

dws_user = (dwd.groupBy("order_date", "user_id")
            .agg(F.countDistinct("order_id").alias("order_cnt"),
                 F.sum("amount").alias("amount")))
dws_user.write.mode("overwrite").partitionBy("order_date").parquet(f"{WH}/dws_user_day")

# ---- ADS：当天报表 ----
ads_daily = (dwd.groupBy("order_date")
             .agg(F.countDistinct("order_id").alias("order_cnt"),
                  F.countDistinct("user_id").alias("uv"),
                  F.sum("amount").alias("gmv")))
ads_daily.show(truncate=False)
ads_daily.write.mode("overwrite").partitionBy("order_date").parquet(f"{WH}/ads_daily_gmv")

# 品类排行、年龄分布：基于全量 DWS 重算（数据量小，直接全量刷）
full_dws_cat = spark.read.parquet(f"{WH}/dws_category_day")
ads_cat = full_dws_cat.groupBy("category").agg(F.sum("amount").alias("gmv"))
total = ads_cat.agg(F.sum("gmv")).collect()[0][0]
ads_cat = ads_cat.withColumn("ratio_pct", F.round(F.col("gmv") / F.lit(total) * 100, 2))
ads_cat.orderBy(F.desc("gmv")).show(truncate=False)
ads_cat.write.mode("overwrite").parquet(f"{WH}/ads_category_rank")

full_dws_age = spark.read.parquet(f"{WH}/dws_age_day")
ads_age = full_dws_age.groupBy("age_group").agg(F.sum("amount").alias("gmv"),
                                                F.sum("order_cnt").alias("order_cnt"))
ads_age.orderBy("age_group").show(truncate=False)
ads_age.write.mode("overwrite").parquet(f"{WH}/ads_age_dist")

print(f"[完成] 耗时 {time.time() - t0:.1f} 秒")
spark.stop()
