# -*- coding: utf-8 -*-
"""v2 全量构建：decimal 金额 + 年龄段维度，写入 warehouse_v2"""
from pyspark.sql import SparkSession, functions as F
import time

RAW = "/home/data/bigdata/jobs/raw"
WH = "/home/data/bigdata/jobs/warehouse_v2"

spark = (SparkSession.builder.appName("star_dw_v2_full")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")


def rd(n):
    return spark.read.csv(f"{RAW}/{n}.csv", header=True, inferSchema=True)


t0 = time.time()
orders = rd("orders").withColumn("order_date", F.to_date("order_date"))
items, users, products, regions = rd("order_items"), rd("users"), rd("products"), rd("region")

orders.write.mode("overwrite").partitionBy("order_date").parquet(f"{WH}/ods_orders")
items.join(orders.select("order_id", "order_date"), "order_id") \
     .write.mode("overwrite").partitionBy("order_date").parquet(f"{WH}/ods_order_items")
for n, d in [("users", users), ("products", products), ("region", regions)]:
    d.write.mode("overwrite").parquet(f"{WH}/ods_{n}")

dwd = (items.alias("i")
       .join(orders.alias("o"), "order_id")
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
print(f"[DWD] 行数 = {dwd.count()}")
dwd.write.mode("overwrite").partitionBy("order_date").parquet(f"{WH}/dwd_order_detail")

dws_cat = (dwd.groupBy("order_date", "category")
           .agg(F.countDistinct("order_id").alias("order_cnt"),
                F.sum("quantity").alias("qty"), F.sum("amount").alias("amount")))
dws_cat.write.mode("overwrite").partitionBy("order_date").parquet(f"{WH}/dws_category_day")

dws_age = (dwd.groupBy("order_date", "age_group")
           .agg(F.countDistinct("order_id").alias("order_cnt"), F.sum("amount").alias("amount")))
dws_age.write.mode("overwrite").partitionBy("order_date").parquet(f"{WH}/dws_age_day")

dws_user = (dwd.groupBy("order_date", "user_id")
            .agg(F.countDistinct("order_id").alias("order_cnt"), F.sum("amount").alias("amount")))
dws_user.write.mode("overwrite").partitionBy("order_date").parquet(f"{WH}/dws_user_day")

ads_daily = (dwd.groupBy("order_date")
             .agg(F.countDistinct("order_id").alias("order_cnt"),
                  F.countDistinct("user_id").alias("uv"),
                  F.sum("amount").alias("gmv")))
ads_daily.orderBy("order_date").show(3, False)
ads_daily.write.mode("overwrite").partitionBy("order_date").parquet(f"{WH}/ads_daily_gmv")

ads_cat = dws_cat.groupBy("category").agg(F.sum("amount").alias("gmv"))
total = ads_cat.agg(F.sum("gmv")).collect()[0][0]
ads_cat = ads_cat.withColumn("ratio_pct", F.round(F.col("gmv") / F.lit(total) * 100, 2))
ads_cat.orderBy(F.desc("gmv")).show(truncate=False)
ads_cat.write.mode("overwrite").parquet(f"{WH}/ads_category_rank")

ads_age = dws_age.groupBy("age_group").agg(F.sum("amount").alias("gmv"),
                                           F.sum("order_cnt").alias("order_cnt"))
ads_age.orderBy("age_group").show(truncate=False)
ads_age.write.mode("overwrite").parquet(f"{WH}/ads_age_dist")

print(f"[完成] 全量构建耗时 {time.time() - t0:.1f} 秒")
spark.stop()
