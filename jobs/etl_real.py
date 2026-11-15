# -*- coding: utf-8 -*-
"""真实数据集 ETL：UCI Online Retail（英国零售商 2010-12 ~ 2011-12 交易明细）
数据规模：约 54 万行、8 个字段
加工链路：xlsx -> CSV(ODS) -> 清洗(DWD) -> 汇总(DWS) -> 报表(ADS)
"""
from pyspark.sql import SparkSession, functions as F
import os, glob, time

BASE = "/home/data/bigdata/jobs"
RAW = f"{BASE}/raw_real"
WH = f"{BASE}/warehouse_real"
CSV = f"{RAW}/online_retail.csv"


def preprocess():
    """xlsx -> csv（pandas 做一次转换，Spark 读 CSV 更快）"""
    if os.path.exists(CSV):
        print("[预处理] CSV 已存在，跳过")
        return
    import pandas as pd
    files = glob.glob(f"{RAW}/*.xlsx")
    if not files:
        raise SystemExit("找不到 xlsx 文件")
    df = pd.read_excel(files[0])
    df.columns = ["invoice_no", "stock_code", "description", "quantity",
                  "invoice_date", "unit_price", "customer_id", "country"]
    df.to_csv(CSV, index=False, encoding="utf-8")
    print(f"[预处理] {len(df)} 行 -> {CSV}")


def main():
    preprocess()
    spark = (SparkSession.builder.appName("online_retail_dw")
             .config("spark.sql.shuffle.partitions", "8")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    t0 = time.time()

    ods = spark.read.csv(CSV, header=True, inferSchema=True)
    ods = ods.withColumn("invoice_date", F.to_timestamp("invoice_date"))
    print(f"[ODS] 原始行数 = {ods.count()}")
    ods.write.mode("overwrite").parquet(f"{WH}/ods_retail")

    # ---- DWD：清洗 ----
    dwd = (ods
           .filter(~F.col("invoice_no").startswith("C"))      # 去掉取消单
           .filter(F.col("quantity") > 0)                      # 去掉负数量
           .filter(F.col("unit_price") > 0)                    # 去掉零/负单价
           .filter(F.col("customer_id").isNotNull())           # 去掉无客户
           .withColumn("amount", F.round(F.col("quantity") * F.col("unit_price"), 2).cast("decimal(14,2)"))
           .withColumn("invoice_day", F.to_date("invoice_date"))
           .withColumn("year_month", F.date_format("invoice_date", "yyyy-MM")))
    print(f"[DWD] 清洗后行数 = {dwd.count()}")
    dwd.write.mode("overwrite").partitionBy("year_month").parquet(f"{WH}/dwd_retail_detail")

    # ---- DWS ----
    dws_month = (dwd.groupBy("year_month")
                 .agg(F.countDistinct("invoice_no").alias("order_cnt"),
                      F.countDistinct("customer_id").alias("uv"),
                      F.sum("quantity").alias("qty"),
                      F.sum("amount").alias("revenue")))
    dws_month.orderBy("year_month").show(20, truncate=False)
    dws_month.write.mode("overwrite").parquet(f"{WH}/dws_month")

    dws_country = (dwd.groupBy("country")
                   .agg(F.countDistinct("invoice_no").alias("order_cnt"),
                        F.sum("amount").alias("revenue"))
                   .filter(F.col("revenue") > 0))
    dws_country.orderBy(F.desc("revenue")).show(10, truncate=False)
    dws_country.write.mode("overwrite").parquet(f"{WH}/dws_country")

    dws_product = (dwd.groupBy("stock_code", "description")
                   .agg(F.sum("quantity").alias("qty"),
                        F.sum("amount").alias("revenue")))
    dws_product.orderBy(F.desc("revenue")).show(10, truncate=False)
    dws_product.write.mode("overwrite").parquet(f"{WH}/dws_product")

    # ---- ADS ----
    total = dwd.agg(F.sum("amount")).collect()[0][0]
    ads_country = (dws_country.withColumn(
        "ratio_pct", F.round(F.col("revenue") / F.lit(total) * 100, 2)))
    ads_country.write.mode("overwrite").parquet(f"{WH}/ads_country_share")

    # RFM：R=最近购买距今天数，F=订单数，M=消费额
    max_day = dwd.agg(F.max("invoice_day")).collect()[0][0]
    rfm = (dwd.groupBy("customer_id")
           .agg(F.datediff(F.lit(max_day), F.max("invoice_day")).alias("recency"),
                F.countDistinct("invoice_no").alias("frequency"),
                F.sum("amount").alias("monetary"))
           .withColumn("segment",
                       F.when((F.col("recency") <= 30) & (F.col("monetary") > 3000), "高价值活跃")
                        .when(F.col("recency") <= 30, "活跃")
                        .when(F.col("monetary") > 3000, "高价值沉睡")
                        .otherwise("一般")))
    rfm.groupBy("segment").agg(F.count("*").alias("users"),
                               F.round(F.sum("monetary"), 2).alias("revenue")).orderBy(F.desc("users")).show(truncate=False)
    rfm.write.mode("overwrite").parquet(f"{WH}/ads_rfm")

    print(f"[完成] 真实数据 ETL 耗时 {time.time() - t0:.1f} 秒")
    spark.stop()


main()
