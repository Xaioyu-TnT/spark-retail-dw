# -*- coding: utf-8 -*-
"""校验 v2 仓库：分区数、增量覆盖范围、金额格式"""
from pyspark.sql import SparkSession

WH = "/home/data/bigdata/jobs/warehouse_v2"
s = SparkSession.builder.appName("chk_v2").getOrCreate()
s.sparkContext.setLogLevel("ERROR")

d = s.read.parquet(f"{WH}/ads_daily_gmv")
print("ads_daily_gmv 行数 =", d.count())
print("日期范围 =", d.agg({"order_date": "min"}).collect()[0][0], "~", d.agg({"order_date": "max"}).collect()[0][0])
print("\n2026-01-15 当天：")
d.filter(d.order_date == "2026-01-15").show(truncate=False)

dw = s.read.parquet(f"{WH}/dwd_order_detail")
print("dwd_order_detail 行数 =", dw.count())
print("schema(amount) =", dict(dw.dtypes)["amount"])
print("schema(age_group) =", dict(dw.dtypes)["age_group"])
s.stop()
