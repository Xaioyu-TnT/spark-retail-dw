# -*- coding: utf-8 -*-
"""把数仓 ADS/DWS 结果导出到 MySQL，供 BI 工具（Metabase）取数"""
from pyspark.sql import SparkSession
import pandas as pd
from sqlalchemy import create_engine, text

MYSQL = "mysql+pymysql://root:123456@127.0.0.1:3306"
DB = "dw_ads"
V2 = "/home/data/bigdata/jobs/warehouse_v2"
REAL = "/home/data/bigdata/jobs/warehouse_real"

spark = (SparkSession.builder.appName("prep_bi")
         .config("spark.sql.shuffle.partitions", "4").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# 建库
base_engine = create_engine(MYSQL, future=True)
with base_engine.begin() as conn:
    conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {DB} DEFAULT CHARSET utf8mb4"))
engine = create_engine(f"{MYSQL}/{DB}?charset=utf8mb4", future=True)


def export(table, path, transform=None):
    pdf = spark.read.parquet(path).toPandas()
    if transform:
        pdf = transform(pdf)
    pdf.to_sql(table, engine, if_exists="replace", index=False, chunksize=1000)
    print(f"[导出] {table:<22} {len(pdf):>5} 行 x {len(pdf.columns)} 列")
    return pdf


# ---- 电商订单数仓（v2）----
export("daily_gmv", f"{V2}/ads_daily_gmv",
       lambda d: d.sort_values("order_date").reset_index(drop=True))
export("category_rank", f"{V2}/ads_category_rank",
       lambda d: d.sort_values("gmv", ascending=False).reset_index(drop=True))
export("age_dist", f"{V2}/ads_age_dist",
       lambda d: d.sort_values("age_group").reset_index(drop=True))

# ---- 零售交易数仓（real）----
export("month_revenue", f"{REAL}/dws_month",
       lambda d: d.sort_values("year_month").reset_index(drop=True))


def top_country(d):
    d = d.sort_values("revenue", ascending=False).head(12).reset_index(drop=True)
    total = d["revenue"].sum()
    d["ratio_pct"] = (d["revenue"] / total * 100).round(2)
    return d


export("country_revenue", f"{REAL}/dws_country", top_country)


def rfm_segment(d):
    g = (d.groupby("segment", as_index=False)
          .agg(users=("customer_id", "count"), revenue=("monetary", "sum")))
    g["revenue"] = g["revenue"].round(2)
    return g.sort_values("revenue", ascending=False).reset_index(drop=True)


export("rfm_segment", f"{REAL}/ads_rfm", rfm_segment)

# 汇总校验
with engine.connect() as conn:
    rows = conn.execute(text("SHOW TABLES")).fetchall()
    print("\n[MySQL] dw_ads 现有表:", [r[0] for r in rows])
    for t in [r[0] for r in rows]:
        n = conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
        print(f"  {t:<18} {n} 行")

spark.stop()
