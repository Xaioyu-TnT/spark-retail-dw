# -*- coding: utf-8 -*-
"""SQL 练习执行器：读取 .sql 文件 → 在数仓上执行 → 打印结果

用法：
    python jobs/sql_practice/run_sql.py retention_d1        # 简单名
    python jobs/sql_practice/run_sql.py q1_group_topn.sql   # 或带后缀
    python jobs/sql_practice/run_sql.py retention_d1 -n 10  # 只显示 10 行

约定：
    · SQL 从 warehouse_v2 读表（dwd_order_detail / dws_* / ads_*）
    · 结果只打印，不落盘；要落盘用 --save 表名
"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

BASE = "/home/data/bigdata/jobs"
WH = f"{BASE}/warehouse_v2"
SQL_DIR = os.path.join(BASE, "sql_practice")


def load_sql(name):
    if not name.endswith(".sql"):
        name += ".sql"
    path = name if os.path.isabs(name) else os.path.join(SQL_DIR, name)
    if not os.path.exists(path):
        raise SystemExit(f"找不到 SQL 文件: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return path, f.read()


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    if not args:
        raise SystemExit("用法: python run_sql.py <sql文件名> [--save 表名] [-n 行数]")
    limit = 20
    if "-n" in sys.argv:
        limit = int(sys.argv[sys.argv.index("-n") + 1])
    save = None
    if "--save" in sys.argv:
        save = sys.argv[sys.argv.index("--save") + 1]

    path, sql = load_sql(args[0])

    spark = (SparkSession.builder.appName("sql_practice")
             .config("spark.sql.shuffle.partitions", "8")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    # 把数仓目录注册成临时视图，SQL 里就能直接写表名
    for t in ("dwd_order_detail", "dws_category_day", "dws_user_day",
              "dws_age_day", "ads_daily_gmv", "ads_category_rank", "ads_age_dist"):
        d = os.path.join(WH, t)
        if os.path.exists(d):
            spark.read.parquet(d).createOrReplaceTempView(t)

    print(f"\n{'=' * 70}")
    print(f"执行: {os.path.basename(path)}")
    print(f"{'=' * 70}")
    try:
        df = spark.sql(sql)
    except AnalysisException as e:
        print("SQL 报错：")
        print(str(e)[:1200])
        spark.stop()
        sys.exit(1)

    df.show(limit, truncate=False)
    print(f"结果行数: {df.count()}")

    if save:
        out = os.path.join(WH, save)
        df.write.mode("overwrite").parquet(out)
        print(f"已落盘: {out}")

    spark.stop()


main()
