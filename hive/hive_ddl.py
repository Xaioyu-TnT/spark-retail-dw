# -*- coding: utf-8 -*-
"""Hive 外表与分区表演示：用 Spark 内置 Hive 支持（Derby 元数据库）跑真实 DDL

覆盖的语法点：
    1) CREATE EXTERNAL TABLE  —— 外部表，删表不删数据
    2) PARTITIONED BY         —— 分区字段不写进列定义
    3) STORED AS PARQUET + LOCATION  —— 直接指向已有的 parquet 目录
    4) MSCK REPAIR TABLE      —— 自动登记目录下的分区
    5) SHOW PARTITIONS / DESCRIBE FORMATTED / 分区裁剪查询

运行：
    source ~/sparkenv/bin/activate
    python hive/hive_ddl.py
"""
import os
import shutil
from pyspark.sql import SparkSession

BASE = "/home/data/bigdata/jobs"
LOC = f"{BASE}/warehouse_v2/dwd_order_detail"          # 已有的 parquet 目录
META = f"{BASE}/hive_meta"                             # Derby 元数据目录

# Derby 只允许单连接，重复运行前清掉旧元数据，避免锁冲突
if os.path.exists(META):
    shutil.rmtree(META, ignore_errors=True)

spark = (SparkSession.builder
         .appName("hive_ddl_demo")
         .config("spark.sql.warehouse.dir", f"{BASE}/hive_warehouse")
         .config("spark.sql.catalogImplementation", "hive")
         .config("javax.jdo.option.ConnectionURL",
                 f"jdbc:derby:;databaseName={META}/metastore_db;create=true")
         .enableHiveSupport()
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")


def run(sql, title=None, limit=10):
    if title:
        print(f"\n=== {title} ===")
    print("SQL>", " ".join(sql.split()))
    df = spark.sql(sql)
    if df.columns:
        df.show(limit, truncate=False)
    return df


print("=" * 78)
print("Hive 外表 / 分区表 DDL 演示")
print("=" * 78)

# 1) 建外部表：注意 order_date 只出现在 PARTITIONED BY 里
spark.sql("DROP TABLE IF EXISTS dwd_order_detail_ext")
spark.sql("""
CREATE EXTERNAL TABLE dwd_order_detail_ext (
    order_id      INT,
    item_id       INT,
    user_id       INT,
    product_id    INT,
    category      STRING,
    region_name   STRING,
    order_status  STRING,
    quantity      INT,
    unit_price    DOUBLE,
    amount        DECIMAL(12,2),
    age_group     STRING
)
PARTITIONED BY (order_date STRING)
STORED AS PARQUET
LOCATION '/home/data/bigdata/jobs/warehouse_v2/dwd_order_detail'
""")
print("\n[1] 外部表已创建，指向 parquet 目录：")
run("DESCRIBE FORMATTED dwd_order_detail_ext", limit=40)

# 2) 登记分区（外部表建完后分区是空的，必须 REPAIR）
print("\n[2] 建表后先看分区（应为空）：")
run("SHOW PARTITIONS dwd_order_detail_ext", limit=5)

spark.sql("MSCK REPAIR TABLE dwd_order_detail_ext")
print("\n[3] MSCK REPAIR 之后的分区数：")
n = spark.sql("SHOW PARTITIONS dwd_order_detail_ext").count()
print(f"    分区数 = {n}")
run("SHOW PARTITIONS dwd_order_detail_ext", limit=5)

# 3) 查询：全表 vs 单分区（体现分区裁剪）
total = spark.sql("SELECT count(*) AS cnt FROM dwd_order_detail_ext").collect()[0]["cnt"]
one = spark.sql("""
SELECT count(*) AS cnt, round(sum(amount),2) AS gmv
FROM dwd_order_detail_ext
WHERE order_date = '2026-01-15'
""").collect()[0]
print(f"\n[4] 全表 {total:,} 行；单分区 2026-01-15 {one['cnt']:,} 行 / GMV {one['gmv']}")

print("\n[5] 分区裁剪的执行计划（PartitionFilters 非空即生效）：")
spark.sql("""
EXPLAIN SELECT count(*) FROM dwd_order_detail_ext WHERE order_date = '2026-01-15'
""").show(truncate=False)

# 4) 聚合：证明表可正常当数仓表用
print("\n[6] 用 Hive 表做聚合：")
run("""
SELECT age_group, count(DISTINCT order_id) AS orders, round(sum(amount),2) AS gmv
FROM dwd_order_detail_ext
GROUP BY age_group
ORDER BY gmv DESC
""", limit=10)

# 5) 外部表的特性：删表只删元数据
print("\n[7] 外部表特性验证：")
before = sum(len(files) for _, _, files in os.walk(LOC))
spark.sql("DROP TABLE dwd_order_detail_ext")
after = sum(len(files) for _, _, files in os.walk(LOC))
print(f"    DROP TABLE 前文件数 = {before}，之后 = {after}")
print(f"    数据目录依然存在：{os.path.exists(LOC)}")
print("    => 外部表只删元数据，不删 HDFS/本地数据")

spark.stop()
print("\n完成。")
