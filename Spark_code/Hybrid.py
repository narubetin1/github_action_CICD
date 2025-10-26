#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.functions import col, lit, when, concat
from pyspark.sql.types import StringType

# ========= 0) Args & Validation =========
if len(sys.argv) < 2:
    raise SystemExit("❌ ต้องส่งวันที่แบบ yyyymmdd เช่น: spark-submit job.py 20251001")

date = sys.argv[1]
if not (len(date) == 8 and date.isdigit()):
    raise SystemExit("❌ รูปแบบวันที่ไม่ถูกต้อง ต้องเป็น yyyymmdd เช่น 20251001")

partition_date = date  # ใช้ทั้ง 8 ตัว (yyyymmdd)

# ========= 1) Spark =========
spark = SparkSession.builder.appName("digital_mobile_app_category_timeband_hybrid").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "200")

# ========= 2) Paths =========
INPUT_TIMEBAND_PATH  = "s3://your-bucket/input/mobile_app_timeband/"
INPUT_DAILY_PATH     = "s3://your-bucket/input/mobile_app_daily/"
INPUT_CATMASTER_PATH = "s3://your-bucket/input/app_categories_master/"
TARGET_PATH          = "s3://your-bucket/output/mobile_app_category_timeband/"

# ========= 3) Load =========
df_timeband = spark.read.parquet(INPUT_TIMEBAND_PATH)
df_daily    = spark.read.parquet(INPUT_DAILY_PATH)
df_cat      = spark.read.parquet(INPUT_CATMASTER_PATH)

# ========= 4) Cleansing + Timeband (DataFrame API) =========
df_timeband = (
    df_timeband
    .withColumnRenamed("ul_kbyte", "ul_byte")
    .filter(df_timeband["starttime"].substr(1, 8) == partition_date)
    .filter((col("dw_byte") > 0) & (col("ul_byte") > 0) & (col("time_cnt") > 0) & (col("duration_sec") > 0))
    .withColumn(
        "timeband",
        when((col("ld_hour") >= 6) & (col("ld_hour") <= 11), "Morning")
        .when((col("ld_hour") >= 12) & (col("ld_hour") <= 17), "Afternoon")
        .when((col("ld_hour") >= 18) & (col("ld_hour") <= 23), "Evening")
        .otherwise("Night")
    )
    .withColumn("mobile_no", col("msisdn"))
    .withColumn(
        "event_partition_date",
        concat(col("starttime").substr(1, 4), lit("-"),
               col("starttime").substr(5, 2), lit("-"),
               col("starttime").substr(7, 2))
    )
)

# ========= 5) Join กับ Category Master =========
df_joined = df_timeband.join(
    f.broadcast(df_cat),
    df_timeband.application == df_cat.application_id,
    "inner"
)

# ========= 6) Union Category Levels =========
dfs = []
for level in ["level_1", "level_2", "level_3", "level_4"]:
    dfs.append(
        df_joined
        .withColumn("category_name", col(level))
        .withColumn("category_level", lit(level).cast(StringType()))
    )
df_union = dfs[0].unionByName(dfs[1]).unionByName(dfs[2]).unionByName(dfs[3])

# ========= 7) Register Temp Views สำหรับ SQL =========
df_union.createOrReplaceTempView("unioned")
df_daily.createOrReplaceTempView("mobile_app_daily")

# ========= 8) Aggregate + Join (Spark SQL) =========
query = """
WITH agg AS (
  SELECT
    mobile_no,
    category_level,
    category_name,
    timeband,
    event_partition_date,
    SUM(time_cnt)              AS total_visit_count,
    SUM(duration_sec)          AS total_visit_duration,
    SUM(dw_byte + ul_byte)     AS total_volume_byte,
    SUM(dw_byte)               AS total_download_byte,
    SUM(ul_byte)               AS total_upload_byte,
    FIRST(priority, TRUE)      AS priority
  FROM unioned
  GROUP BY mobile_no, category_level, category_name, timeband, event_partition_date
)
SELECT
  d.subscription_identifier,
  a.mobile_no,
  a.category_level,
  a.category_name,
  a.priority,
  a.total_visit_count,
  a.total_visit_duration,
  a.total_volume_byte,
  a.total_download_byte,
  a.total_upload_byte,
  d.total_visit_count         AS total_visit_count_daily,
  d.total_visit_duration      AS total_visit_duration_daily,
  d.total_volume_byte         AS total_volume_byte_daily,
  d.total_download_byte       AS total_download_byte_daily,
  d.total_upload_byte         AS total_upload_byte_daily,
  a.timeband,
  a.event_partition_date
FROM agg a
INNER JOIN mobile_app_daily d
  ON a.mobile_no = d.mobile_no
 AND a.category_name = d.category_name
 AND a.event_partition_date = d.event_partition_date
"""

df_output = spark.sql(query)

# ========= 9) Write =========
(
    df_output
    .write
    .mode("overwrite")
    .partitionBy("event_partition_date")
    .parquet(TARGET_PATH)
)

spark.stop()
