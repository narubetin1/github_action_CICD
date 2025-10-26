#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.functions import col, lit, when, concat
from pyspark.sql.types import StringType

# ========= 0) Args & Validation =========
#if len(sys.argv) < 2:
#    raise SystemExit("❌ ต้องส่งวันที่แบบ yyyymmdd เป็นอาร์กิวเมนต์แรก เช่น: spark-submit job.py 20251001")

#
#if not (len(date) == 8 and date.isdigit()):
#    raise SystemExit("❌ รูปแบบวันที่ไม่ถูกต้อง ต้องเป็น yyyymmdd เช่น 20251001")


date = sys.argv[1]
partition_date = date[0:8]   # yyyymmdd
ld_year = date[0:4]
ld_month = date[4:6]
ld_day = date[6:8]

# ========= 1) Spark =========
spark = SparkSession.builder.appName("digital_mobile_app_category_timeband_agg").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "200")  # ปรับตามคลัสเตอร์

# ========= 2) Paths (แก้ให้ตรงกับของคุณ) =========
INPUT_TIMEBAND_PATH = "s3://your-bucket/input/mobile_app_timeband/"
INPUT_DAILY_PATH    = "s3://your-bucket/input/mobile_app_daily/"
INPUT_CATMASTER_PATH= "s3://your-bucket/input/app_categories_master/"
TARGET_PATH         = "s3://your-bucket/output/mobile_app_category_timeband/"

# ========= 3) Load =========
df_timeband = spark.read.parquet(INPUT_TIMEBAND_PATH)
df_daily    = spark.read.parquet(INPUT_DAILY_PATH)
df_cat      = spark.read.parquet(INPUT_CATMASTER_PATH)

# ========= 4) Filter by partition date from starttime =========
# สมมติ starttime เป็นสตริงรูปแบบ yyyymmddHHMMSS
df_timeband = df_timeband.filter(df_timeband["starttime"].substr(1, 8) == partition_date)

# ========= 5) Cleansing & Timeband =========
df_timeband = (
    df_timeband
    .withColumnRenamed("ul_kbyte", "ul_byte")
    .filter((col("dw_byte") > 0) & (col("ul_byte") > 0) & (col("time_cnt") > 0) & (col("duration_sec") > 0))
    .withColumn(
        "timeband",
        when((col("ld_hour") >= 6) & (col("ld_hour") <= 11), "Morning")
        .when((col("ld_hour") >= 12) & (col("ld_hour") <= 17), "Afternoon")
        .when((col("ld_hour") >= 18) & (col("ld_hour") <= 23), "Evening")
        .otherwise("Night")
    )
)

# ========= 6) Join กับ Category Master =========
df_joined = df_timeband.join(
    f.broadcast(df_cat),
    on=(df_timeband.application == df_cat.application_id),
    how="inner"
)

# ========= 7) สร้าง category 4 levels และ union =========
dfs = []
for level in ["level_1", "level_2", "level_3", "level_4"]:
    dfs.append(
        df_joined
        .withColumn("category_name", col(level))
        .withColumn("category_level", lit(level).cast(StringType()))
    )

df_union = dfs[0].unionByName(dfs[1]).unionByName(dfs[2]).unionByName(dfs[3])

# ========= 8) event_partition_date & mobile_no =========
df_union = (
    df_union
    .withColumn("mobile_no", col("msisdn"))
    .withColumn(
        "event_partition_date",
        concat(col("starttime").substr(1, 4), lit("-"), col("starttime").substr(5, 2), lit("-"), col("starttime").substr(7, 2))
    )
)

# ========= 9) AGGREGATE ตามที่กำหนด =========
# l1_digital_mobile_app_agg_category_timeband:
#   feature_list:
#     total_visit_count      = sum(time_cnt)
#     total_visit_duration   = sum(duration_sec)
#     total_volume_byte      = sum(dw_byte + ul_byte)
#     total_download_byte    = sum(dw_byte)
#     total_upload_byte      = sum(ul_byte)
#   granularity:
#     mobile_no, category_level, category_name, timeband, event_partition_date

agg_group_cols = ["mobile_no", "category_level", "category_name", "timeband", "event_partition_date"]

df_agg = (
    df_union
    .groupBy(*[col(c) for c in agg_group_cols])
    .agg(
        f.sum(col("time_cnt")).alias("total_visit_count"),
        f.sum(col("duration_sec")).alias("total_visit_duration"),
        f.sum(col("dw_byte") + col("ul_byte")).alias("total_volume_byte"),
        f.sum(col("dw_byte")).alias("total_download_byte"),
        f.sum(col("ul_byte")).alias("total_upload_byte"),
        # ถ้าต้องการคงค่า priority ใส่ไว้ด้วย (เลือก first/ max ตามที่เหมาะสม)
        f.first(col("priority"), ignorenulls=True).alias("priority")
    )
)

# ========= 10) เตรียม df_daily และ JOIN ใส่คอลัมน์ daily (ออปชัน) =========
# ถ้าไม่ต้องการคอลัมน์รายวัน บล็อกนี้สามารถตัดออกได้
df_daily_renamed = (
    df_daily
    .withColumnRenamed("total_visit_count", "total_visit_count_daily")
    .withColumnRenamed("total_visit_duration", "total_visit_duration_daily")
    .withColumnRenamed("total_volume_byte", "total_volume_byte_daily")
    .withColumnRenamed("total_download_byte", "total_download_byte_daily")
    .withColumnRenamed("total_upload_byte", "total_upload_byte_daily")
)

# df_daily ควรมี mobile_no, category_name, event_partition_date (ฟอร์แมต yyyy-mm-dd ให้ตรงกัน)
df_final = df_agg.join(
    df_daily_renamed,
    on=[
        df_agg.mobile_no == df_daily_renamed.mobile_no,
        df_agg.category_name == df_daily_renamed.category_name,
        df_agg.event_partition_date == df_daily_renamed.event_partition_date
    ],
    how="inner"  # ถ้าต้องการเก็บทุกแถวจาก agg ให้เปลี่ยนเป็น "left"
)

# ========= 11) Select คอลัมน์สุดท้าย =========
# subscription_identifier คาดว่ามาจาก df_daily
df_output = df_final.select(
    "subscription_identifier",
    df_final["mobile_no"],
    "category_level",
    "category_name",
    "priority",
    "total_visit_count",
    "total_visit_duration",
    "total_volume_byte",
    "total_download_byte",
    "total_upload_byte",
    "total_visit_count_daily",
    "total_visit_duration_daily",
    "total_volume_byte_daily",
    "total_download_byte_daily",
    "total_upload_byte_daily",
    "timeband",
    df_final["event_partition_date"]
)

# ========= 12) Write Target =========
(
    df_output
    .write
    .mode("overwrite")                         # เปลี่ยนเป็น "append" ได้
    .partitionBy("event_partition_date")       # สำหรับประสิทธิภาพการอ่าน
    .parquet(TARGET_PATH)
)

spark.stop()
