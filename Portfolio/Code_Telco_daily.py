import pyspark
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col, lpad
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import StringType


partition_date = sys.argv[1] #YYYY-MM-DD

ld_year = partition_date[0:4]
ld_month = partition_date[5:7]
ld_day = partition_date[8:10]

spark = (
    SparkSession.builder
    .appName("agg_mobile_app_category_daily")
    .getOrCreate()
)

# /data/curated/streaming/mobile_app_daily

INPUT_PATH = '/data/curated/streaming/mobile_app_daily/'
#df = spark.read.parquet(INPUT_PATH).where(f.col("partition_date") == partition_date)
#INPUT_PATH  = '/data/curated/streaming/mobile_app_daily'


# 1) read Parquet
mobile_app_daily = spark.read.parquet(INPUT_PATH).where(f.col("partition_date") == partition_date)

#mobile_app_daily = spark.read.parquet(INPUT_PATH)

# 2) 
mobile_app_daily = mobile_app_daily.where(f.col("count_trans")  > 0)
mobile_app_daily = mobile_app_daily.where(f.col("duration")     > 0)
mobile_app_daily = mobile_app_daily.where(f.col("total_byte")   > 0)
mobile_app_daily = mobile_app_daily.where(f.col("download_byte")> 0)
mobile_app_daily = mobile_app_daily.where(f.col("upload_byte")  > 0)

# 3) 
mobile_app_daily_cat_lv_1 = mobile_app_daily.withColumn('category_name', mobile_app_daily.category_level_1)
mobile_app_daily_cat_lv_1 = mobile_app_daily_cat_lv_1.withColumn("category_level", f.lit("level_1").cast(StringType()))
mobile_app_daily_cat_lv_2 = mobile_app_daily.withColumn('category_name', mobile_app_daily.category_level_2)
mobile_app_daily_cat_lv_2 = mobile_app_daily_cat_lv_2.withColumn("category_level", f.lit("level_2").cast(StringType()))
mobile_app_daily_cat_lv_3 = mobile_app_daily.withColumn('category_name', mobile_app_daily.category_level_3)
mobile_app_daily_cat_lv_3 = mobile_app_daily_cat_lv_3.withColumn("category_level", f.lit("level_3").cast(StringType()))
mobile_app_daily_cat_lv_4 = mobile_app_daily.withColumn('category_name', mobile_app_daily.category_level_4)
mobile_app_daily_cat_lv_4 = mobile_app_daily_cat_lv_4.withColumn("category_level", f.lit("level_4").cast(StringType()))

mobile_app_daily = mobile_app_daily_cat_lv_1.union(mobile_app_daily_cat_lv_2)
mobile_app_daily = mobile_app_daily.union(mobile_app_daily_cat_lv_3)
mobile_app_daily = mobile_app_daily.union(mobile_app_daily_cat_lv_4)

# 4) 
mobile_app_daily = mobile_app_daily.withColumn("priority", f.lit(None).cast(StringType()))
if "partition_date" in mobile_app_daily.columns:
    mobile_app_daily = mobile_app_daily.withColumnRenamed('partition_date', 'event_partition_date')
# 
if "event_partition_date" not in mobile_app_daily.columns:
    mobile_app_daily = mobile_app_daily.withColumn("event_partition_date", f.lit(None).cast(StringType()))

# 5) Aggregate 
#   granularity: mobile_no, subscription_identifier, category_level, category_name, priority, event_partition_date
group_cols = [
    "mobile_no",
    "subscription_identifier",
    "category_level",
    "category_name",
    "priority",
    "event_partition_date"
]

agg_df = (
    mobile_app_daily
    .groupBy(*group_cols)
    .agg(
        f.sum("count_trans").alias("total_visit_count"),
        f.sum("duration").alias("total_visit_duration"),
        f.sum("total_byte").alias("total_volume_byte"),
        f.sum("download_byte").alias("total_download_byte"),
        f.sum("upload_byte").alias("total_upload_byte"),
    )
)


# /projects/prod/c360/data/DIGITAL/l1_features/l1_digital_customer_app_category_agg_daily

# 6) write (Parquet)  partition 
OUTPUT_PATH = '/projects/prod/digital/l1_digital_customer_app_category_agg_daily/'
agg_df.coalesce(200).write.mode("overwrite").partitionBy("event_partition_date").parquet(OUTPUT_PATH)



