from pyspark.sql.functions import countDistinct, count
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext.getOrCreate()
spark = SparkSession.builder.getOrCreate()



SILVER_BASE = "s3://steam-github-sync-bucket/silver/"
GOLD_BASE   = "s3://steam-github-sync-bucket/gold/"

applications_path = f"{SILVER_BASE}/applications/bi_applications_capped/parquet/"

devs_path       = f"{SILVER_BASE}/dimensions/app_developers/"
publishers_path = f"{SILVER_BASE}/dimensions/app_publishers/"
genres_path     = f"{SILVER_BASE}/dimensions/app_genres/"
categories_path = f"{SILVER_BASE}/dimensions/app_categories/"
platforms_path  = f"{SILVER_BASE}/dimensions/app_platforms/"

master_out_parquet = f"{GOLD_BASE}/masterdata/parquet/"
master_out_csv     = f"{GOLD_BASE}/masterdata/csv/"


applications_df = spark.read.parquet(applications_path)



applications_df.select(
    countDistinct("appid").alias("distinct_appids"),
    count("*").alias("total_rows")
).show()

app_devs_df       = spark.read.parquet(devs_path)
app_publishers_df = spark.read.parquet(publishers_path)
app_genres_df     = spark.read.parquet(genres_path)
app_categories_df = spark.read.parquet(categories_path)
app_platforms_df  = spark.read.parquet(platforms_path)

master_df = applications_df.join(app_devs_df, on="appid", how="left")
master_df = master_df.join(app_publishers_df, on="appid", how="left")
master_df = master_df.join(app_genres_df, on="appid", how="left")
master_df = master_df.join(app_categories_df, on="appid", how="left")
master_df = master_df.join(app_platforms_df, on="appid", how="left")


master_df.select(
    countDistinct("appid").alias("distinct_appids"),
    count("*").alias("total_rows")
).show()


master_df.write.mode("overwrite").parquet(master_out_parquet)


master_df.coalesce(1) \
    .write.mode("overwrite") \
    .option("header", "true") \
    .csv(master_out_csv)
