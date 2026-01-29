import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    countDistinct,
    count,
    from_unixtime,
    to_date,
    year,
    month,
    date_format,
    col,
    when
)


sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


RAW_BASE = "s3://steam-github-sync-bucket/bronze/steam-csv"
SILVER_BASE = "s3://steam-github-sync-bucket/silver/reviews/"

reviews_input = f"{RAW_BASE}/reviews.csv"

review_out_parquet = f"{SILVER_BASE}/fact_review_level/parquet/"
review_out_csv     = f"{SILVER_BASE}/fact_review_level/csv/"


reviews_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")   # VERY IMPORTANT
    .option("quote", "\"")
    .option("escape", "\"")
    .option("mode", "PERMISSIVE")
    .option("encoding", "UTF-8")
    .load(reviews_input)
)

bi_reviews_df = reviews_df.select(
    "recommendationid",
    "appid",

    "votes_up",
    "votes_funny",
    "comment_count",
    "weighted_vote_score",

    "author_playtime_at_review",
    "author_playtime_forever",
    "author_playtime_last_two_weeks",
    "author_num_games_owned",
    "author_num_reviews",

    "steam_purchase",
    "received_for_free",
    "written_during_early_access",

    "language",
    "timestamp_created"
)



bi_reviews_df.select(
    countDistinct("recommendationid").alias("distinct_reviews"),
    count("*").alias("total_rows")
).show()


from pyspark.sql.functions import when, col

bi_reviews_capped_df = (
    bi_reviews_df
    .withColumn(
        "author_playtime_forever_capped",
        when(col("author_playtime_forever") > 26026, 26026)
        .otherwise(col("author_playtime_forever"))
    )
    .withColumn(
        "author_playtime_at_review_capped",
        when(col("author_playtime_at_review") > 22677, 22677)
        .otherwise(col("author_playtime_at_review"))
    )
    .withColumn(
        "author_playtime_last_two_weeks_capped",
        when(col("author_playtime_last_two_weeks") > 2673, 2673)
        .otherwise(col("author_playtime_last_two_weeks"))
    )
    .withColumn(
        "votes_up_capped",
        when(col("votes_up") > 49, 49)
        .otherwise(col("votes_up"))
    )
    .withColumn(
        "votes_funny_capped",
        when(col("votes_funny") > 10, 10)
        .otherwise(col("votes_funny"))
    )
    .withColumn(
        "comment_count_capped",
        when(col("comment_count") > 4, 4)
        .otherwise(col("comment_count"))
    )
)



review_fact_df = (
    bi_reviews_capped_df
    .withColumn("review_timestamp", from_unixtime(col("timestamp_created")))
    .withColumn("review_date", to_date(col("review_timestamp")))
    .withColumn("review_year", year(col("review_timestamp")))
    .withColumn("review_month", month(col("review_timestamp")))
    .withColumn("review_year_month", date_format(col("review_timestamp"), "yyyy-MM"))
)

review_fact_df.write.mode("overwrite").parquet(review_out_parquet)

review_fact_df.coalesce(1) \
    .write.mode("overwrite") \
    .option("header", "true") \
    .csv(review_out_csv)
