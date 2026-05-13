from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as spark_sum, struct, collect_list, row_number
from pyspark.sql.window import Window

INPUT_PATH = r"C:\BigData_Project2\output\clean_logs_parquet"
OUTPUT_PATH = r"C:\BigData_Project2\output\user_profiles"

spark = (
    SparkSession.builder
    .appName("User Affinity Aggregation")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "16")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(INPUT_PATH)

# Assign weights:
# view = 1, cart = 3, purchase = 5
scored_df = (
    df
    .filter(col("category").isNotNull())
    .withColumn(
        "event_score",
        when(col("event_type") == "view", 1)
        .when(col("event_type") == "cart", 3)
        .when(col("event_type") == "purchase", 5)
        .otherwise(0)
    )
)

category_scores = (
    scored_df
    .groupBy("user_id", "category")
    .agg(spark_sum("event_score").alias("score"))
)

window_spec = Window.partitionBy("user_id").orderBy(col("score").desc())

ranked = (
    category_scores
    .withColumn("rank", row_number().over(window_spec))
    .filter(col("rank") <= 3)
)

user_profiles = (
    ranked
    .groupBy("user_id")
    .agg(
        collect_list(
            struct(
                col("category"),
                col("score")
            )
        ).alias("top_categories")
    )
)

print("Sample user profiles:")
user_profiles.show(20, truncate=False)

(
    user_profiles
    .write
    .mode("overwrite")
    .json(OUTPUT_PATH)
)

print(f"Done. User profiles written to: {OUTPUT_PATH}")

spark.stop()