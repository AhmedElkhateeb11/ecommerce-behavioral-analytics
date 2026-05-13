from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object, to_timestamp

RAW_PATH = r"C:\BigData_Project2\data\raw_logs.csv"
OUTPUT_PATH = r"C:\BigData_Project2\output\clean_logs_parquet"

spark = (
    SparkSession.builder
    .appName("Clean Ecommerce Logs")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "16")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

raw_df = (
    spark.read
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .option("quote", '"')
    .option("escape", '"')
    .csv(RAW_PATH)
)

clean_df = (
    raw_df
    .select(
        to_timestamp(col("timestamp")).alias("event_timestamp"),
        col("session_id"),
        col("user_id"),
        col("event_type"),
        col("product_id").alias("item_id"),
        col("price").cast("double").alias("price"),
        col("referrer"),
        get_json_object(col("user_metadata"), "$.device").alias("device"),
        get_json_object(col("user_metadata"), "$.tier").alias("user_tier"),
        get_json_object(col("user_metadata"), "$.loc").alias("user_location"),
        get_json_object(col("product_metadata"), "$.category").alias("category"),
        get_json_object(col("product_metadata"), "$.brand").alias("brand"),
        get_json_object(col("product_metadata"), "$.stock").cast("int").alias("stock")
    )
    .filter(col("session_id").isNotNull())
    .filter(col("user_id").isNotNull())
    .filter(col("event_type").isin("view", "cart", "purchase"))
    .filter(col("item_id").isNotNull())
)

print("Clean schema:")
clean_df.printSchema()

print("Sample rows:")
clean_df.show(10, truncate=False)

print("Clean row count:")
print(clean_df.count())

print("Writing clean Parquet dataset...")
(
    clean_df
    .repartition(16)
    .write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)

print(f"Done. Clean data written to: {OUTPUT_PATH}")

spark.stop()