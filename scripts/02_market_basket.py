from pyspark.sql import SparkSession
from pyspark.sql.functions import col

INPUT_PATH = r"C:\BigData_Project2\output\clean_logs_parquet"
OUTPUT_PATH = r"C:\BigData_Project2\output\item_pairs"

spark = (
    SparkSession.builder
    .appName("Market Basket Analysis")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "16")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(INPUT_PATH)

# Keep only purchased items.
# Each row becomes: session_id, item_id
purchase_df = (
    df
    .filter(col("event_type") == "purchase")
    .select("session_id", "item_id")
    .dropna()
    .dropDuplicates(["session_id", "item_id"])
)

# Self-join purchases within the same session to generate item pairs.
# item_1 < item_2 prevents duplicate pairs like A-B and B-A.
p1 = purchase_df.alias("p1")
p2 = purchase_df.alias("p2")

pairs_df = (
    p1.join(
        p2,
        (col("p1.session_id") == col("p2.session_id")) &
        (col("p1.item_id") < col("p2.item_id")),
        "inner"
    )
    .select(
        col("p1.item_id").alias("item_1"),
        col("p2.item_id").alias("item_2")
    )
)

pair_counts = (
    pairs_df
    .groupBy("item_1", "item_2")
    .count()
    .withColumnRenamed("count", "pair_count")
    .orderBy(col("pair_count").desc())
)

print("Top 20 purchased-together item pairs:")
pair_counts.show(20, truncate=False)

(
    pair_counts
    .write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)

print(f"Done. Item pairs written to: {OUTPUT_PATH}")

spark.stop()