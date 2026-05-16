from pyspark.sql import SparkSession
from pyspark.sql.functions import col

BASE = r"C:\BigData_Project2\output"

CLEAN_PATH = BASE + r"\clean_logs_parquet"
ITEM_PAIRS_PATH = BASE + r"\item_pairs"
USER_PROFILES_PATH = BASE + r"\user_profiles"
CAMPAIGN_OUTPUT_PATH = BASE + r"\campaign_output"

spark = (
    SparkSession.builder
    .appName("Verify Generated Project Outputs")
    .master("local[2]")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("=" * 80)
print("BIG DATA PROJECT - GENERATED OUTPUT VERIFICATION")
print("=" * 80)

print("\n[1] CLEAN LOGS OUTPUT")
clean_df = spark.read.parquet(CLEAN_PATH)
print("Clean output path:", CLEAN_PATH)
print("Clean row count:", clean_df.count())
print("Clean schema:")
clean_df.printSchema()
print("Clean sample rows:")
clean_df.show(10, truncate=False)

print("\n[2] MARKET BASKET ANALYSIS OUTPUT")
item_pairs = spark.read.parquet(ITEM_PAIRS_PATH)
print("Item pairs output path:", ITEM_PAIRS_PATH)
print("Top 20 purchased-together item pairs:")
item_pairs.orderBy(col("pair_count").desc()).show(20, truncate=False)

print("\n[3] USER AFFINITY OUTPUT")
profiles = spark.read.json(USER_PROFILES_PATH)
print("User profiles output path:", USER_PROFILES_PATH)
print("Sample user profiles:")
profiles.show(20, truncate=False)

print("\n[4] CART ABANDONMENT CAMPAIGN OUTPUT")
campaign = spark.read.parquet(CAMPAIGN_OUTPUT_PATH)
print("Campaign output path:", CAMPAIGN_OUTPUT_PATH)
print("Campaign type counts:")
campaign.groupBy("campaign_type").count().show(truncate=False)
print("Campaign sample:")
campaign.show(20, truncate=False)

print("\n" + "=" * 80)
print("VERIFICATION COMPLETE - NO HEAVY PIPELINE WAS RERUN")
print("=" * 80)

spark.stop()