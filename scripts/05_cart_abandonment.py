import json
import os
import shutil
import sys
from pathlib import Path

from pymongo import MongoClient

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, expr, lit, when

# Force Spark to use this virtual environment's Python
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

CLEAN_LOGS_PATH = r"C:\BigData_Project2\output\clean_logs_parquet"
PROFILES_STAGE_PATH = r"C:\BigData_Project2\output\mongo_profiles_for_spark"
CAMPAIGN_OUTPUT_PATH = r"C:\BigData_Project2\output\campaign_output"

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "ecommerce_recs"

# This represents the "new batch" for cart-abandonment recovery.
# We limit cart events to keep the demo stable on Windows.
# Increase this later if your machine handles it.
BATCH_CART_LIMIT = 200000


def export_mongo_profiles_to_json():
    print("Exporting user profiles from MongoDB to Spark-readable JSON...")

    stage_dir = Path(PROFILES_STAGE_PATH)

    if stage_dir.exists():
        shutil.rmtree(stage_dir)

    stage_dir.mkdir(parents=True, exist_ok=True)

    output_file = stage_dir / "profiles.json"

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    count = 0

    with output_file.open("w", encoding="utf-8") as f:
        for doc in db.user_profiles.find({}, {"_id": 0, "user_id": 1, "top_categories": 1}):
            top_category_names = [
                item.get("category")
                for item in doc.get("top_categories", [])
                if item.get("category")
            ]

            clean_doc = {
                "profile_user_id": doc["user_id"],
                "top_category_names": top_category_names
            }

            f.write(json.dumps(clean_doc) + "\n")
            count += 1

    client.close()

    print(f"Exported {count} user profiles to {output_file}")


export_mongo_profiles_to_json()

spark = (
    SparkSession.builder
    .appName("Cart Abandonment Recovery Stable")
    .master("local[2]")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.python.worker.reuse", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("Reading cleaned logs...")
df = (
    spark.read
    .parquet(CLEAN_LOGS_PATH)
    .select(
        "user_id",
        "session_id",
        "event_type",
        "item_id",
        "category",
        "event_timestamp"
    )
)

print("Preparing cart events batch...")
cart_events = (
    df
    .filter(col("event_type") == "cart")
    .filter(col("category").isNotNull())
    .select(
        "user_id",
        "session_id",
        "item_id",
        "category",
        "event_timestamp"
    )
    .dropDuplicates(["user_id", "session_id", "item_id"])
    .limit(BATCH_CART_LIMIT)
)

print("Preparing purchase events...")
purchase_events = (
    df
    .filter(col("event_type") == "purchase")
    .select(
        col("user_id").alias("p_user_id"),
        col("session_id").alias("p_session_id"),
        col("item_id").alias("p_item_id")
    )
    .dropDuplicates(["p_user_id", "p_session_id", "p_item_id"])
)

print("Finding abandoned cart items...")
abandoned_carts = (
    cart_events
    .join(
        purchase_events,
        (cart_events.user_id == purchase_events.p_user_id) &
        (cart_events.session_id == purchase_events.p_session_id) &
        (cart_events.item_id == purchase_events.p_item_id),
        "left_anti"
    )
)

print("Reading staged MongoDB user profiles into Spark...")
profiles_df = spark.read.json(PROFILES_STAGE_PATH)

print("Joining abandoned carts with user profiles...")
campaign_df = (
    abandoned_carts
    .join(
        broadcast(profiles_df),
        abandoned_carts.user_id == profiles_df.profile_user_id,
        "left"
    )
    .withColumn(
        "campaign_type",
        when(
            expr("array_contains(top_category_names, category)"),
            lit("High_Discount")
        ).otherwise(lit("Standard_Reminder"))
    )
    .select(
        "user_id",
        "session_id",
        "item_id",
        "category",
        "campaign_type"
    )
)

print("Campaign output sample:")
campaign_df.limit(30).show(30, truncate=False)

print("Campaign type counts:")
campaign_df.groupBy("campaign_type").count().show(truncate=False)

print("Writing campaign output...")
(
    campaign_df
    .coalesce(4)
    .write
    .mode("overwrite")
    .parquet(CAMPAIGN_OUTPUT_PATH)
)

print(f"Done. Campaign output written to: {CAMPAIGN_OUTPUT_PATH}")

spark.stop()