import json
import os
import sys
from pathlib import Path

from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Force Spark workers to use this virtual environment's Python
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

USER_PROFILES_PATH = r"C:\BigData_Project2\output\user_profiles"
ITEM_PAIRS_PARQUET_PATH = r"C:\BigData_Project2\output\item_pairs"
ITEM_PAIRS_JSON_PATH = r"C:\BigData_Project2\output\item_pairs_json_for_mongo"

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "ecommerce_recs"

# Keep this reasonable for demo speed.
# You can increase it later if needed.
TOP_ITEM_PAIR_LIMIT = 50000


def insert_json_folder(collection, folder_path, batch_size=1000):
    folder = Path(folder_path)
    json_files = sorted(folder.glob("part-*.json"))

    if not json_files:
        raise FileNotFoundError(f"No part-*.json files found in {folder_path}")

    total_inserted = 0
    batch = []

    for file_path in json_files:
        print(f"Reading {file_path.name}...")

        with file_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()

                if not line:
                    continue

                doc = json.loads(line)
                batch.append(doc)

                if len(batch) >= batch_size:
                    collection.insert_many(batch)
                    total_inserted += len(batch)
                    batch = []

    if batch:
        collection.insert_many(batch)
        total_inserted += len(batch)

    return total_inserted


print("Connecting to MongoDB...")
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

print("Clearing old data...")
db.user_profiles.delete_many({})
db.item_pairs.delete_many({})

print("Starting Spark to prepare item pairs JSON...")
spark = (
    SparkSession.builder
    .appName("Prepare Item Pairs for MongoDB")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "16")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("Reading item pairs Parquet...")
item_pairs_df = spark.read.parquet(ITEM_PAIRS_PARQUET_PATH)

print(f"Writing top {TOP_ITEM_PAIR_LIMIT} item pairs as JSON for MongoDB...")
(
    item_pairs_df
    .orderBy(col("pair_count").desc())
    .limit(TOP_ITEM_PAIR_LIMIT)
    .coalesce(4)
    .write
    .mode("overwrite")
    .json(ITEM_PAIRS_JSON_PATH)
)

spark.stop()

print("Loading user profiles into MongoDB...")
user_profile_count = insert_json_folder(db.user_profiles, USER_PROFILES_PATH)

print("Loading item pairs into MongoDB...")
item_pair_count = insert_json_folder(db.item_pairs, ITEM_PAIRS_JSON_PATH)

print("Creating indexes...")
db.user_profiles.create_index("user_id")
db.item_pairs.create_index("item_1")
db.item_pairs.create_index("item_2")
db.item_pairs.create_index([("item_1", 1), ("item_2", 1)])

print("\nMongoDB counts:")
print("user_profiles:", db.user_profiles.count_documents({}))
print("item_pairs:", db.item_pairs.count_documents({}))

print("\nSample user profile:")
print(db.user_profiles.find_one({}, {"_id": 0}))

print("\nSample item pair:")
print(db.item_pairs.find_one({}, {"_id": 0}))

client.close()

print("\nDone. MongoDB loading completed successfully.")