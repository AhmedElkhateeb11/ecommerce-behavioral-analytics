import json
import os
import sys
from pathlib import Path

from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, first

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

CLEAN_LOGS_PATH = r"C:\BigData_Project2\output\clean_logs_parquet"
PRODUCT_CATALOG_JSON_PATH = r"C:\BigData_Project2\output\product_catalog_json_for_mongo"

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "ecommerce_recs"


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

                batch.append(json.loads(line))

                if len(batch) >= batch_size:
                    collection.insert_many(batch)
                    total_inserted += len(batch)
                    batch = []

    if batch:
        collection.insert_many(batch)
        total_inserted += len(batch)

    return total_inserted


print("Starting Spark to prepare product catalog...")

spark = (
    SparkSession.builder
    .appName("Prepare Product Catalog")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "16")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(CLEAN_LOGS_PATH)

product_catalog = (
    df
    .filter(col("item_id").isNotNull())
    .filter(col("category").isNotNull())
    .groupBy("item_id")
    .agg(
        first("category", ignorenulls=True).alias("category"),
        first("brand", ignorenulls=True).alias("brand")
    )
)

print("Sample product catalog:")
product_catalog.show(20, truncate=False)

print("Writing product catalog as JSON...")
(
    product_catalog
    .coalesce(2)
    .write
    .mode("overwrite")
    .json(PRODUCT_CATALOG_JSON_PATH)
)

spark.stop()

print("Connecting to MongoDB...")
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

print("Clearing old product_catalog collection...")
db.product_catalog.delete_many({})

print("Loading product catalog into MongoDB...")
count = insert_json_folder(db.product_catalog, PRODUCT_CATALOG_JSON_PATH)

print("Creating index...")
db.product_catalog.create_index("item_id")

print("\nMongoDB counts:")
print("product_catalog:", db.product_catalog.count_documents({}))

print("\nSample product:")
print(db.product_catalog.find_one({}, {"_id": 0}))

client.close()

print("\nDone. Product catalog loaded successfully.")