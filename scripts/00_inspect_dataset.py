from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object, regexp_replace

DATA_PATH = r"C:\BigData_Project2\data\raw_logs.csv"

spark = (
    SparkSession.builder
    .appName("Inspect Ecommerce Dataset Fixed CSV")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = (
    spark.read
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .option("quote", '"')
    .option("escape", '"')
    .option("multiLine", "false")
    .csv(DATA_PATH)
)

print("\n========== COLUMNS ==========")
print(df.columns)

print("\n========== SCHEMA ==========")
df.printSchema()

print("\n========== FIRST 10 ROWS ==========")
df.show(10, truncate=False)

print("\n========== PRODUCT METADATA SAMPLE ==========")
df.select("product_metadata").show(10, truncate=False)

print("\n========== TRY EXTRACT CATEGORY ==========")
test_df = df.withColumn(
    "category",
    get_json_object(col("product_metadata"), "$.category")
)

test_df.select(
    "product_id",
    "event_type",
    "product_metadata",
    "category"
).show(20, truncate=False)

print("\n========== CATEGORY COUNTS ==========")
test_df.groupBy("category").count().orderBy("count", ascending=False).show(30, truncate=False)

print("\n========== EVENT TYPE COUNTS ==========")
df.groupBy("event_type").count().orderBy("count", ascending=False).show(20, truncate=False)

print("\n========== DONE ==========")
spark.stop()