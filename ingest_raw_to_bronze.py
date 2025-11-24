from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, current_timestamp

# Create Spark session
spark = SparkSession.builder \
    .appName("IngestRawToBronze") \
    .master("local[*]") \
    .getOrCreate()

# Raw file path (Linux/WSL)
raw_path = "raw/retail_sales_dataset.csv"

# Read raw CSV
df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(raw_path) \
    .withColumn("ingest_time", current_timestamp()) \
    .withColumn("source_file", input_file_name())

# Write to bronze as Parquet
df_raw.write \
    .mode("overwrite") \
    .parquet("bronze/retail_bronze.parquet")

print("Bronze layer written successfully!")

spark.stop()
