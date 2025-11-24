from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, upper, to_date

spark = SparkSession.builder \
    .appName("BronzeToSilver") \
    .master("local[*]") \
    .getOrCreate()

# Read from bronze
df = spark.read.parquet("bronze/retail_bronze.parquet")

# 1. Rename columns (remove spaces, lowercase)
df = df.toDF(*[c.replace(" ", "_").lower() for c in df.columns])

# 2. Convert date string → DateType
df = df.withColumn("date", to_date(col("date"), "dd-MM-yyyy"))


# 3. Trim and format text fields
df = df.withColumn("gender", trim(col("gender"))) \
       .withColumn("product_category", trim(col("product_category")))

# 4. Remove duplicates (based on transaction_id)
df = df.dropDuplicates(["transaction_id"])

# 5. Recalculate total_amount for consistency
df = df.withColumn("total_amount", col("quantity") * col("price_per_unit"))

# 6. Drop rows with nulls in critical columns
df = df.dropna(subset=["transaction_id", "date", "quantity"])

# Write to silver
df.write.mode("overwrite").parquet("silver/retail_silver.parquet")

print("Silver layer written successfully!")

spark.stop()
