from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, upper, to_date

spark = SparkSession.builder \
    .appName("BronzeToSilver") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.parquet("bronze/retail_bronze.parquet")
df = df.toDF(*[c.replace(" ", "_").lower() for c in df.columns])
df = df.withColumn("date", to_date(col("date"), "dd-MM-yyyy"))
df = df.withColumn("gender", trim(col("gender"))) \
       .withColumn("product_category", trim(col("product_category")))
df = df.dropDuplicates(["transaction_id"])
df = df.withColumn("total_amount", col("quantity") * col("price_per_unit"))
df = df.dropna(subset=["transaction_id", "date", "quantity"])

df.write.mode("overwrite").parquet("silver/retail_silver.parquet")

print("Silver layer written successfully!")

spark.stop()
