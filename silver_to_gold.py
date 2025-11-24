from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, date_format,
    when, floor
)

spark = SparkSession.builder \
    .appName("SilverToGold") \
    .master("local[*]") \
    .getOrCreate()

# Read silver layer
df = spark.read.parquet("silver/retail_silver.parquet")

# ---------------------------------------------
# 1. DAILY SALES
# ---------------------------------------------
daily_sales = df.groupBy("date") \
    .agg(
        spark_sum("total_amount").alias("total_sales"),
        count("*").alias("num_transactions")
    )

daily_sales.write.mode("overwrite").parquet("gold/daily_sales")


# ---------------------------------------------
# 2. MONTHLY SALES
# ---------------------------------------------
monthly_sales = df.withColumn("month", date_format(col("date"), "yyyy-MM")) \
    .groupBy("month") \
    .agg(
        spark_sum("total_amount").alias("total_sales"),
        count("*").alias("num_transactions")
    )

monthly_sales.write.mode("overwrite").parquet("gold/monthly_sales")


# ---------------------------------------------
# 3. SALES BY PRODUCT CATEGORY
# ---------------------------------------------
category_sales = df.groupBy("product_category") \
    .agg(
        spark_sum("total_amount").alias("total_sales"),
        count("*").alias("num_transactions")
    )

category_sales.write.mode("overwrite").parquet("gold/category_sales")


# ---------------------------------------------
# 4. SALES BY GENDER
# ---------------------------------------------
gender_sales = df.groupBy("gender") \
    .agg(
        spark_sum("total_amount").alias("total_sales"),
        count("*").alias("num_transactions")
    )

gender_sales.write.mode("overwrite").parquet("gold/gender_sales")


# ---------------------------------------------
# 5. SALES BY AGE GROUP
# ---------------------------------------------
df = df.withColumn(
    "age_group",
    when(col("age") < 20, "Below 20")
    .when((col("age") >= 20) & (col("age") < 30), "20-29")
    .when((col("age") >= 30) & (col("age") < 40), "30-39")
    .when((col("age") >= 40) & (col("age") < 50), "40-49")
    .otherwise("50+")
)

age_sales = df.groupBy("age_group") \
    .agg(
        spark_sum("total_amount").alias("total_sales"),
        count("*").alias("num_transactions")
    )

age_sales.write.mode("overwrite").parquet("gold/age_sales")


# ---------------------------------------------
# 6. TOP CUSTOMERS
# ---------------------------------------------
top_customers = df.groupBy("customer_id") \
    .agg(
        spark_sum("total_amount").alias("lifetime_value")
    ) \
    .orderBy(col("lifetime_value").desc())

top_customers.write.mode("overwrite").parquet("gold/top_customers")


print("Gold layer successfully created!")

spark.stop()
