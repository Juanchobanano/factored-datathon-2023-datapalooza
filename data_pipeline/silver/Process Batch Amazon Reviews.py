# Databricks notebook source
# MAGIC %md
# MAGIC # Process Amazon Reviews

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Notebook configuration

# COMMAND ----------

# DBTITLE 1,Import libraries
from pyspark.sql.types import *
import pyspark.sql.functions as f 

# COMMAND ----------

BRONZE_BUCKET_NAME = "s3://datapalooza-products-reviews-bronze"
SILVER_BUCKET_NAME = "s3://datapalooza-products-reviews-silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Process bronze data

# COMMAND ----------

# DBTITLE 1,User defined functions
def process_array_images(string: str):
    if string is None:
        return None
    if string == "[]":
        return []
    return string.replace("[", "").replace("]", "").replace('"', '').split(",")
udf_process_array_images = f.udf(lambda z: process_array_images(z), ArrayType(StringType()))

# COMMAND ----------

# DBTITLE 1,Transform data
(
    spark.table("products.bronze.amazon_reviews")
    .withColumn("image", udf_process_array_images(f.col("image")))
    .withColumn("overall", f.col("overall").cast("int"))
    .withColumn("unixReviewTime", f.to_date(f.from_unixtime(f.col("unixReviewTime"))))
    .withColumnRenamed("unixReviewTime", "date")
    .withColumn("verified", f.when(f.col("verified") == "true", f.lit(True)).otherwise(f.lit(False)))
    .withColumn("vote", f.col("vote").cast("int"))
    .drop("style")
    .filter(f.col("asin").isNotNull())
    .write
    .format("delta")
    .saveAsTable(
        "products.silver.amazon_reviews", 
        path = f"{SILVER_BUCKET_NAME}/amazon_reviews.delta"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data profiling

# COMMAND ----------

silver = spark.table("products.silver.amazon_reviews")
dbutils.data.summarize(silver)

# COMMAND ----------

silver = spark.table("products.silver.amazon_reviews").withColumn("source", f.lit("batch"))
silver.display()

# COMMAND ----------

silver = spark.table("products.silver.amazon_reviews") #.withColumn("source", f.lit("batch"))
silver = silver.withColumn("timestamp_ingested", f.lit("2023-07-26 00:00:00"))
silver = silver.withColumn("timestamp_ingested", f.col("timestamp_ingested").cast("timestamp"))
silver.display()

# COMMAND ----------

silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("products.silver.amazon_reviews_silver",  path = f"{SILVER_BUCKET_NAME}/amazon_reviews_silver.delta")

# COMMAND ----------

# MAGIC %md
# MAGIC ## We are done!
