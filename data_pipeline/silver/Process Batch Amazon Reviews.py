# Databricks notebook source
# MAGIC %md
# MAGIC # Process Amazon Reviews
# MAGIC
# MAGIC ### Dependencies: Process Batch Amazon Metadata

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
select_columns = ["asin", "category", "image", "overall", "reviewerID", "reviewerName", "reviewText", "summary", "date", "verified", 
                  "vote", "source", "timestamp_ingested", "reviewID"]

(
    spark.table("products.bronze.amazon_reviews")
    .filter(f.col("asin").isNotNull())
    .withColumn("image", udf_process_array_images(f.col("image")))
    .withColumn("overall", f.col("overall").cast("int"))
    .withColumn("unixReviewTime", f.to_date(f.from_unixtime(f.col("unixReviewTime"))))
    .withColumnRenamed("unixReviewTime", "date")
    .filter(f.col('date') >= '2017-01-01') # Only consider reviews after 2017-01-01
    .withColumn("verified", f.when(f.col("verified") == "true", f.lit(True)).otherwise(f.lit(False)))
    .withColumn("vote", f.col("vote").cast("int"))
    .withColumn("concat", f.concat(f.col("asin"), f.col("reviewerID"), f.col("reviewText"), f.col("date")))
    .withColumn("reviewID", f.sha1(f.col("concat")))
    .dropDuplicates(["reviewID"])
    .drop("concat")
    .drop("style")
    .select(select_columns)
    .join(
        spark.table("products.silver.amazon_categories_selected"), 
        on = ["asin"], 
        how = "inner"
    )
    .withColumnRenamed("main_category", "category")
    .select(reorder_columns)
    .write
    .format("delta")
    .saveAsTable(
        "products.silver.amazon_reviews_selected", 
        path = f"{SILVER_BUCKET_NAME}/amazon_reviews_selected.delta"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data profiling

# COMMAND ----------

silver = spark.table("products.silver.amazon_reviews_selected")
dbutils.data.summarize(silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ## We are done!
