# Databricks notebook source
from pyspark.sql.types import *
import pyspark.sql.functions as f 

# COMMAND ----------

bronze = (
    spark.table("products.bronze.amazon_reviews")
)

# COMMAND ----------

bronze.display()

# COMMAND ----------

dbutils.data.summarize(bronze)

# COMMAND ----------



# COMMAND ----------

bronze = (
    spark.table("products.bronze.sample_amazon_reviews")
    #.withColumn("asin", f.col("asin").cast("int"))
    #.withColumn("image")
    .withColumn("overall", f.col("overall").cast("float"))
    #.withColumn("reviewText", )
    #.withColumn("reviewerID", "")
    #.withColumn("reviewerName", "")
    #.withColumn("style", "")
    #.withColumn("summary", "")
    .withColumn("unixReviewTime", f.to_date(f.from_unixtime(f.col("unixReviewTime"))))
    .withColumn("verified", f.when(f.col("verified") == "true", f.lit(True)).otherwise(f.lit(False)))
    .withColumn("vote", f.col("vote").cast("int"))
)
bronze.display()

# COMMAND ----------

bronze.select(f.col("style"), "style.Size").display()

# COMMAND ----------



# COMMAND ----------

bronze.select("image").distinct().collect()

# COMMAND ----------


