# Databricks notebook source
silver = spark.table("products.silver.amazon_metadata_silver")

# COMMAND ----------

silver.columns

# COMMAND ----------



# COMMAND ----------

silver_reviews = spark.table("products.silver.amazon_reviews_silver")
silver_metadata = spark.table("products.silver.amazon_metadata_silver")

# COMMAND ----------

silver_reviews.join(
    silver_metadata.select("asin", "main_category")
    .where(
        (f.col("main_category") == "Electronics") & (f.col("main_category") == "Cell Phones & Accessories")),
        on = "asin", 
        how = "left"
    ).groupBy("main_category").count().display()

# COMMAND ----------

import pyspark.sql.functions as f
silver_metadata.groupBy(f.col("main_category")).count().display()

# COMMAND ----------


