# Databricks notebook source
# MAGIC %sql 
# MAGIC USE CATALOG products;
# MAGIC USE SCHEMA silver;
# MAGIC SELECT date, COUNT(*) FROM products.silver.amazon_reviews GROUP BY date;

# COMMAND ----------

silver = spark.table("products.silver.amazon_reviews")

# COMMAND ----------

silver.display()

# COMMAND ----------

silver.groupBy("date").count().display()

# COMMAND ----------

import pyspark.sql.functions as f
aggregation = silver.groupBy("date").count().orderBy(f.col("date").asc())

# COMMAND ----------

aggregation.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date, COUNT(*) FROM products.silver.amazon_reviews GROUP BY date;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG products;
# MAGIC CREATE SCHEMA gold;
# MAGIC CREATE SCHEMA platinum;

# COMMAND ----------

aggregation.write.format("delta").saveAsTable("products.platinum.reviews_count_per_day", path = "s3://datapalooza-products-reviews-platinum/reviews_count_per_day.delta")

# COMMAND ----------


