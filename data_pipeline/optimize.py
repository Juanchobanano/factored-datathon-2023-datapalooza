# Databricks notebook source
# MAGIC %md
# MAGIC ## Optimize tables

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE products.bronze.amazon_reviews;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE products.bronze.amazon_metadata;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE products.silver.amazon_reviews;
