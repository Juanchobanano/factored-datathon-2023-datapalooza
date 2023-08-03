# Databricks notebook source
# MAGIC %md
# MAGIC # init file

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG products;
# MAGIC CREATE SCHEMA bronze;
# MAGIC CREATE SCHEMA silver;
# MAGIC CREATE SCHEMA gold;
# MAGIC CREATE SCHEMA platinum;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG products;
# MAGIC SELECT SCHEMA bronze;
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS products.bronze.amazon_metadata;
# MAGIC DROP TABLE IF EXISTS products.bronze.amazon_reviews;
# MAGIC DROP TABLE IF EXISTS products.bronze.sample_amazon_metadata;
# MAGIC DROP TABLE IF EXISTS products.bronze.sample_amazon_reviews;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS products.silver.amazon_reviews;
# MAGIC DROP TABLE IF EXISTS products.silver.sample_amazon_metadata;
