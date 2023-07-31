# Databricks notebook source
# MAGIC %md
# MAGIC # Get Amazon Metadata

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Notebook configuration

# COMMAND ----------

# DBTITLE 1,Import libraries
from pyspark.sql.types import *
import pyspark.sql.functions as f 

# COMMAND ----------

# DBTITLE 1,Storage settings
STORAGE_ACCOUNT = "safactoreddatathon"
CONTAINER_NAME = "source-files"
AUTHENTIFICATION_METHOD = "SAS"
TOKEN = "sp=rle&st=2023-07-25T18:12:36Z&se=2023-08-13T02:12:36Z&sv=2022-11-02&sr=c&sig=l2TCTwPWN8LSM922lR%2Fw78mZWQK2ErEOQDUaCJosIaw%3D"  
BRONZE_BUCKET_NAME = "s3://datapalooza-products-reviews-bronze"

#STORAGE_ACCOUNT = dbutils.secrets.get("datapalooza-products-reviews", "storage_account") 
#CONTAINER_NAME = dbutils.secrets.get("datapalooza-products-reviews", "container_name") 
#AUTHENTIFICATION_METHOD = dbutils.secrets.get("datapalooza-products-reviews", "authentification_method") 
#TOKEN = dbutils.secrets.get("datapalooza-products-reviews", "factored_azure_token")
#BRONZE_BUCKET_NAME = dbutils.secrets.get("datapalooza-products-reviews", "bronze_bucket_name")

# COMMAND ----------

# DBTITLE 1,Spark configuration
spark.conf.set(f"fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net", AUTHENTIFICATION_METHOD)
spark.conf.set(f"fs.azure.sas.token.provider.type.{STORAGE_ACCOUNT}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{STORAGE_ACCOUNT}.dfs.core.windows.net", TOKEN)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Get data

# COMMAND ----------

# Get partitions folder
partitions_folders = dbutils.fs.ls(f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT}.dfs.core.windows.net/amazon_metadata/")
print(f"There are {len(partitions_folders)} partitions folder.")

# COMMAND ----------

# Define metadata schema
metadata_schema = StructType([
    StructField("also_buy", StringType()),
    StructField("also_view", StringType()),
    StructField("asin", StringType()),
    StructField("brand", StringType()),
    StructField("category", StringType()),
    StructField("date", StringType()),
    StructField("description", StringType()),
    StructField("feature", StringType()),
    StructField("details", StructType([
        StructField("\n    Item Weight: \n    ", StringType()),
        StructField("\n    Package Dimensions: \n    ", StringType()),
        StructField("\n    Product Dimensions: \n    ", StringType()),
        StructField("ASIN::", StringType()),
        StructField("ASIN: ", StringType()),
        StructField("Audio CD", StringType()),
        StructField("Batteries", StringType()),
        StructField("Discontinued by manufacturer:", StringType()),
        StructField("Domestic Shipping: ", StringType()),
        StructField("ISBN-10:", StringType()),
        StructField("ISBN-13:", StringType()),
        StructField("International Shipping: ", StringType()),
        StructField("Item model number:", StringType()),
        StructField("Label:", StringType()),
        StructField("Language:", StringType()),
        StructField("Number of Discs:", StringType()),
        StructField("Pamphlet:", StringType()),
        StructField("Paperback:", StringType()),
        StructField("Publisher:", StringType()),
        StructField("Shipping Advisory:", StringType()),
        StructField("Shipping Weight:", StringType()),
        StructField("UPC:", StringType()),
    ])),
    StructField("fit", StringType()),
    StructField("image", StringType()),
    StructField("main_cat", StringType()),
    StructField("price", StringType()),
    StructField("rank", StringType()),
    StructField("similar_item", StringType()),
    StructField("tech1", StringType()),
    StructField("tech2", StringType()),
    StructField("title", StringType())
])

# COMMAND ----------

# Get bronze table
counter = 0
for folder in partitions_folders:
    amazon_metadata = (
       spark
       .read
       .schema(metadata_schema)
       .format("json")
       .load(folder.path)
       .withColumn("source", f.lit("batch"))
       .write
       .format("delta")
       .mode("append")
       .option("delta.columnMapping.mode", "name")
       .saveAsTable(
           "products.bronze.amazon_metadata", 
           path = f"{BRONZE_BUCKET_NAME}/amazon_metadata.delta"
        )
    )
    print(f"Partition {folder} with count {counter}")
    counter += 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data profiling

# COMMAND ----------

bronze = spark.table("products.bronze.amazon_metadata")
bronze.display()

# COMMAND ----------

dbutils.data.summarize(bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ## We are done!

# COMMAND ----------

bronze = spark.table("products.bronze.amazon_metadata")
bronze.display()

# COMMAND ----------

bronze = bronze.withColumn("timestamp_ingested", f.lit("2023-07-26 00:00:00"))
bronze = bronze.withColumn("timestamp_ingested", f.col("timestamp_ingested").cast("timestamp"))
bronze.display()

# COMMAND ----------

BRONZE_BUCKET_NAME

# COMMAND ----------

bronze.write.format("delta").mode("overwrite").option("mergeSchema", "true").option("delta.columnMapping.mode", "name").saveAsTable("products.bronze.amazon_metadata_bronze", path = BRONZE_BUCKET_NAME + "/amazon_metadata_bronze.delta")

# COMMAND ----------


