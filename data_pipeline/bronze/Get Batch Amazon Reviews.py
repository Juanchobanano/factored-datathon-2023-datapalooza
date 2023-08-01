# Databricks notebook source
# MAGIC %md
# MAGIC # Get Amazon Reviews

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
SILVER_BUCKET_NAME = "s3://datapalooza-products-reviews-silver"
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
partitions_folders = dbutils.fs.ls(f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT}.dfs.core.windows.net/amazon_reviews/")
print(f"There are {len(partitions_folders)} partitions folder.")

# COMMAND ----------

# DBTITLE 0,Define review schema
# Define reviews schema
reviews_schema = StructType([
    StructField("asin", StringType()),
    StructField("image", StringType()),
    StructField("overall", StringType()),
    StructField("reviewText", StringType()),
    StructField("reviewerID", StringType()),
    StructField("reviewerName", StringType()),
    StructField("style", StringType()),
    StructField("summary", StringType()),
    StructField("unixReviewTime", StringType()),
    StructField("verified", StringType()),
    StructField("vote", StringType()),
])

# COMMAND ----------

# Get bronze table
counter = 1
for folder in partitions_folders:
    df = (
       spark
       .read
       .schema(reviews_schema)
       .format("json")
       .load(folder.path)
       .withColumn("source", f.lit("batch"))
       .write
       .format("delta") 
       .mode("append")
       .saveAsTable(
           "products.bronze.amazon_reviews", 
           path = f"{BRONZE_BUCKET_NAME}/amazon_reviews.delta"
        )
    )
    counter += 1
    print(f"Partition {counter} ... {folder}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data profiling

# COMMAND ----------

# DBTITLE 1,Bronze columns profiling
bronze = spark.table("products.bronze.amazon_reviews")
dbutils.data.summarize(bronze)

# COMMAND ----------

# DBTITLE 1,Style column keys profiling
keys = (bronze
    .select(f.explode("style"))
    .select("key")
    .distinct()
    .rdd.flatMap(lambda x: x)
    .collect())
exprs = [f.col("style").getItem(k).alias(k) for k in keys]
new_keys = list(set([x.lower().replace(":", "").replace(" ", "_") for x in keys]))
exprs = [f.col("style").getItem(k).alias(k) for k in new_keys]

# COMMAND ----------

dbutils.data.summarize(silver.select(*exprs))

# COMMAND ----------

# MAGIC %md
# MAGIC ## We are done!

# COMMAND ----------

bronze = spark.table("products.bronze.amazon_reviews") #.withColumn("source", f.lit("batch"))
#bronze = bronze.withColumn("date_ingested", f.unix_timestamp(f.lit("2023-07-26 00:00:00")))
bronze = bronze.withColumn("timestamp_ingested", f.lit("2023-07-26 00:00:00"))
bronze = bronze.withColumn("timestamp_ingested", f.col("timestamp_ingested").cast("timestamp"))
bronze.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("products.bronze.amazon_reviews_bronze", path = BRONZE_BUCKET_NAME + "/amazon_reviews_bronze.delta")
