# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze data

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Notebook configuration

# COMMAND ----------

# DBTITLE 1,Import libraries
from pyspark.sql.types import *
import pyspark.sql.functions as f 
import dlt

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

# MAGIC %md
# MAGIC ### Amazon reviews

# COMMAND ----------

# DBTITLE 0,Define review schema
# Reviews schema
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

# Get partitions folder
partitions_folders = dbutils.fs.ls(f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT}.dfs.core.windows.net/amazon_reviews/")
print(f"There are {len(partitions_folders)} partitions folder.")

# COMMAND ----------

def string_to_dicc(string: str):
    if string is None: 
        return None
    return json.loads(string)
udf_string_to_dicc = f.udf(lambda z: string_to_dicc(z), StringType())

# COMMAND ----------

def string_to_dicc(string: str):
    if string is None: 
        return None
    dicc = json.loads(string)
    new_dicc = {}
    for key in dicc.keys():
        new_dicc[key.lower().replace(":", "").replace(" ", "_")] = dicc[key].lower().strip()
    return new_dicc
udf_string_to_dicc = f.udf(lambda z: string_to_dicc(z), MapType(StringType(), StringType()))

# COMMAND ----------

f.struct(f.col("style"))

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG products;
# MAGIC CREATE SCHEMA silver;

# COMMAND ----------

silver = (
    spark.table("products.bronze.amazon_reviews")
    .withColumnRenamed("unixReviewTime", "date")
    .drop("style")
    .filter(f.col("asin").isNotNull())
    #.withColumn("style", f.struct(f.col("style")))
    #.withColumn("style", udf_string_to_dicc(f.col("style")))
    #.select("*", "style.*")
).write.format("delta").saveAsTable("products.silver.amazon_reviews", path = f"{SILVER_BUCKET_NAME}/amazon_reviews.delta")

# COMMAND ----------

bronze = spark.table("products.bronze.amazon_reviews")
bronze.display()

# COMMAND ----------

silver.printSchema()

# COMMAND ----------

silver.printSchema()

# COMMAND ----------


keys = (silver
    .select(f.explode("style"))
    .select("key")
    .distinct()
    .rdd.flatMap(lambda x: x)
    .collect())
exprs = [f.col("style").getItem(k).alias(k) for k in keys]

# COMMAND ----------

import joblib
joblib.dump(keys, "keys.joblib")

# COMMAND ----------



# COMMAND ----------

keys, exprs

# COMMAND ----------

new_keys = list(set([x.lower().replace(":", "").replace(" ", "_") for x in keys]))
exprs = [f.col("style").getItem(k).alias(k) for k in new_keys]

# COMMAND ----------

[(exprs[i], i) for i in range(len(exprs))]

# COMMAND ----------

silver.select(exprs[77]).distinct().display()

# COMMAND ----------

dbutils.data.summarize(silver.select(*exprs))

# COMMAND ----------

silver.select()

# COMMAND ----------

silver.select(*exprs).display()

# COMMAND ----------

silver.select(exprs[0]).withColumnRenamed("Scent:", "scent").where("scent IS NOT NULL").display()

# COMMAND ----------

res = [x["style"] for x in silver.select("style").distinct().limit(10).collect()]

# COMMAND ----------

res[0]

# COMMAND ----------

type(res[0])

# COMMAND ----------

dictionary = '{"Color:":" Blue Insert with Black Trim","Configuration:":" Front Row Sport Buckets with separate headrests and without integrated seat airbags"}'
import json 
def string_to_dict(string: str):
    dicc = json.loads(string)
    #format_dicc = {}
    #for key in dicc.keys():
    #    format_dicc[key] = dicc[key].strip()
    return dicc

stirng

# COMMAND ----------



# COMMAND ----------

string_to_dict(dictionary)

# COMMAND ----------

#bronze = (
#    spark.table("products.silver.amazon_reviews")
#)
bronze = (
    spark.table("products.bronze.amazon_reviews")
    .withColumn("unixReviewTime", f.unix_timestamp(f.col("unixReviewTime")))
)
for column in bronze.columns:
    bronze = bronze.withColumn(column, f.col(column).cast("string"))
bronze = bronze.withColumn("overall", f.concat(f.col("overall"), f.lit(".0")))

# COMMAND ----------

bronze.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("products.bronze.amazon_reviews_2", path = f"{BRONZE_BUCKET_NAME}/amazon_reviews_2.delta")

# COMMAND ----------

bronze_sample = spark.table("products.bronze.sample_amazon_reviews")
bronze_sample.display()

# COMMAND ----------

bronze.display()

# COMMAND ----------

bronze.printSchema()

# COMMAND ----------

bronze.write.format("delta").saveAsTable("products.bronze.amazon_reviews", path = f"{BRONZE_BUCKET_NAME}/amazon_reviews.delta")

# COMMAND ----------

silver.printSchema()

# COMMAND ----------



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
       .withColumn("image", udf_process_array_images(f.col("image")))
       .withColumn("overall", f.col("overall").cast("int"))
       .withColumn("unixReviewTime", f.to_date(f.from_unixtime(f.col("unixReviewTime"))))
       .withColumn("verified", f.when(f.col("verified") == "true", f.lit(True)).otherwise(f.lit(False)))
       .withColumn("vote", f.col("vote").cast("int"))
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



# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

def process_array_images(string: str):
    if string is None:
        return None
    if string == "[]":
        return []
    return string.replace("[", "").replace("]", "").replace('"', '').split(",")
udf_process_array_images = f.udf(lambda z: process_array_images(z), ArrayType(StringType()))

# COMMAND ----------

df.select("image").distinct().display()

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Amazon metadata

# COMMAND ----------

# Metadata schema
metadata_schema = StructType([
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



# COMMAND ----------

# MAGIC %md
# MAGIC ## We are done!
