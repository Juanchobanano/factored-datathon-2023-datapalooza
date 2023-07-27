# Databricks notebook source
from pyspark.sql.types import *
import pyspark.sql.functions as f 
import dlt

# COMMAND ----------

STORAGE_ACCOUNT = "safactoreddatathon"
CONTAINER_NAME = "source-files"
AUTHENTIFICATION_METHOD = "SAS"
TOKEN = "sp=rle&st=2023-07-25T18:12:36Z&se=2023-08-13T02:12:36Z&sv=2022-11-02&sr=c&sig=l2TCTwPWN8LSM922lR%2Fw78mZWQK2ErEOQDUaCJosIaw%3D"  
BRONZE_BUCKET_NAME = "s3://datapalooza-products-reviews-bronze"

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net", AUTHENTIFICATION_METHOD)
spark.conf.set(f"fs.azure.sas.token.provider.type.{STORAGE_ACCOUNT}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{STORAGE_ACCOUNT}.dfs.core.windows.net", TOKEN)

# COMMAND ----------

# Get partitions folder
partitions_folders = dbutils.fs.ls(f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT}.dfs.core.windows.net/amazon_metadata/")
print(f"There are {len(partitions_folders)} partitions folder.")

# COMMAND ----------

partitions_folders[0]

# COMMAND ----------

metadata = spark.read.format("json").load(partitions_folders[0])
metadata.display()

# COMMAND ----------

metadata.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE products.bronze.sample_amazon_reviews;

# COMMAND ----------

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
]))

# COMMAND ----------

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

# MAGIC %sql
# MAGIC USE CATALOG products;
# MAGIC USE SCHEMA bronze;
# MAGIC CREATE TABLE IF NOT EXISTS amazon_metadata LOCATION 's3://datapalooza-products-reviews-bronze';

# COMMAND ----------

emptyRDD = spark.sparkContext.emptyRDD()
df_test = spark.createDataFrame(emptyRDD,metadata_schema)

# COMMAND ----------

df_test.printSchema()

# COMMAND ----------

df_test.write.format("delta").saveAsTable(
           "products.bronze.amazon_metadata", 
           path = f"{BRONZE_BUCKET_NAME}/amazon_metadata.delta"
        )

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS products.bronze.amazon_metadata;

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

amazon_metadata = spark.table("products.bronze.amazon_metadata")
amazon_metadata.display()

# COMMAND ----------

data = spark.table("products.bronze.sample_amazon_metadata")
data.display()

# COMMAND ----------

identifiers = [x["col"] for x in data.select(f.explode(f.col("also_buy"))).distinct().collect()]

# COMMAND ----------

data.select("asin").withColumn("asin_in_list", f.array_contains(f.array(identifiers), f.col("asin"))).display()

# COMMAND ----------

data.select("asin").where(f.col("asin") == "B00006IFGW").display()

# COMMAND ----------

 
stringColumns = [item[0] for item in amazon_metadata.dtypes if item[1].startswith('string')]
stringColumns


# COMMAND ----------

for column in stringColumns:
    amazon_metadata.withColumn(column, f.when(f.col(column) == "", f.lit(None)).otherwise(f.col(column)))
    

# COMMAND ----------

amazon_metadata.display()

# COMMAND ----------

data = spark.table("products.bronze.sample_amazon_reviews")

# COMMAND ----------

data.display()

# COMMAND ----------


