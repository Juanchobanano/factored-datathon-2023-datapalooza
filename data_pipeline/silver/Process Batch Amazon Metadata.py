# Databricks notebook source
# MAGIC %md
# MAGIC # Process Amazon Metadata

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Notebook configuration

# COMMAND ----------

# DBTITLE 1,Import libraries
from pyspark.sql.types import *
import pyspark.sql.functions as f 
from bs4 import BeautifulSoup
from functools import wraps
import re
from pyspark.sql.window import Window

# COMMAND ----------

BRONZE_BUCKET_NAME = "s3://datapalooza-products-reviews-bronze"
SILVER_BUCKET_NAME = "s3://datapalooza-products-reviews-silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Process bronze data

# COMMAND ----------

# DBTITLE 1,Utils functions
def handle_errors(func):
    @wraps(func)
    def handle_error(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as error:
            return None
    return handle_error

# COMMAND ----------

# DBTITLE 1,User defined functions
@handle_errors
def process_arrays(string: str):
    if string == "[]":
        return []
    return string.replace("[", "").replace("]", "").replace('"', '').split(",")

@handle_errors
def process_html(string: str):
    soup = BeautifulSoup(string)
    return soup.text

@handle_errors
def process_maincat(string: str):
    soup = BeautifulSoup(string)
    try:
        category = soup.find("img")["alt"]
        return category
    except:
        return soup.string
        
@handle_errors
def process_description(string: str):
    string = string.replace("[", "").replace("]", "").replace('"', "")
    CLEANR = re.compile('<.*?>') 
    cleantext = re.sub(CLEANR, '', string)
    return cleantext.strip()

@handle_errors
def process_feature(string: str):
    string = string.replace("[", "").replace("]", "").split('"')
    string = [x for x in string if x != '' and x != ',']
    return string

@handle_errors
def process_price(string: str):
    string = string.replace("$", "")
    if string.find(",") != -1:
        if string.find(",") < string.find("."):
            string = string[:string.find(".")]
            string = string.replace(".", "").replace(",", "")
    if "-" in string:
        string = [float(x.strip()) for x in string.split("-")]
        print(string)
        return sum(string) / len(string)
    else:
        return float(string)
    
@handle_errors
def process_rank(string: str):
    if string is None: 
        return None
    result = []

    if "[" in string:
        list_ = string.replace("[", "").replace("]", "").split('"')
        list_ = [x for x in list_ if x != '' and x != ","]
    else:
        list_ = [string]

    for string in list_:
        string = re.search(">(.*) \(", string) or re.search(">(.*)", string)
        string = string.group(1)
        number = int("".join(re.findall('\d+', string)))
        a = string.find("in")
        string = string[a + len("in"):].strip()
        data = {"top": number, "category": string}
        result.append(data)
    return result

def process_similar_items(string: str):
    ids = list()
    soup = BeautifulSoup(string)
    items = soup.find_all("a", attrs = {"class": "a-link-normal"})
    for item in items:
        value = re.search(r'/product-reviews/(.*?)/ref', item["href"])
        if value:
            ids.append(value.group(1))
    return list(set(ids))

def process_feature(string: str):
    string = string.replace("[", "").replace("]", "").split('"')
    string = [x for x in string if x != '' and x != ',']
    return string
    
udf_process_arrays = f.udf(lambda z: process_arrays(z), ArrayType(StringType()))
udf_process_feature = f.udf(lambda z: process_feature(z), ArrayType(StringType()))
udf_process_similar_items = f.udf(lambda z: process_similar_items(z), ArrayType(StringType()))
udf_process_maincat = f.udf(lambda z: process_maincat(z)) #, StringType()) 
udf_process_html = f.udf(lambda z: process_html(z), StringType())
udf_process_description = f.udf(lambda z: process_description(z), StringType())
udf_process_feature = f.udf(lambda z: process_feature(z), ArrayType(StringType()))
udf_process_price = f.udf(lambda z: process_price(z), FloatType())
udf_process_rank = f.udf(lambda z: process_rank(z), 
                         ArrayType(
                         StructType([
                             StructField("top", IntegerType()), 
                             StructField("category", StringType())
                         ]))
)

# COMMAND ----------

for file_ in dbutils.fs.ls(BRONZE_BUCKET_NAME + "/amazon_metadata_bronze.delta/"):
    print(file_)

# COMMAND ----------

bronze_columns = spark.table("products.bronze.amazon_metadata_bronze").columns
bronze_columns

# COMMAND ----------

len(dbutils.fs.ls(BRONZE_BUCKET_NAME + "/amazon_metadata_bronze.delta/"))

# COMMAND ----------

for file_ in dbutils.fs.ls(BRONZE_BUCKET_NAME + "/amazon_metadata_bronze.delta/"):
    print(f"Processing batch {file_} ...")
    table = (
        spark.read.format("parquet").load(file_.path + "*")
    )

    for i in range(len(table.columns)):
        table = table.withColumnRenamed(table.columns[i], bronze_columns[i])
    table.display()
    break

# COMMAND ----------

import tqdm

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS products.silver.amazon_metadata_silver;

# COMMAND ----------

partition = Window.orderBy(f.lit("A"))
select_columns = ["asin", "title", "brand", "main_category", "category", "description", "feature", "mean_price", "rank", "also_buy", "also_view", "similar_items", "image", "source", "timestamp_ingested"]

#counter = 1
for file_ in tqdm.tqdm(dbutils.fs.ls(BRONZE_BUCKET_NAME + "/amazon_metadata_bronze.delta/")[95:]):
    print(f"Processing batch {file_} ... with counter {counter}")
    table = (
        spark.read.format("parquet").load(file_.path + "*")
    )

    #print(table.count())
    #break
    for i in range(len(table.columns)):
        table = table.withColumnRenamed(table.columns[i], bronze_columns[i])
    table = (
        table
        .withColumn("also_buy", udf_process_arrays(f.col("also_buy")))
        .withColumn("also_view", udf_process_arrays(f.col("also_view")))
        .withColumn("also_buy", f.when(f.size(f.col("also_buy")) == 0, f.lit(None)).otherwise(f.col("also_buy")))
        .withColumn("also_view", f.when(f.size(f.col("also_view")) == 0, f.lit(None)).otherwise(f.col("also_view")))
        .withColumn("brand", f.initcap(f.trim(f.col("brand"))))
        .withColumn("brand", f.when(f.col("brand") == "", f.lit(None)).otherwise(f.col("brand")))
        .withColumn("category", udf_process_arrays(f.col("category")))
        .withColumn("category", f.when(f.size(f.col("category")) == 0, f.lit(None)).otherwise(f.col("category")))
        .withColumn("description", udf_process_description(f.col("description")))
        .withColumn("description", f.when(f.col("description") == "", f.lit(None)).otherwise(f.col("description")))
        .withColumn("feature", udf_process_feature(f.col("feature")))
        .withColumn("feature", f.when(f.size(f.col("feature")) == 0, f.lit(None)).otherwise(f.col("feature")))
        .withColumn("main_cat", f.when(f.col("category").isNotNull(), f.col("category")[0]).otherwise(f.lit(None)))
        .withColumnRenamed("main_cat", "main_category")
        .withColumn("image", udf_process_arrays(f.col("image")))
        .withColumn("image", f.when(f.size(f.col("image")) != 0, f.col("image")).otherwise(f.lit(None)))
        .withColumn("price", f.when(f.col("price") == "", f.lit(None))
                            .when(~f.col("price").contains("$"), f.lit(None))
                            .otherwise(udf_process_price(f.col("price")))
        )
        .withColumnRenamed("price", "mean_price")
        .withColumn("rank", udf_process_rank(f.col("rank")))
        .withColumn("rank", f.when(f.size(f.col("rank")) == 0, f.lit(None)).otherwise(f.col("rank")))
        .withColumn("main_category", 
                    f.when(
                        (f.col("main_category").isNull()) & (f.col("rank").isNotNull()), 
                        f.col("rank")[0]["category"]
                ).otherwise(f.col("main_category"))
        )
        .withColumn("category", 
                    f.when(
                        (f.col("category").isNull()) & (f.col("rank").isNotNull()), 
                        f.array(f.col("rank")[0]["category"])
                    ).otherwise(f.col("category"))
        )
        .withColumn("similar_item", udf_process_similar_items(f.col("similar_item")))
        .withColumn("similar_item", f.when(f.size(f.col("similar_item")) == 0, f.lit(None)).otherwise(f.col("similar_item")))
        .withColumnRenamed("similar_item", "similar_items")
        .drop("date")  # no consistency
        .drop("details") # null values 
        .drop("fit")
        .drop("tech1") #no time
        .drop("tech2") #no time
        #.drop("row_number")
        .select(select_columns)
        #.display()
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(
            "products.silver.amazon_metadata_silver",
            path = SILVER_BUCKET_NAME + "/amazon_metadata_silver.delta"
        )
    )
    counter += 1

# COMMAND ----------

dbutils.fs.ls(BRONZE_BUCKET_NAME + "/amazon_metadata_bronze.delta/")[95:]

# COMMAND ----------

# MAGIC %md
# MAGIC ## We are done!
