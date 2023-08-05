# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType
from pyspark.sql.functions import decode, col, from_json, lit, length, to_date, from_unixtime, when, unix_timestamp, regexp_replace, split, concat ,sha1

# COMMAND ----------

# DBTITLE 1,Event Hubs configuration
EH_NAMESPACE                    = "factored-datathon"
EH_NAME                         = "factored_datathon_amazon_reviews_4"

EH_CONN_SHARED_ACCESS_KEY_NAME  = "datathon_group_4"
EH_CONN_SHARED_ACCESS_KEY_VALUE = "zkZkK6UnK6PpFAOGbgcBfnHUZsXPZpuwW+AEhEH24uc="

EH_CONN_STR                     = f"Endpoint=sb://{EH_NAMESPACE}.servicebus.windows.net/;SharedAccessKeyName={EH_CONN_SHARED_ACCESS_KEY_NAME};SharedAccessKey={EH_CONN_SHARED_ACCESS_KEY_VALUE}"

# COMMAND ----------

# DBTITLE 1,Kafka Consumer configuration
KAFKA_OPTIONS = {
  "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
  "subscribe"                : EH_NAME,
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";",
  "kafka.request.timeout.ms" : "60000",
  "kafka.session.timeout.ms" : "30000",
  "maxOffsetsPerTrigger"     : "1000",
  "failOnDataLoss"           : "false",
  "startingOffsets"          : "earliest"
}

# COMMAND ----------

# Read streaming data

streaming_amazon_reviews =  (
    spark
    .readStream
    .format("kafka")
    .options(**KAFKA_OPTIONS)
    .load()    
)

# COMMAND ----------

streaming_amazon_reviews.groupBy().count().display()

# COMMAND ----------

# Schema of raw streaming data that would be ingested on bronze table 

schema = StructType(
    [
        StructField('asin', StringType(), True),
        StructField('image', StringType(), True),
        StructField('overall', StringType(), True),
        StructField('reviewText', StringType(), True),
        StructField('reviewerID', StringType(), True),
        StructField('reviewerName', StringType(), True),
        StructField('style', StringType(), True),
        StructField('summary', StringType(), True),
        StructField('unixReviewTime', StringType(), True),
        StructField('verified', StringType(), True),
        StructField('vote', StringType(), True)
    ]
)


# COMMAND ----------

BRONZE_BUCKET_NAME = "s3://datapalooza-products-reviews-bronze"
SILVER_BUCKET_NAME = "s3://datapalooza-products-reviews-silver"
test = "offline"
checkpointLocation_bronze = f"{BRONZE_BUCKET_NAME}/streaming/{test}/checkpointLocation_bronze"
checkpointLocation_silver = f"{SILVER_BUCKET_NAME}/streaming/{test}/checkpointLocation_silver"
checkpointLocation_selected_silver = f"{SILVER_BUCKET_NAME}/streaming/{test}/checkpointLocation_selected_silver"
path_bronze = f"{BRONZE_BUCKET_NAME}/amazon_reviews_bronze.delta"
path_silver = f"{SILVER_BUCKET_NAME}/amazon_reviews_silver.delta"
path_selected_silver = f"{SILVER_BUCKET_NAME}/amazon_reviews_selected"

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Streaming Data

# COMMAND ----------

# Processed data from streaming
streaming_amazon_reviews_processed = (
    streaming_amazon_reviews
    .withColumn("value", decode(col("value"), "utf-8"))
    .withColumn("data", from_json(col("value"), schema))
    .select("data.*", "timestamp")
    .withColumn("source", lit("streaming"))
    .withColumnRenamed("timestamp", "timestamp_ingested")
)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Write Streaming Data to Bronze

# COMMAND ----------

# Write streaming data to bronze
write_to_bronze_query = (
    streaming_amazon_reviews_processed
    .writeStream
    .format("delta")
    .outputMode("append")
    .trigger(once=True)
    .option("checkpointLocation", checkpointLocation_bronze)
    .option("path", path_bronze)
)

# COMMAND ----------

write_to_bronze_query.start().awaitTermination(timeout=10)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Write Streaming Data to General Silver

# COMMAND ----------

streaming_amazon_reviews_silver = (
    streaming_amazon_reviews_processed
    .withColumn("image", split(regexp_replace(col("image"), r"[\[\]]", ""), ","))
    .withColumn("overall", col("overall").cast("int"))
    .withColumn("verified", when(col("verified") == "true", lit(True)).otherwise(lit(False)))
    .withColumn("vote", col("vote").cast("int"))
    .drop("style")
    .drop("unixReviewTime")
    .withColumn("date", to_date(col("timestamp_ingested")))
    .filter(col("date") >= "2017-01-01")
    .withColumn("concat", concat(col("asin"), col("reviewerID"), col("reviewText"), col("date")))   
    .withColumn("reviewID", sha1(col("concat")))
    .drop("concat")
    .filter(col("asin").isNotNull())
    .dropDuplicates(["reviewID"])
    #falta el category
)

# COMMAND ----------

streaming_amazon_reviews_silver.groupBy().count().display()

# COMMAND ----------

write_to_silver_query = ( 
    streaming_amazon_reviews_silver
    .writeStream
    .format("delta")
    .outputMode("append")
    .trigger(once=True)
    .option("checkpointLocation", checkpointLocation_silver)
    .option("path", path_silver)
)

# COMMAND ----------

initial_silver_count = spark.read.format("delta").load(path_silver).count()
print(initial_silver_count)

# COMMAND ----------

write_to_silver_query.start().awaitTermination()

# COMMAND ----------

final_silver_count = spark.read.format("delta").load(path_silver).count()
print(final_silver_count)

# COMMAND ----------

final_silver_count - initial_silver_count

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Write Streaming Data to Selected Silver

# COMMAND ----------

categories = spark.table("products.silver.amazon_categories_selected")

# COMMAND ----------

write_to_selected_silver_query = ( 
    streaming_amazon_reviews_silver
    .join(categories, on="asin", how="inner")
    .withColumnRenamed("main_category", "category")
)

# COMMAND ----------

write_to_selected_silver_query.groupBy().count().display()

# COMMAND ----------

df = spark.read.format("delta").load(path_selected_silver)

# COMMAND ----------

query = (
    write_to_selected_silver_query
    .writeStream
    .format("delta")
    .outputMode("append")
    .trigger(once=True)
    .option("checkpointLocation", checkpointLocation_selected_silver)
    .option("path", path_selected_silver)
)

# COMMAND ----------

query.start().awaitTermination()

# COMMAND ----------

# MAGIC %md 
# MAGIC # Real time Analytics
