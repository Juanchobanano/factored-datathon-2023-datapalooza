# Databricks notebook source
# import dlt
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import decode, col, from_json, lit, length, unix_timestamp

# COMMAND ----------

# Event Hubs configuration

EH_NAMESPACE                    = "factored-datathon"
EH_NAME                         = "factored_datathon_amazon_reviews_4"

EH_CONN_SHARED_ACCESS_KEY_NAME  = "datathon_group_4"
EH_CONN_SHARED_ACCESS_KEY_VALUE = "zkZkK6UnK6PpFAOGbgcBfnHUZsXPZpuwW+AEhEH24uc="

EH_CONN_STR                     = f"Endpoint=sb://{EH_NAMESPACE}.servicebus.windows.net/;SharedAccessKeyName={EH_CONN_SHARED_ACCESS_KEY_NAME};SharedAccessKey={EH_CONN_SHARED_ACCESS_KEY_VALUE}"

# COMMAND ----------

# Kafka Consumer configuration

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

BRONZE_BUCKET_NAME = "s3://datapalooza-products-reviews-bronze"
test = "offline"
checkpointLocation = f"{BRONZE_BUCKET_NAME}/streaming/{test}/checkpointLocation"
path = f"{BRONZE_BUCKET_NAME}/streaming/{test}/amazon_reviews_bronze_sample.delta"

# COMMAND ----------

write_to_bronze_query = (
    streaming_amazon_reviews
    .withColumn("value", decode(col("value"), "utf-8"))
    .writeStream
    .format("delta")
    .trigger(once=True) 
    .outputMode("append")
    .option("checkpointLocation", checkpointLocation)
    .option("path", path)
)

# COMMAND ----------

write_to_bronze_query.start().awaitTermination()

# COMMAND ----------

df = spark.read.format("delta").load(path)

# COMMAND ----------

df.count()

# COMMAND ----------

df.display()

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

(
    df.
    withColumn("data", from_json(col("value"), schema))
    .select("data.*", "timestamp")
    .withColumn("unixReviewTime", unix_timestamp(col("timestamp")))
    .drop("timestamp")
).display()

# COMMAND ----------

spark.table("products.silver.amazon_reviews_silver").select("image").display()

# COMMAND ----------

spark.table("products.silver.amazon_reviews_silver").select("image").schema
