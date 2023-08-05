# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType
from pyspark.sql.functions import decode, col, from_json, lit, length, to_date, from_unixtime, when, unix_timestamp, regexp_replace, split, concat ,sha1, current_timestamp

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
  "startingOffsets"          : "latest"
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
test = "online"
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
    .option("checkpointLocation", checkpointLocation_bronze)
    .option("path", path_bronze)
)

# COMMAND ----------

write_to_bronze_query.start()

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
    .withColumn("timestamp_ingested", current_timestamp())
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
    .option("checkpointLocation", checkpointLocation_silver)
    .option("path", path_silver)
)

# COMMAND ----------

write_to_silver_query.start()

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

query = (
    write_to_selected_silver_query
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpointLocation_selected_silver)
    .option("path", path_selected_silver)
)

# COMMAND ----------

query.start()

# COMMAND ----------

# MAGIC %md 
# MAGIC # Real time Analytics

# COMMAND ----------

# MAGIC %md
# MAGIC ### Intention

# COMMAND ----------

from sentence_transformers import SentenceTransformer
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from pyspark.sql.types import StructField, StructType, StringType
from pyspark.sql.functions import col, lit

# COMMAND ----------

#load model
model  = SentenceTransformer("bert-base-uncased")

# COMMAND ----------

intention_labels = [
    "Expressing Satisfaction",
    "Voicing Dissatisfaction",
    "Seeking Support",
    "Providing Feedback",
    "Comparing Alternatives",
    "Promoting or Critiquing a Brand",
    "Sharing Tips and Tricks",
    "Expressing Brand Loyalty",
    "Warning about Potential Issues",
    "Seeking Validation"
]

# COMMAND ----------

#takin embedding of list
intention_embeddings =  model.encode(intention_labels)

# COMMAND ----------

def get_embedding(text):
    return model.encode([text])

def get_intention(text):
    embedding = get_embedding(text)
    similarities = cosine_similarity(embedding, intention_embeddings)[0]
    idx = np.argmax(similarities)
    return intention_labels[idx]

get_intention_udf = udf(
    lambda z:get_intention(z),
    StringType()
) 

# COMMAND ----------

intention = spark.table("products.gold.intention_selected")
sentiment = spark.table("products.gold.sentiment_selected")

# COMMAND ----------

intention_query = (
    streaming_amazon_reviews_silver
    .select("reviewID", "reviewText")
    .withColumn("intention", get_intention_udf(col("reviewText")))
    .withColumn("timestamp_ingested", current_timestamp())
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpointLocation_selected_silver + "_intention")
    .option("path", "s3://datapalooza-products-reviews-gold/intention.delta")
)

# COMMAND ----------

intention_query.start()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sentiment

# COMMAND ----------

from pyspark.sql.functions import sha1, concat, lit, col, substring
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import concurrent
import time
from transformers import pipeline

# COMMAND ----------

sentiment_pipeline = pipeline("sentiment-analysis")

# COMMAND ----------

def get_sentiment(text):
    return sentiment_pipeline([text])[0]

get_sentiment_udf = udf(
    lambda z:get_sentiment(z),
    StructType(
        [
            StructField("label", StringType(), True),
            StructField("score", FloatType(), True)
        ]
    )
) 

# COMMAND ----------

sentiment_query = (
    streaming_amazon_reviews_silver
    .select("reviewID", "reviewText")
    .withColumn("sentiment", get_sentiment_udf(col("reviewText")))
    .select("reviewID", "reviewText", "sentiment.*")
    .withColumn("timestamp_ingested", current_timestamp())
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpointLocation_selected_silver + "_sentiment")
    .option("path", "s3://datapalooza-products-reviews-gold/sentiment.delta")
)

# COMMAND ----------

sentiment_query.start()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Platinum count analytics
# MAGIC
# MAGIC

# COMMAND ----------

count_query = (
    streaming_amazon_reviews_silver
    .groupBy()
    .count()
)

# COMMAND ----------

count_query.display()

# COMMAND ----------

count_query_ = (
    count_query
    .writeStream
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation", checkpointLocation_selected_silver + "_count")
    .option("path", "s3://datapalooza-products-reviews-platinum/streaming_count.delta")
)

# COMMAND ----------

count_query_.start()
