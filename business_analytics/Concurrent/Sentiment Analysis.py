# Databricks notebook source
from pyspark.sql.functions import sha1, concat, lit, col, substring
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import concurrent
import time
from transformers import pipeline

# COMMAND ----------

df = spark.table("products.silver.amazon_reviews_selected")

# COMMAND ----------

df.count()

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

df_ = (
    df
    .select("reviewID", "reviewText")
    # .limit(1000)
)

# COMMAND ----------

sentiment_df = (
    df_
    .withColumn("sentiment", get_sentiment_udf(col("reviewText")))
    .select("reviewID", "reviewText", "sentiment.*")
)

# COMMAND ----------

(
    sentiment_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "products.gold.sentiment_selected",
        path = "s3://datapalooza-products-reviews-gold/sentiment_selected.delta"
    )
)

# COMMAND ----------


