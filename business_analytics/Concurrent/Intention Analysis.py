# Databricks notebook source
from sentence_transformers import SentenceTransformer
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from pyspark.sql.types import StructField, StructType, StringType
from pyspark.sql.functions import col, lit

# COMMAND ----------

df = spark.table("products.silver.amazon_reviews_selected")

# COMMAND ----------

df.count()

# COMMAND ----------

#load model
model  = SentenceTransformer("bert-base-uncased")
# see the details of model with print
print(model)

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

df_ = (
    df
    .select("reviewID", "reviewText")
    # .limit(1000)
)

# COMMAND ----------

intention_df = (
    df_
    .withColumn("intention", get_intention_udf(col("reviewText")))
)

# COMMAND ----------

# intention_df.display()

# COMMAND ----------

(
    intention_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "products.gold.intention_selected",
        path = "s3://datapalooza-products-reviews-gold/intention_selected.delta"
    )
)
