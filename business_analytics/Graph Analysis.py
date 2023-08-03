# Databricks notebook source
silver = spark.table("products.silver.amazon_metadata_silver")

# COMMAND ----------

silver.columns

# COMMAND ----------

silver_reviews = spark.table("products.silver.amazon_reviews_silver")
silver_metadata = spark.table("products.silver.amazon_metadata_silver")

# COMMAND ----------

filter_ = silver_metadata.select("asin", "main_category").where((f.col("main_category") == "Electronics") | (f.col("main_category") == "Cell Phones & Accessories"))

# COMMAND ----------

filter_ = silver_metadata.select("asin", "main_category").where((f.col("main_category") == "Electronics") | (f.col("main_category") == "Cell Phones & Accessories"))
silver_reviews.join(
    filter_,
    on = "asin", 
    how = "left"
).groupBy("main_category").count().display()

# COMMAND ----------

4697813 + 3327913

# COMMAND ----------

import pyspark.sql.functions as f
silver_metadata.groupBy(f.col("main_category")).count().display()

# COMMAND ----------

silver_reviews = spark.table("products.silver.amazon_reviews_selected")

# COMMAND ----------

silver_reviews.display()

# COMMAND ----------

import pyspark.sql.functions as f 
silver_reviews = silver_reviews.withColumn("reviewLength", f.length(f.col("reviewText")))
silver_reviews.agg(f.mean(f.col("reviewLength"))).display()

# COMMAND ----------

(484.7171385811831 / 4) * 1796698

# COMMAND ----------

(217722578 / 1000) * 0.0001

# COMMAND ----------

$0.0001 / 1K tokens


# COMMAND ----------

silver_reviews.count()

# COMMAND ----------

# General Metrics => Tablero 1 
# agregados, series de tiempo, etc. => EDUARDS Y DAVID

# Análisis de sentimientos (NICE)
# notebook <= libreria que ud utilizo

# Análisis de intenciones (NICE)
# Guardar los embeddings de las reviews
# notebook <= Open AI

# Calcular los embeddings de los títulos de los productos (NICE)
# para poder hacer búsqueda
# notebook
# revisar pinecone (ver si es viable)

# PRE-REQUISITO: SENTIMIENTOS E INTENCIONES
# Grafo
# notebook


# Nombre de producto => click => lambda => calcula la distancia del coseno con los embeddings de los títulos => retorna información


# STREAMLIT 
# Alguien que se dedique a esto
