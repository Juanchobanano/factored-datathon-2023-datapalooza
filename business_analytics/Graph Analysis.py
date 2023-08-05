# Databricks notebook source
# MAGIC %md
# MAGIC # Graph Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Notebook settings

# COMMAND ----------

# DBTITLE 1,Import libraries
import pyspark.sql.functions as f
import networkx as nx
import pandas as pd
import numpy as np
from tqdm import tqdm 

# COMMAND ----------

# DBTITLE 1,Notebook constants
GOLD_BUCKET_NAME = "s3://datapalooza-products-reviews-gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Graph

# COMMAND ----------

table = ( 
  spark.table("products.silver.amazon_reviews_selected")
  .join(
      spark.table("products.silver.amazon_metadata_silver_selected"),
      on = "asin",
      how = "inner"
  )
  .join(
      spark.table("products.gold.sentiment_selected").select("reviewID", "label", "score"), 
      on = "reviewID", 
      how = "inner"
    )
  .join(
      spark.table("products.gold.intention_selected").select("reviewID", "intention"), 
      on = "reviewID", 
      how = "inner"
  )
)
table.display()

# COMMAND ----------

# DBTITLE 1,ASIN has sentiment weight
asin_has_sentiment_weight = (
    table.select("asin", "label")
    .groupBy(["asin", "label"])
    .count()
    .withColumnRenamed("asin", "src")
    .withColumnRenamed("label", "dst")
    .withColumn("label", f.lit("HAS_SENTIMENT"))
    .withColumnRenamed("count", "weight")
    .select("src", "dst", "label", "weight")
)
asin_has_sentiment_weight.display()

# COMMAND ----------

# DBTITLE 1,ASIN has review
asin_has_review = (
    table.select("asin", "reviewID", "reviewText")
    .withColumnRenamed("asin", "src")
    .withColumnRenamed("reviewID", "dst")
    .withColumn("label", f.lit("HAS_REVIEW"))
    .withColumn("weight", f.lit(1))
    .drop("reviewText")
)
asin_has_review.display()

# COMMAND ----------

# DBTITLE 1,ASIN has intention weight
asin_has_intention_weight = (
    table.select("asin", "intention")
    .groupBy(["asin", "intention"])
    .count()
    .withColumnRenamed("asin", "src")
    .withColumnRenamed("intention", "dst")
    .withColumn("intention", f.lit("HAS_INTENTION"))
    .withColumnRenamed("count", "weight")
    .select("src", "dst", "intention", "weight")
    .withColumn("weight", f.lit(1))
)
asin_has_intention_weight.display()

# COMMAND ----------

# DBTITLE 1,Asin has brand (brand has asin)
asin_has_brand = (
    spark.table("products.silver.amazon_metadata_silver_selected")
    .filter((f.col("brand").isNotNull()) & (f.col("brand") != "."))
    .select("asin", "brand")
    .withColumnRenamed("asin", "src")
    .withColumnRenamed("brand", "dst")
    .withColumn("label", f.lit("HAS_BRAND"))
    .withColumn("weight", f.lit(1))
)
asin_has_brand.display()

# COMMAND ----------

# DBTITLE 1,Asins to consider
asins_to_consider = [x["asin"] for x in table.select("asin").distinct().collect()]
brands = [x["brand"] for x in table.select("brand").distinct().collect()]

# COMMAND ----------

# DBTITLE 1,Asin also buy
asin_also_buy = ( 
  table.select("asin", "also_buy")
  .filter(f.col("also_buy").isNotNull())
  .withColumn("also_buy", f.explode(f.col("also_buy")))
  .withColumnRenamed("asin", "src")
  .withColumnRenamed("also_buy", "dst")
  .withColumn("label", f.lit("ALSO_BUY"))
  .withColumn("weight", f.lit(1))
  .withColumn("consider", f.when(f.col("dst").isin(asins_to_consider), f.lit(True)).otherwise(f.lit(False)))
  .filter(f.col("consider") == True)
  .drop("consider")
  .dropDuplicates()
) 
asin_also_buy.display()

# COMMAND ----------

# DBTITLE 1,Asin also view
asin_also_view = ( 
  table.select("asin", "also_view")
  .filter(f.col("also_view").isNotNull())
  .withColumn("also_view", f.explode(f.col("also_view")))
  .withColumnRenamed("asin", "src")
  .withColumnRenamed("also_view", "dst")
  .withColumn("label", f.lit("ALSO_VIEW"))
  .withColumn("weight", f.lit(1))
  .withColumn("consider", f.when(f.col("dst").isin(asins_to_consider), f.lit(True)).otherwise(f.lit(False)))
  .filter(f.col("consider") == True)
  .drop("consider")
  .dropDuplicates()
) 
asin_also_view.display()

# COMMAND ----------

# DBTITLE 1,Asin similar items
asin_similar_item = ( 
  table.select("asin", "similar_items")
  .filter(f.col("similar_items").isNotNull())
  .withColumn("similar_items", f.explode(f.col("similar_items")))
  .withColumnRenamed("asin", "src")
  .withColumnRenamed("similar_items", "dst")
  .withColumn("label", f.lit("IS_SIMILAR_TO"))
  .withColumn("weight", f.lit(1))
  .withColumn("consider", f.when(f.col("dst").isin(asins_to_consider), f.lit(True)).otherwise(f.lit(False)))
  .filter(f.col("consider") == True)
  .drop("consider")
  .dropDuplicates()

) 
asin_similar_item.display()

# COMMAND ----------

# DBTITLE 1,Asin has category
asin_has_category = ( 
  table.select("asin", "products.silver.amazon_reviews_selected.category")
  .dropDuplicates()
  .withColumn("label", f.lit("HAS_CATEGORY"))
  .withColumnRenamed("asin", "src")
  .withColumnRenamed("category", "dst")
  .withColumn("weight", f.lit(1))
) 
asin_has_category.display()

# COMMAND ----------

# DBTITLE 1,Join all dataframes
# Get graph
graph = (
    asin_has_sentiment_weight
    .union(asin_has_review)
    .union(asin_has_intention_weight)
    .union(asin_has_brand)
    .union(asin_also_buy)
    .union(asin_also_view)
    .union(asin_similar_item)
    .union(asin_has_category)
)

# COMMAND ----------

# DBTITLE 1,Visualize graph
graph.groupBy("label").count().display()

# COMMAND ----------

graph.write.format("delta").mode("overwrite").saveAsTable("products_graph")

# COMMAND ----------

len(G.nodes())

# COMMAND ----------

len(G.edges())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Metrics

# COMMAND ----------

# DBTITLE 1,Compute Degree Centrality
# Only consider asins and brands
degree_centrality_graph = graph.filter(
    (f.col("label") == 'HAS_REVIEW') |
    (f.col("label") == 'ALSO_BUY') |
    (f.col("label") == 'ALSO_VIEW') |
    (f.col("label") == 'IS_SIMILAR_TO')
)

pandas_communities_data = degree_centrality_graph.select("src", "dst", "weight").toPandas()
G = nx.from_pandas_edgelist(pandas_communities_data, "src", "dst", edge_attr="weight")

centrality =  nx.degree_centrality(G)
data = {"node": list(centrality.keys()), "degree_centrality": list(centrality.values()), "degree_centrality_normalized": list(centrality.values())}
pandas_df = pd.DataFrame(data = data)
pandas_df["degree_centrality_normalized"] = (pandas_df["degree_centrality_normalized"] - pandas_df["degree_centrality_normalized"].min()) / (pandas_df["degree_centrality_normalized"].max() - pandas_df["degree_centrality_normalized"].min())
degree_centrality = spark.createDataFrame(pandas_df)
degree_centrality.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("products.gold.degree_centrality_normalized", path = f"{GOLD_BUCKET_NAME}/degree_centrality_normalized.delta")

# COMMAND ----------

# DBTITLE 1,Betweenness Centrality
pandas_communities_data = graph.select("src", "dst", "weight").toPandas()
G = nx.from_pandas_edgelist(pandas_communities_data, "src", "dst", edge_attr="weight")

counter = 0
bet_results = list()

for node in tqdm(G):

    # Only consider asins and brands
    if not ((node in asins_to_consider) or (node in brands)):
        continue

    # Get subgraph
    subgraph = nx.ego_graph(G, node, radius = 1, center = True, undirected = False, distance=False)
    betweennes_centrality_score = nx.betweenness_centrality(subgraph)[node]
    data = {"node": node, 
            "betweenes_centrality": betweennes_centrality_score, 
            "betweenes_centrality_normalized": betweennes_centrality_score}
    bet_results.append(data)

# COMMAND ----------

len(bet_results)

# COMMAND ----------

betweennes_centrality_pd = pd.DataFrame(bet_results)
betweennes_centrality_pd["betweenes_centrality_normalized"] = (betweennes_centrality_pd["betweenes_centrality_normalized"] - betweennes_centrality_pd["betweenes_centrality_normalized"].min()) / (betweennes_centrality_pd["betweenes_centrality_normalized"].max() - betweennes_centrality_pd["betweenes_centrality_normalized"].min())
betweenes_centrality = spark.createDataFrame(betweennes_centrality_pd)

# COMMAND ----------

betweenes_centrality.display()

# COMMAND ----------

betweenes_centrality.count()

# COMMAND ----------

betweenes_centrality.write.format("delta").mode("overwrite").saveAsTable("products.gold.betweenes_centrality", path = f"{GOLD_BUCKET_NAME}/betweenes_centrality.delta")

# COMMAND ----------

graph = spark.table("products.gold.products_graph")

# COMMAND ----------

graph.display()

# COMMAND ----------

graph.select("label").distinct().display()

# COMMAND ----------

graph.coalesce(1).write.format("parquet").mode("overwrite").save("s3://datapalooza-products-reviews-gold/graph.parquet")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM products.gold.products_graph
# MAGIC WHERE src = 'B0009J1JJI';

# COMMAND ----------



# COMMAND ----------

products_graph = spark.table("products.gold.products_graph").toPandas()

# COMMAND ----------

import pandas as pd

# Assuming you already have the data in a DataFrame named products_graph
# If not, load the data into a DataFrame using the appropriate method.

# Step 1: Filter g1
g1 = products_graph[products_graph['src'] == 'B00005YDIC']

# Step 2: Join g1 and g2
g2 = pd.merge(g1[['dst']], products_graph[['src', 'dst', 'label']], left_on='dst', right_on='dst', suffixes=('', '_g2'))

# Step 3: Join g2 and g3
result = pd.merge(g2[['src', 'dst', 'label']], products_graph[['src', 'dst', 'label']], left_on='src', right_on='src', suffixes=('_g2', ''))

# Select the desired columns from the final result
result = result[['src', 'dst', 'label']]

# Display the final result
print(result)

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC WITH g1 AS
# MAGIC ( SELECT *
# MAGIC   FROM products.gold.products_graph
# MAGIC   WHERE src = 'B00005YDIC'
# MAGIC ),
# MAGIC g2 AS 
# MAGIC (
# MAGIC   SELECT g2.src, g2.dst, g2.label
# MAGIC   FROM g1
# MAGIC   JOIN products.gold.products_graph g2 ON g1.dst = g2.dst
# MAGIC )
# MAGIC SELECT g3.src, g3.dst, g3.label
# MAGIC FROM g2
# MAGIC JOIN products.gold.products_graph g3 ON g2.src = g3.src;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM g2;hghh

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT src, dst, label, weight
# MAGIC FROM products.gold.products_graph g1
# MAGIC WHERE src = '0078764343'
# MAGIC INNERJOIN products.gold.products_graph g2 ON (g1.dst = g2.src)

# COMMAND ----------

graph.filter(f.col("src") == '0078764343').display()

# COMMAND ----------

graph.filter(f.col("dst") == 'Ea Games').display()

# COMMAND ----------

graph.filter(f.col("src") == 'B0000ARQMZ').display()

# COMMAND ----------

spark.table("products.silver.amazon_reviews_selected").coalesce(1).write.format("parquet").save("s3://datapalooza-products-reviews-silver/amazon_reviews_selected.parquet")

# COMMAND ----------

spark.table("products.silver.amazon_metadata_silver_selected").coalesce(1).write.format("parquet").save("s3://datapalooza-products-reviews-silver/amazon_metadata_silver_selected.parquet")

# COMMAND ----------

spark.table("products.gold.betweenes_centrality").coalesce(1).write.format("parquet").save("s3://datapalooza-products-reviews-silver/betweenes_centrality.parquet")

# COMMAND ----------

spark.table("products.gold.degree_centrality_normalized").coalesce(1).write.format("parquet").mode("overwrite").save("s3://datapalooza-products-reviews-gold/degree_centrality_normalized.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC ## We are done!

# COMMAND ----------

graph = spark.table("products.gold.products_graph")
graph.display()

# COMMAND ----------

spark.table("products.gold.degree_centrality_normalized").filter(f.col("node") == 'B003DSAT0C').display()

# COMMAND ----------

import pyspark.sql.functions as f
graph.filter(f.col("src") == '0439339006').display()

# COMMAND ----------

degree_centrality = spark.table("products.gold.degree_centrality")

# COMMAND ----------

degree_centrality.display()

# COMMAND ----------


