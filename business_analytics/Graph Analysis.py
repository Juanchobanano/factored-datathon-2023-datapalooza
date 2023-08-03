# Databricks notebook source
# MAGIC %md
# MAGIC # Graph Analysis

# COMMAND ----------

import pyspark.sql.functions as f
import networkx as nx

# COMMAND ----------

spark.table("products.gold.intention").display()

# COMMAND ----------

spark.table("products.gold.sentiment").display()

# COMMAND ----------

table = ( 
  spark.table("products.silver.amazon_reviews_selected")
  .join(
      spark.table("products.silver.amazon_metadata_silver_selected"),
      on = "asin",
      how = "left"
  )
  .join(
      spark.table("products.gold.sentiment").select("reviewID", "label", "score"), 
      on = "reviewID", 
      how = "inner"
    )
  .join(
      spark.table("products.gold.intention").select("reviewID", "intention"), 
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

asin_also_buy = ( 
  table.select("asin", "also_buy")
  .filter(f.col("also_buy").isNotNull())
  .withColumn("also_buy", f.explode(f.col("also_buy")))
  .withColumnRenamed("asin", "src")
  .withColumnRenamed("also_buy", "dst")
  .withColumn("label", f.lit("ALSO_BUY"))
  .withColumn("weight", f.lit(1))
  .dropDuplicates()
) 
asin_also_buy.display()

# COMMAND ----------

asin_also_view = ( 
  table.select("asin", "also_view")
  .filter(f.col("also_view").isNotNull())
  .withColumn("also_view", f.explode(f.col("also_view")))
  .withColumnRenamed("asin", "src")
  .withColumnRenamed("also_view", "dst")
  .withColumn("label", f.lit("ALSO_VIEW"))
  .withColumn("weight", f.lit(1))
  .dropDuplicates()
) 
asin_also_view.display()

# COMMAND ----------

asin_similar_item = ( 
  table.select("asin", "similar_items")
  .filter(f.col("similar_items").isNotNull())
  .withColumn("similar_items", f.explode(f.col("similar_items")))
  .withColumnRenamed("asin", "src")
  .withColumnRenamed("similar_items", "dst")
  .withColumn("label", f.lit("IS_SIMILAR_TO"))
  .dropDuplicates()
  .withColumn("weight", f.lit(1))
) 
asin_similar_item.display()

# COMMAND ----------

# DBTITLE 1,Join all dataframes
graph = (
    asin_has_sentiment_weight
    #.union(asin_has_review)
    .union(asin_has_intention_weight)
    .union(asin_has_brand)
    .union(asin_also_buy)
    .union(asin_also_view)
    .union(asin_similar_item)
)

# COMMAND ----------

graph.display()

# COMMAND ----------

graph.write.format("delta").saveAsTable("products.gold.products_graph", path = "s3://datapalooza-products-reviews-gold/products_graph.delta")

# COMMAND ----------

graph.groupBy("label").count().display()


# COMMAND ----------

graph.count()

# COMMAND ----------

pandas_communities_data = graph.select("src", "dst", "weight").toPandas()
G = nx.from_pandas_edgelist(pandas_communities_data, "src", "dst", edge_attr="weight")

# COMMAND ----------

centrality =  nx.degree_centrality(G)

# COMMAND ----------

data = {"keys": list(centrality.keys()), "values": list(centrality.values())}

# COMMAND ----------

import pandas as pd
pandas_df = pd.DataFrame(data = data)
pandas_df["values"] = (pandas_df["values"] - pandas_df["values"].min()) / (pandas_df["values"].max() - pandas_df["values"].min())
spark_centrality = spark.createDataFrame(pandas_df)
spark_centrality.display()

# COMMAND ----------

( 
 spark_centrality
 .withColumnRenamed("keys", "node")
 .withColumnRenamed("values", "degree_centrality_normalized")
 .withColumn("degree_centrality_normalized", f.round(f.col("degree_centrality_normalized"), 2))
 .write
 .format("delta")
 .saveAsTable("products.gold.degree_centrality_normalized",
              path = "s3://datapalooza-products-reviews-gold/degree_centrality_normalized.delta")
)

# COMMAND ----------

counter = 0
for node in G:
    subgraph = nx.ego_graph(G, node, radius = 1, center = True, undirected = False, distance=False)
    betweenes_centrality = nx.betweenness_centrality(subgraph)
    closeness_centrality = nx.closeness_centrality(subgraph)
    print(node, " => ", betweenes_centrality[node])
    print(node, " => ", closeness_centrality[node])
    counter += 1
    print("---")
    if counter == 10:
        break

# COMMAND ----------

counter = 0
for node in G:
    subgraph = nx.ego_graph(G, node, radius = 2, center = True, undirected = False, distance=False)
    #betweenes_centrality = nx.betweenness_centrality(subgraph)
    closeness_centrality = nx.closeness_centrality(subgraph)
    #print(node, " => ", betweenes_centrality[node])
    print(node, " => ", closeness_centrality[node])
    counter += 1
    print("---")
    if counter == 10:
        break

# COMMAND ----------

counter = 0
for node in G:
    subgraph = nx.ego_graph(G, node, radius = 2, center = True, undirected = False, distance=False)
    #betweenes_centrality = nx.betweenness_centrality(subgraph)
    page_rank = nx.pagerank(subgraph)
    #print(node, " => ", betweenes_centrality[node])
    print(page_rank)
    counter += 1
    print("---")
    #if counter == 10:
    break

# COMMAND ----------

len(subgraph.edges())

# COMMAND ----------

result = nx.betweenness_centrality(subgraph)

# COMMAND ----------

result["B00JM3R6M6"]

# COMMAND ----------

pandas_communities_data = graph.select("src", "dst").toPandas()
G = nx.from_pandas_edgelist(pandas_communities_data, "src", "dst")
connected_components = nx.connected_components(G)
data = list()
idx = 1
for community in connected_components:
    obj = {"community_id": idx, "nodes": list(community)}
    idx += 1
    data.append(obj)
    
partition = Window().orderBy(lit("A"))
communities = spark.createDataFrame(pd.DataFrame(data))
communities = (
    communities.withColumn("community_size", size(col("nodes"))).orderBy(col("community_size").desc())
    .withColumn("community_id", row_number().over(partition))
    .select("community_id", explode(col("nodes")).alias("id"))
    .join(nodes, on = "id", how = "left")
    .filter("label == 'ad'")
    .select("id", "community_id")
)
result = (
    edges_communities.join(communities, edges_communities["src"] == communities["id"], how = "left").drop("id")
)
result = result.union(edges_location.join(result.select("src", "community_id"), on = "src", how = "left"))
result = result.filter("community_id IS NOT NULL") # filter relationship with NULL community_id
result.display()
