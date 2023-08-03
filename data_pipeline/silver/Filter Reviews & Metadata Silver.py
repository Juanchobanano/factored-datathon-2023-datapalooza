# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

silver_metadata =  spark.table("products.silver.amazon_metadata_silver")
silver_reviews = spark.table("products.silver.amazon_reviews_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE TABLE products.silver.amazon_metadata_silver;

# COMMAND ----------

silver_metadata.select("main_category", "category", "title").where(f.col("main_category") == 'Software').distinct().display()

# COMMAND ----------

filter_ = silver_metadata.select("asin", "main_category") #.where((f.col("main_category") == "Electronics") | (f.col("main_category") == "Cell Phones & Accessories"))
selected_reviews = silver_reviews.join(
    filter_,
    on = "asin", 
    how = "left"
)#.groupBy("main_category").count().display()

# COMMAND ----------

spark.table("products.silver.amazon_metadata_silver").where(f.col("asin") == 'B00000K4DK').display()

# COMMAND ----------

#silver_metadata = (
(
    spark.table("products.silver.amazon_metadata_silver")
    .where((f.col("main_category") == 'Video Games') | (f.col("main_category") == 'Software') )
    .dropDuplicates(["asin"]) 
    .withColumn("span_position", f.array_position(f.col("category"), "</span></span></span>"))
    .withColumn("span_position", f.when(f.col("span_position") == 0, f.lit(None)).otherwise(f.col("span_position")))
    .select("asin", "main_category", "category", "span_position")
    #.withColumn("category", f.slice(f.col("category"), 1, f.col("span_position")))
    .withColumn("category", 
                f.when(
                    f.col("span_position").isNotNull(), f.slice(f.col("category"), 1, f.col("span_position") - 1))
                    .otherwise(f.col("category")
                )
    )
    .withColumn("category_explode", f.explode(f.col("category")))
    .withColumn("category_explode", f.regexp_replace(f.col("category_explode"), "&amp;", "&"))
    .withColumn("category_string_length", f.length("category_explode"))
    .filter(f.col("category_string_length") <= 32)
    .orderBy(f.col("category_string_length").desc())
    .groupBy("asin", "main_category", "category")
    .agg(f.collect_list("category_explode").alias("collect_categories"))
    .drop("category")
    .withColumnRenamed("collect_categories", "category")
    .createOrReplaceTempView("categories_fixed")
)
#silver_metadata.display()

# COMMAND ----------

from pyspark.sql.types import *
def filter_images(images):
    if images is None:
        return None
    filtered_images = list()
    for image in images:
        if image.find("http") != -1:
            filtered_images.append(image)
    return filtered_images
udf_filter_images = f.udf(lambda z: filter_images(z), ArrayType(StringType()))

# COMMAND ----------

( 
    spark.table("products.silver.amazon_metadata_silver")
    .where((f.col("main_category") == 'Video Games') | (f.col("main_category") == 'Software') )
    .dropDuplicates(["asin"]) 
    .withColumn("title", f.regexp_replace(f.col("title"), "&trade;", " "))
    .withColumn("title", f.regexp_replace(f.col("title"), "&amp;", "&"))
    .withColumn("brand", f.trim(f.regexp_replace(f.col("brand"), "\n", "")))
    .withColumn("brand", f.trim(f.regexp_replace(f.col("brand"), "By", "")))
    .withColumn("image", udf_filter_images(f.col("image")))
    .drop("category")
    .createOrReplaceTempView("filter_and_process_metadata")
)

# COMMAND ----------

spark.table("categories_fixed").display()

# COMMAND ----------

spark.table("filter_and_process_metadata").display()

# COMMAND ----------

columns = spark.table("products.silver.amazon_metadata_silver").columns

# COMMAND ----------

columns

# COMMAND ----------

final_metadata_table = spark.table("categories_fixed").join(spark.table("filter_and_process_metadata"), on = ["asin", "main_category"], how = "left").select(columns) #.display()

# COMMAND ----------

final_metadata_table.write.format("delta").mode("overwrite").saveAsTable("products.silver.amazon_metadata_silver_selected", path = "s3://datapalooza-products-reviews-silver/amazon_metadata_silver_selected.delta")

# COMMAND ----------

final_metadata_table.count()

# COMMAND ----------

final_metadata_table.select("asin", "main_category").write.format("delta").mode("overwrite").saveAsTable("products.silver.amazon_categories_selected", path = "s3://datapalooza-products-reviews-silver/amazon_categories_selected")

# COMMAND ----------

final_metadata_table.select("main_category").distinct().display()

# COMMAND ----------

reorder_columns = ["asin", "category", "image", "overall", "reviewerID", "reviewerName", "reviewText", "summary", "date", "verified", "vote", "source", "timestamp_ingested"]
silver_reviews = (
    spark.table("products.silver.amazon_reviews_silver")
    .join(final_metadata_table.select("asin", "main_category"), on = ["asin"], how = "inner")
    .withColumnRenamed("main_category", "category")
    .select(reorder_columns)
)

# COMMAND ----------

silver_reviews.printSchema()

# COMMAND ----------

silver_reviews.groupBy("category").count().display()

# COMMAND ----------

silver_reviews.write.format("delta").mode("overwrite").saveAsTable("products.silver.amazon_reviews_selected", path = "s3://datapalooza-products-reviews-silver/amazon_reviews_selected")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM categories_fixed;

# COMMAND ----------

selected_reviews = selected_reviews.where((f.col("main_category") == 'Video Games') | (f.col("main_category") == 'Software'))

# COMMAND ----------

selected_reviews.display()

# COMMAND ----------

selected_reviews.groupBy("main_category").count().display()

# COMMAND ----------

selected_reviews.count()

# COMMAND ----------

silver_metadata.display()

# COMMAND ----------

silver_metadata.groupBy("main_category").count().display()

# COMMAND ----------

silver_reviews.display()
