# Databricks notebook source
# MAGIC %md
# MAGIC # Reviews Metrics

# COMMAND ----------

platinum_bucket = "s3://datapalooza-products-reviews-platinum"

# COMMAND ----------

def save_metric(name):    
    (
        _sqldf
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(
            name = f"products.platinum.{name}",
            path = f"{platinum_bucket}/{name}.delta"
        )
    )
    print(f"Succesfully writed table:")
    print(f"\tproducts.platinum.{name}")
    print(f"\t{platinum_bucket}/{name}.delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Recuento de reseñas por calificación (overall)
# MAGIC SELECT overall, COUNT(*) AS count
# MAGIC FROM products.silver.amazon_reviews_selected
# MAGIC GROUP BY overall

# COMMAND ----------

name = "count_reviews_per_overall"
save_metric(name)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Recuento de reseñas por categoría de producto
# MAGIC SELECT category, COUNT(*) AS count
# MAGIC FROM products.silver.amazon_reviews_selected
# MAGIC GROUP BY category;

# COMMAND ----------

name = "count_reviews_per_category"
save_metric(name)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Distribución de reseñas verificadas y no verificadas
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC         WHEN verified = 'true' THEN 'Verificado'
# MAGIC         ELSE 'No verificado'
# MAGIC     END AS verification_status,
# MAGIC     COUNT(*) AS count
# MAGIC FROM products.silver.amazon_reviews_selected
# MAGIC GROUP BY verification_status;

# COMMAND ----------

name = "verified_distribution"
save_metric(name)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Distribución de reseñas con y sin voto
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC         WHEN vote IS NOT NULL THEN 'Votado'
# MAGIC         ELSE 'Sin voto'
# MAGIC     END AS vote_status,
# MAGIC     COUNT(*) AS count
# MAGIC FROM products.silver.amazon_reviews_selected
# MAGIC GROUP BY vote_status;

# COMMAND ----------

name = "vote_distribution"
save_metric(name)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Promedio de calificaciones por producto (ASIN).
# MAGIC SELECT asin, AVG(overall) AS avg_rating, count(asin) AS count
# MAGIC FROM products.silver.amazon_reviews_selected
# MAGIC GROUP BY asin;

# COMMAND ----------

name =  "mean_ratings_per_asin"
save_metric(name)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Total de reseñas por revisor (reviewerID)
# MAGIC SELECT reviewerID, COUNT(*) AS total_reviews
# MAGIC FROM products.silver.amazon_reviews_selected
# MAGIC GROUP BY reviewerID;

# COMMAND ----------

name = "total_reviews_per_reviewerid"
save_metric(name)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Recuento de reseñas por categoría y calificación (overall)
# MAGIC SELECT category, overall, COUNT(*) AS count
# MAGIC FROM products.silver.amazon_reviews_selected
# MAGIC GROUP BY category, overall;

# COMMAND ----------

name = "count_reviews_per_Category_and_overall"
save_metric(name)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Recuento de reseñas verificadas y no verificadas por categoría
# MAGIC SELECT category, 
# MAGIC        SUM(CASE WHEN verified = 'true' THEN 1 ELSE 0 END) AS verified_count,
# MAGIC        SUM(CASE WHEN verified = 'false' THEN 1 ELSE 0 END) AS unverified_count
# MAGIC FROM products.silver.amazon_reviews_selected
# MAGIC GROUP BY category;

# COMMAND ----------

name = "count_verified_reviews_per_category"
save_metric(name)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Contenido de los campos reviewText y summary.
# MAGIC SELECT reviewText, summary
# MAGIC FROM products.silver.amazon_reviews_selected;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Promedio de palabras por reseña (reviewText)
# MAGIC SELECT AVG(LENGTH(reviewText) - LENGTH(REPLACE(reviewText, ' ', '')) + 1) AS avg_words
# MAGIC FROM products.silver.amazon_reviews_selected;

# COMMAND ----------

name = "avg_words_per_review"
save_metric(name)

# COMMAND ----------

# MAGIC %md
# MAGIC # Metadata Metrics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Total de productos por categoría principal (main_category)
# MAGIC SELECT main_category, COUNT(*) AS total_products
# MAGIC FROM products.silver.amazon_metadata_silver_selected
# MAGIC GROUP BY main_category;

# COMMAND ----------

name =  "total_products_per_category"
save_metric(name)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Total de productos por subcategoría (category)
# MAGIC SELECT category, COUNT(*) AS total_products
# MAGIC FROM products.silver.amazon_metadata_silver_selected
# MAGIC GROUP BY category;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Total de productos por marca (brand)
# MAGIC SELECT brand, COUNT(*) AS total_products
# MAGIC FROM products.silver.amazon_metadata_silver_selected
# MAGIC GROUP BY brand;

# COMMAND ----------

name = "total_products_per_brand"
save_metric(name)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Distribución de precios medios de productos (mean_price)
# MAGIC SELECT mean_price, COUNT(*) AS total_products
# MAGIC FROM products.silver.amazon_metadata_silver_selected
# MAGIC GROUP BY mean_price;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Precio máximo y mínimo de los productos
# MAGIC SELECT MAX(mean_price) AS max_price, MIN(mean_price) AS min_price
# MAGIC FROM products.silver.amazon_metadata_silver_selected;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Total de productos con enlaces "also_buy"
# MAGIC SELECT COUNT(also_buy) AS also_buy_count
# MAGIC FROM products.silver.amazon_metadata_silver_selected
# MAGIC WHERE also_buy IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Total de productos con enlaces "also_view"
# MAGIC SELECT COUNT(also_view) AS also_view_count
# MAGIC FROM products.silver.amazon_metadata_silver_selected
# MAGIC WHERE also_view IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Rango de clasificación (rank) máximo y mínimo de los productos
# MAGIC SELECT MAX(rank) AS max_rank, MIN(rank) AS min_rank
# MAGIC FROM products.silver.amazon_metadata_silver_selected;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Distribución de productos con y sin descripción
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC         WHEN description IS NOT NULL THEN 'Con descripción'
# MAGIC         ELSE 'Sin descripción'
# MAGIC     END AS description_status,
# MAGIC     COUNT(*) AS count
# MAGIC FROM products.silver.amazon_metadata_silver_selected
# MAGIC GROUP BY description_status;

# COMMAND ----------

name = "distribution_of_description"
save_metric(name)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Distribución de productos con y sin imagen
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC         WHEN image IS NOT NULL THEN 'Con imagen'
# MAGIC         ELSE 'Sin imagen'
# MAGIC     END AS image_status,
# MAGIC     COUNT(*) AS count
# MAGIC FROM products.silver.amazon_metadata_silver_selected
# MAGIC GROUP BY image_status;

# COMMAND ----------

name = "distrobution_images"
save_metric(name)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cantidad de productos ingresados por fecha de ingreso (timestamp_ingested)
# MAGIC SELECT DATE(timestamp_ingested) AS ingested_date, COUNT(*) AS count
# MAGIC FROM products.silver.amazon_metadata_silver_selected
# MAGIC GROUP BY ingested_date;

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC -- Total de productos por combinación de categoría principal y subcategoría
# MAGIC SELECT main_category, category, COUNT(*) AS count
# MAGIC FROM products.silver.amazon_metadata_silver_selected
# MAGIC GROUP BY main_category, category;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Total de productos con enlaces "similar_items" por ASIN
# MAGIC SELECT asin, COUNT(similar_items) AS similar_items_count
# MAGIC FROM products.silver.amazon_metadata_silver_selected
# MAGIC GROUP BY asin;

# COMMAND ----------

name = "similar_items_per_asin"
save_metric(name)

# COMMAND ----------

# MAGIC %md
# MAGIC # Joint Metrics
# MAGIC

# COMMAND ----------

spark.table("products.silver.amazon_metadata_silver_selected")

# COMMAND ----------

spark.table("products.silver.amazon_reviews_selected")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Distribución de Calificaciones por Categoría de Producto
# MAGIC SELECT 
# MAGIC     t1.main_category AS category,
# MAGIC     AVG(t2.overall) AS avg_rating
# MAGIC FROM products.silver.amazon_metadata_silver_selected t1
# MAGIC JOIN products.silver.amazon_reviews_selected t2 ON t1.asin = t2.asin
# MAGIC GROUP BY t1.main_category;

# COMMAND ----------

name = "overall_distribution_per_product"
save_metric(name)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Recuento de Reviews por Producto y Categoría
# MAGIC SELECT 
# MAGIC     t1.asin,
# MAGIC     t1.main_category AS category,
# MAGIC     COUNT(*) AS total_reviews
# MAGIC FROM products.silver.amazon_metadata_silver_selected t1
# MAGIC JOIN products.silver.amazon_reviews_selected t2 ON t1.asin = t2.asin
# MAGIC GROUP BY t1.asin, t1.main_category;

# COMMAND ----------

name = "count_reviews_per_product_per_category"
save_metric(name)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Comparativa de Precios Promedio entre Video Juegos y Software
# MAGIC SELECT 
# MAGIC     t1.main_category AS category, 
# MAGIC     AVG(t1.mean_price) AS avg_price
# MAGIC FROM products.silver.amazon_metadata_silver_selected t1
# MAGIC JOIN products.silver.amazon_reviews_selected t2 ON t1.asin = t2.asin
# MAGIC GROUP BY t1.main_category;

# COMMAND ----------

name = "price_comparison_between_categories"
save_metric(name)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cantidad de Productos Similares entre Video Juegos y Software
# MAGIC SELECT 
# MAGIC     t1.main_category AS category,
# MAGIC     COUNT(DISTINCT t1.also_buy) AS also_buy_count,
# MAGIC     COUNT(DISTINCT t1.also_view) AS also_view_count
# MAGIC FROM products.silver.amazon_metadata_silver_selected t1
# MAGIC JOIN products.silver.amazon_reviews_selected t2 ON t1.asin = t2.asin
# MAGIC GROUP BY t1.main_category;

# COMMAND ----------

name = "count_similar_products_between_Categories"
save_metric(name)

# COMMAND ----------


