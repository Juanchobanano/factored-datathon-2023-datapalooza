# Databricks notebook source
# MAGIC %md
# MAGIC # Reviews Metrics

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG products;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Recuento de reseñas por calificación (overall)
# MAGIC CREATE TABLE products.platinum.count_reviews_per_overall AS (
# MAGIC   SELECT overall, COUNT(*) AS count
# MAGIC   FROM products.silver.amazon_reviews_selected
# MAGIC   GROUP BY overall
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Recuento de reseñas por categoría de producto
# MAGIC SELECT category, COUNT(*) AS count
# MAGIC FROM products.silver.amazon_reviews_selected
# MAGIC GROUP BY category;

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

# MAGIC %sql
# MAGIC -- Promedio de calificaciones por producto (ASIN).
# MAGIC SELECT asin, AVG(overall) AS avg_rating
# MAGIC FROM products.silver.amazon_reviews_selected
# MAGIC GROUP BY asin;

# COMMAND ----------

#TODO: incluir el número de reseñas

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Total de reseñas por revisor (reviewerID)
# MAGIC SELECT reviewerID, COUNT(*) AS total_reviews
# MAGIC FROM products.silver.amazon_reviews_selected
# MAGIC GROUP BY reviewerID;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Recuento de reseñas por categoría y calificación (overall)
# MAGIC SELECT category, overall, COUNT(*) AS count
# MAGIC FROM products.silver.amazon_reviews_selected
# MAGIC GROUP BY category, overall;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Recuento de reseñas verificadas y no verificadas por categoría
# MAGIC SELECT category, 
# MAGIC        SUM(CASE WHEN verified = 'true' THEN 1 ELSE 0 END) AS verified_count,
# MAGIC        SUM(CASE WHEN verified = 'false' THEN 1 ELSE 0 END) AS unverified_count
# MAGIC FROM products.silver.amazon_reviews_selected
# MAGIC GROUP BY category;

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

# MAGIC %md
# MAGIC # Metadata Metrics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Total de productos por categoría principal (main_category)
# MAGIC SELECT main_category, COUNT(*) AS total_products
# MAGIC FROM products.silver.amazon_metadata_silver_selected
# MAGIC GROUP BY main_category;

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

# MAGIC %sql
# MAGIC -- Cantidad de productos ingresados por fecha de ingreso (timestamp_ingested)
# MAGIC SELECT DATE(timestamp_ingested) AS ingested_date, COUNT(*) AS count
# MAGIC FROM products.silver.amazon_metadata_silver_selected
# MAGIC GROUP BY ingested_date;

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

# MAGIC %sql
# MAGIC -- Análisis de Palabras Comunes en Descripciones de Productos por Categoría
# MAGIC SELECT 
# MAGIC     t1.main_category AS category,
# MAGIC     word, 
# MAGIC     COUNT(*) AS count
# MAGIC FROM products.silver.amazon_metadata_silver_selected t1
# MAGIC JOIN products.silver.amazon_reviews_selected t2 ON t1.asin = t2.asin
# MAGIC CROSS JOIN (
# MAGIC     SELECT SPLIT(t1.description, ' ') AS words
# MAGIC     FROM products.silver.amazon_metadata_silver_selected t1
# MAGIC ) words_table
# MAGIC LATERAL VIEW explode(words_table.words) w AS word
# MAGIC GROUP BY t1.main_category, word
# MAGIC ORDER BY t1.main_category, count DESC;

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

# MAGIC %sql
# MAGIC -- Cantidad de Productos Similares entre Video Juegos y Software
# MAGIC SELECT 
# MAGIC     t1.main_category AS category,
# MAGIC     COUNT(DISTINCT t1.also_buy) AS also_buy_count,
# MAGIC     COUNT(DISTINCT t1.also_view) AS also_view_count
# MAGIC FROM products.silver.amazon_metadata_silver_selected t1
# MAGIC JOIN products.silver.amazon_reviews_selected t2 ON t1.asin = t2.asin
# MAGIC GROUP BY t1.main_category;
