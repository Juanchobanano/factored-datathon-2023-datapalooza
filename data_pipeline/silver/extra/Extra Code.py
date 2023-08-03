# Databricks notebook source
for file_ in dbutils.fs.ls(BRONZE_BUCKET_NAME + "/amazon_metadata_bronze.delta/"):
    print(f"Processing batch {file_} ...")
    table = (
        spark.read.format("parquet").load(file_.path + "*")
    )

    for i in range(len(table.columns)):
        table = table.withColumnRenamed(table.columns[i], bronze_columns[i])
    table.display()
    break
