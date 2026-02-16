# Databricks notebook source
# Read from S3
df = spark.read.json("s3://raw-bucket/sales/")

# Incremental filter
df_new = df.filter(col("updated_at") > last_max_ts)

# Deduplicate
df_final = df_new.dropDuplicates(["id"])

# Write to Delta
df_final.write.format("delta").mode("append").save("/mnt/gold/sales")
