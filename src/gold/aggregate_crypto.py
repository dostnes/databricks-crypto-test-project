# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max, round, current_timestamp

# --- LOKAL UTVIKLING SETUP ---
spark = SparkSession.builder.appName("CryptoGold").getOrCreate()
# -----------------------------

# 1. Les Silver-data
df_silver = spark.table("crypto_silver")

# 2. Aggreger data per dag og mynt
# Vi beregner snitt, min og maks pris for hver dag.
df_gold = df_silver.groupBy("date_key", "coin_id") \
    .agg(
        round(avg("price_usd"), 2).alias("avg_price"),
        round(min("price_usd"), 2).alias("min_price"),
        round(max("price_usd"), 2).alias("max_price")
    ) \
    .withColumn("updated_at", current_timestamp())

# 3. Lagre til Gold
# Overwrite er trygt her siden vi rekalkulerer historikken basert på Silver hver gang.
# I store systemer ville vi kanskje brukt 'merge' (upsert), men overwrite er fint for nå.
df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("crypto_gold_daily")

print("Gold-tabell 'crypto_gold_daily' er oppdatert.")
display(df_gold)