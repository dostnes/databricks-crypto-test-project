# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, to_date

# --- LOKAL UTVIKLING SETUP ---
spark = SparkSession.builder.appName("CryptoSilver").getOrCreate()
# -----------------------------

# 1. Les rådata fra Bronze
# I Databricks leser vi rett fra tabellen vi lagde i stad
df_bronze = spark.table("crypto_bronze")

# 2. Transformasjoner
df_silver = df_bronze \
    .withColumn("last_updated", from_unixtime(col("last_updated_unixtime")).cast("timestamp")) \
    .withColumn("date_key", to_date(col("last_updated"))) \
    .drop("last_updated_unixtime") \
    .dropDuplicates(["coin_id", "last_updated"]) 
    # dropDuplicates sikrer at vi bare har én rad per mynt per tidspunkt

# 3. Lagre til Silver
# Vi bruker "overwrite" her for enkelhets skyld i dev. 
# "overwriteSchema" true lar oss endre kolonnene (vi fjernet unix-tid og la til date_key)
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("crypto_silver")

print("---------------------------------------")
print("Suksess! Data er vasket og lagret i 'crypto_silver'.")
print("---------------------------------------")