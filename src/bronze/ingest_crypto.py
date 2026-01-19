# Databricks notebook source
import requests
import json
from datetime import datetime
from pyspark.sql.types import *

# 1. Definer API URL (Bitcoin pris i USD)
url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd&include_last_updated_at=true"

# 2. Hent data
response = requests.get(url)
data = response.json()

# Legg til en timestamp for når vi kjørte jobben (viktig for lineage)
current_time = datetime.now()

# 3. Forbered data for Spark DataFrame
# API returnerer: {'bitcoin': {'usd': 50000, 'last_updated_at': 123...}, ...}
# Vi må flate ut dette litt for å lage en tabell.

rows = []
for coin, details in data.items():
    rows.append((coin, details['usd'], details['last_updated_at'], current_time))

# 4. Definer Schema
schema = StructType([
    StructField("coin_id", StringType(), True),
    StructField("price_usd", DoubleType(), True),
    StructField("last_updated_unixtime", LongType(), True),
    StructField("ingestion_timestamp", TimestampType(), True)
])

# 5. Lag DataFrame
df = spark.createDataFrame(rows, schema)

# 6. Lagre til Bronze (Append mode - vi legger til nye priser hver gang)
# I Community Edition bruker vi standard hive_metastore
df.write.format("delta").mode("overwrite").saveAsTable("crypto_project.crypto_bronze")

print("Data lasted inn i crypto_project.crypto_bronze tabellen.")
display(df)