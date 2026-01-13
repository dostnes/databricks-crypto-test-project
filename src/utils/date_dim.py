# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, sequence, to_date, col, year, month, dayofweek, quarter

# --- LOKAL UTVIKLING SETUP ---
spark = SparkSession.builder.appName("DateDim").getOrCreate()
# -----------------------------

# 1. Definer start og slutt for kalenderen vår
start_date = "2024-01-01"
end_date = "2026-12-31"

# 2. Generer en rekke med datoer (Spark SQL triks)
# Vi lager en dataframe med én kolonne som inneholder en liste av datoer, 
# og bruker 'explode' for å lage en rad per dato.
df_dates = spark.sql(f"""
    SELECT explode(
        sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)
    ) as date
""")

# 3. Legg til attributter (år, måned, uke, osv.)
df_dim_date = df_dates \
    .withColumn("date_key", col("date")) \
    .withColumn("year", year("date")) \
    .withColumn("month", month("date")) \
    .withColumn("quarter", quarter("date")) \
    .withColumn("day_of_week", dayofweek("date")) \
    .withColumn("day_name", date_format(col("date"), "EEEE"))

# 4. Lagre som en tabell
# Vi bruker overwrite fordi denne tabellen er statisk og kan gjenskapes helt hver gang ved behov.
df_dim_date.write.format("delta").mode("overwrite").saveAsTable("dim_date")

print("Dimensjonstabell 'dim_date' er opprettet.")