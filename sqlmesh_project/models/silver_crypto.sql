MODEL (
    name crypto_silver,
    kind FULL, -- bare for testing
    cron '@daily',
    grain coin_id
);

SELECT 
    coin_id,
    price_usd,
    cast(from_unixtime(last_updated_unixtime) as timestamp) as last_updated,
    to_date(cast(from_unixtime(last_updated_unixtime) as timestamp)) as date_key,
    ingestion_timestamp
FROM
spark_catalog.default.crypto_bronze

