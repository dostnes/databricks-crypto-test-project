MODEL (
    name crypto_gold_daily,
    kind FULL,
    grain [date_key, coin_id]
);

SELECT
    date_key,
    coin_id,
    ROUND(AVG(price_usd), 2) AS avg_price,
    ROUND(MIN(price_usd), 2) AS min_price,
    ROUND(MAX(price_usd), 2) AS max_price,
    CURRENT_TIMESTAMP() as updated_at
FROM
    crypto_silver
GROUP BY
    date_key,
    coin_id;