WITH staging AS (
    SELECT * FROM {{ ref('stg_bitcoin_prices') }}
),

monthly AS (
    SELECT
        DATE_TRUNC('month', price_date)             AS month,
        COUNT(*)                                     AS trading_days,
        MIN(close_price)                             AS monthly_low,
        MAX(close_price)                             AS monthly_high,
        ROUND(AVG(close_price), 2)                   AS monthly_avg,
        ROUND(SUM(volume_usd), 2)                    AS total_volume,
        ROUND(AVG(market_cap_usd), 2)                AS avg_market_cap,
        ROUND(AVG(volume_to_mcap_ratio), 4)          AS avg_volume_to_mcap,

        -- Berapa hari di setiap kategori harga
        COUNT(*) FILTER (
            WHERE price_category = 'high'
        )                                            AS high_price_days,
        COUNT(*) FILTER (
            WHERE price_category = 'medium'
        )                                            AS medium_price_days,
        COUNT(*) FILTER (
            WHERE price_category = 'low'
        )                                            AS low_price_days,

        -- Volatilitas: selisih high dan low dalam sebulan
        ROUND(MAX(close_price) - MIN(close_price), 2) AS monthly_volatility

    FROM staging
    GROUP BY DATE_TRUNC('month', price_date)
    ORDER BY month
)

SELECT * FROM monthly