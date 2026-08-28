WITH source AS (
    SELECT * FROM {{ source('raw', 'bitcoin_prices') }}
),

cleaned AS (
    SELECT
        date                                    AS price_date,
        close                                   AS close_price,
        volume                                  AS volume_usd,
        market_cap                              AS market_cap_usd,
        created_at,

        -- Kalkulasi tambahan
        ROUND(volume / NULLIF(market_cap, 0) * 100, 4)    AS volume_to_mcap_ratio,
        CASE
            WHEN close >= 50000 THEN 'high'
            WHEN close >= 30000 THEN 'medium'
            ELSE 'low'
        END                                     AS price_category
    FROM source
    WHERE close IS NOT NULL
      AND volume IS NOT NULL
      AND market_cap IS NOT NULL
)

SELECT * FROM cleaned