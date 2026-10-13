WITH 
lazada AS (
    SELECT *
    FROM {{ ref('stg_product_lazada') }}
),

shopee AS (
    SELECT *
    FROM {{ ref('stg_product_shopee') }}
),

tiktok AS (
    SELECT *
    FROM {{ ref('stg_product_tiktok') }}
)

SELECT * FROM lazada
UNION ALL
SELECT * FROM shopee
UNION ALL
SELECT * FROM tiktok
