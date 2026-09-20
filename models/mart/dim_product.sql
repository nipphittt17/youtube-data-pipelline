WITH 
lazada AS (
    SELECT
        master_product_id,
        platform,
        region,
        product_id,
        sku_id,
        product_name,
        shop_name,
        price,
        stock,
        rating,
        units_sold,
        category,
        brand
    FROM {{ ref('stg_product_lazada') }}
),

shopee AS (
    SELECT
        master_product_id,
        platform,
        region,
        product_id,
        sku_id,
        product_name,
        shop_name,
        price,
        stock,
        rating,
        units_sold,
        category,
        brand
    FROM {{ ref('stg_product_shopee') }}
),

tiktok AS (
    SELECT
        master_product_id,
        platform,
        region,
        product_id,
        sku_id,
        product_name,
        shop_name,
        price,
        stock,
        rating,
        units_sold,
        category,
        brand
    FROM {{ ref('stg_product_tiktok') }}
)

SELECT * FROM lazada
UNION ALL
SELECT * FROM shopee
UNION ALL
SELECT * FROM tiktok


