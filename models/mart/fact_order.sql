WITH 
lazada AS (
    SELECT
        master_product_id,
        platform,
        region,
        order_id,
        product_id,
        sku_id,
        order_at,
        quantity,
        item_price,
        item_platform_discount,
        item_seller_discount,
        item_shipping_fee,
        total_price,
        order_status
    FROM {{ ref('stg_order_lazada') }}
),

shopee AS (
    SELECT
        master_product_id,
        platform,
        region,
        order_id,
        product_id,
        sku_id,
        order_at,
        quantity,
        item_price,
        item_platform_discount,
        item_seller_discount,
        item_shipping_fee,
        total_price,
        order_status
    FROM {{ ref('stg_order_shopee') }}
),

tiktok AS (
    SELECT
        master_product_id,
        platform,
        region,
        order_id,
        product_id,
        sku_id,
        order_at,
        quantity,
        item_price,
        item_platform_discount,
        item_seller_discount,
        item_shipping_fee,
        total_price,
        order_status
    FROM {{ ref('stg_order_tiktok') }}
)

SELECT * FROM lazada
UNION ALL
SELECT * FROM shopee
UNION ALL
SELECT * FROM tiktok


