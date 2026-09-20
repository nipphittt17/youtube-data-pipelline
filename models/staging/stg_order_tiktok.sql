WITH
    orders AS (
        SELECT
            upper(platform) AS platform,
            upper(region) AS region,
            upper(order_id) AS order_id,
            upper(product_id) AS product_id,
            upper(sku_id) AS sku_id,
            created_time AS order_at,
            quantity,
            sale_price AS item_price,
            round(platform_discount,2) AS item_platform_discount,
            round(seller_discount,2) AS item_seller_discount,
            round(shipping_fee,2) AS item_shipping_fee,
            upper(status) AS order_status
        FROM  {{ source('raw_data', 'order_tiktok') }}
        ORDER BY order_at DESC
    ),

    mapping AS (
        SELECT
            master_product_id,
            platform,
            region,
            product_id,
            sku_id
        FROM {{ source('blue_reference', 'master_product_mapping') }}
    )

SELECT
    m.master_product_id,
    o.platform,
    o.region,
    o.order_id,
    o.product_id,
    o.sku_id,
    o.order_at,
    o.quantity,
    o.item_price,
    o.item_platform_discount,
    o.item_seller_discount,
    o.item_shipping_fee,
    ((o.item_price * o.quantity) 
        - o.item_platform_discount
        - o.item_seller_discount 
        + o.item_shipping_fee) 
    AS total_price,
    o.order_status
FROM orders o
LEFT JOIN mapping m
    ON o.platform = m.platform
    AND o.region = m.region
    AND o.product_id = m.product_id
    AND o.sku_id = m.sku_id

