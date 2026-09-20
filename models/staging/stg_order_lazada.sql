WITH
    orders AS (
        SELECT
            upper(platform) AS platform,
            upper(region) AS region,
            upper(order_number) AS order_id,
            upper(product_code) AS product_id,
            upper(seller_sku) AS sku_id,
            created_at AS order_at,
            qty AS quantity,
            round(unit_price,2) AS item_price,
            round(seller_discount,2) AS item_platform_discount,
            round(voucher_amount,2) AS item_seller_discount,
            round(shipping_cost,2) AS item_shipping_fee,
            upper(status) AS order_status
        FROM  {{ source('raw_data', 'order_lazada') }}
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

