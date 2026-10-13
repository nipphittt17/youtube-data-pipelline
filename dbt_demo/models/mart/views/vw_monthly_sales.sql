-- Grain: Platform - Region - Product ID
WITH
    orders AS (
        SELECT
            master_product_id,
            platform,
            region,
            order_id,
            product_id,
            sku_id,
            order_at,
            TO_CHAR(order_at, 'YYYY-MM') AS order_year_month,
            quantity,
            item_price,
            item_platform_discount,
            item_seller_discount,
            item_shipping_fee,
            total_price,
            order_status
        FROM {{ ref('fact_order') }}
    )

SELECT
    order_year_month,
    platform,
    region,
    COUNT(DISTINCT order_id) AS total_orders,
    COALESCE(SUM(quantity),0) AS total_units,
    COALESCE(SUM(total_price),0) AS total_revenue
FROM orders o
GROUP BY ALL
ORDER BY platform, region, order_year_month



