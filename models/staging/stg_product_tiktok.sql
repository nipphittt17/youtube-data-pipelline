WITH
    product AS (
        SELECT
            upper(platform) AS platform,
            upper(region) AS region,
            upper(product_id) AS product_id,
            upper(sku_id) AS sku_id,
            product_name,
            shop_name,
            listing_price AS price,
            available_stock AS stock,
            rating,
            orders AS units_sold
        FROM {{ source('raw_data', 'product_tiktok') }}
    ),

    mapping AS (
        SELECT
            master_product_id,
            platform,
            region,
            product_id,
            sku_id
        FROM {{ source('blue_reference', 'master_product_mapping') }}
    ),
    
    cat_brand_map AS (
        SELECT
            master_product_id,
            category,
            brand
        FROM {{source('blue_reference', 'master_product_catalog')}}
    )

SELECT
    m.master_product_id,
    p.platform,
    p.region,
    p.product_id,
    p.sku_id,
    p.product_name,
    p.shop_name,
    p.price,
    p.stock,
    p.rating,
    p.units_sold,
    c.category,
    c.brand
FROM product p
LEFT JOIN mapping m
    ON p.platform = m.platform
    AND p.region = m.region
    AND p.product_id = m.product_id
    AND p.sku_id = m.sku_id
LEFT JOIN cat_brand_map c
    ON m.master_product_id = c.master_product_id
