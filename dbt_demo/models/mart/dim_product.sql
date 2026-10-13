SELECT *
FROM {{ ref('fact_product_snapshot') }}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
        platform,
        region,
        product_id,
        sku_id
    ORDER BY updated_at DESC
) = 1
