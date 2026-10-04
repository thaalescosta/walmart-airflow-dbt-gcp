SELECT
    DISTINCT product_id,
    product_name,
    category,
    brand
    price,
    product_created_timestamp,
    product_updated_timestamp,
    product_is_active,
    current_timestamp() AS products_gold_processed_at
FROM {{ ref("obt_biz") }}