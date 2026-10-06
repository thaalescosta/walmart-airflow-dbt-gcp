{{ config(severity='warn')}}

-- obt_biz fans orders out through order_items and LEFT JOINs the remaining
-- dimensions. fact_orders and the dim_* snapshots all join back on these
-- surrogate keys, so a NULL here silently drops rows downstream.
-- employee_id is nullable by construction (LEFT JOIN on order.store_id), so it
-- is reported as a warning rather than an error.
SELECT
    order_id,
    order_item_id,
    product_id,
    store_id,
    employee_id,
    customer_id
FROM {{ ref('obt_biz') }}
WHERE
       order_id IS NULL
    OR order_item_id IS NULL
    OR product_id IS NULL
    OR store_id IS NULL
    OR employee_id IS NULL
    OR customer_id IS NULL
