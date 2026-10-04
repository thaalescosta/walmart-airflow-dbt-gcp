{{ config(severity='warn')}}

SELECT 1
FROM {{ ref('obt') }} as obt
WHERE obt.id IS NULL
WHERE
    obt.order_id IS NULL
    OR
    obt.product_id IS NULL
    OR
    obt.employee_id IS NULL
    OR
    obt.store_id IS NULL
    OR
    obt.order_item_id IS NULL
    OR
    obt.customer_id IS NULL