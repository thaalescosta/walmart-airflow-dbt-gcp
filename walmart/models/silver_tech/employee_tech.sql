{{
    config(
        unique_key='employee_id',
    )
}}

SELECT
    *,
    current_timestamp() AS processed_at
FROM
    {{ source('walmart', 'employees') }}
WHERE
    is_active = 'Y'
{% if is_incremental() %}
AND updated_timestamp > (SELECT COALESCE(MAX(updated_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}