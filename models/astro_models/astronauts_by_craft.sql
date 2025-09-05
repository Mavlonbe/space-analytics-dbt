{{
    config(
        materialized='table',
        order_by=('craft', 'name')
    )
}}

SELECT
    craft,
    name,
    COUNT() as appearances,
    MIN(_inserted_at) as first_seen,
    MAX(_inserted_at) as last_seen,
    NOW() as processed_at
FROM {{ source('space_data', 'astronauts_parsed') }}
GROUP BY craft, name