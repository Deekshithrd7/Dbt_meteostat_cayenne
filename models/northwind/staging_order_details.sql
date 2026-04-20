WITH source_data AS (
    SELECT *
    FROM {{ source('northwind_data', 'order_details') }}
)
SELECT
    orderid AS order_id
    ,productid AS product_id
    ,unitprice::NUMERIC AS unit_price
    ,quantity::INT AS quantity
    ,discount::NUMERIC AS discount
FROM source_data