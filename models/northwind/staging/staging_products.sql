with source_data as (
    select * from {{source('northwind_data','products')}}
)
select 
     productid AS product_id
    ,productname product_name
    ,supplierid AS supplier_id
    ,categoryid AS category_id
	,quantityperunit AS quantity_per_unit
    ,unitprice::NUMERIC AS unit_price
	,unitsinstock::INT AS units_in_stock
	,unitsonorder::INT AS units_on_order
	,discontinued
FROM source_data