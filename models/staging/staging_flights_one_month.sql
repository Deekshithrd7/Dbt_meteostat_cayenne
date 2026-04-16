{{ config(materialized='view') }}

WITH flights_one_month AS (
    SELECT * 
    FROM {{source('flights_data', 'flights')}}
    WHERE DATE_PART('month', flight_date) = 9 
)
SELECT * FROM flights_one_month
