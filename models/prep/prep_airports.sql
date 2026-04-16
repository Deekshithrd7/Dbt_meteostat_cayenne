WITH airports_reorder AS (
    SELECT 
        faa,
        name,
        city,
        country,
        lat,
        lon
    FROM {{ref('staging_airports')}}
)

SELECT * 
FROM airports_reorder