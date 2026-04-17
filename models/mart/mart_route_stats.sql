WITH flights AS (
    SELECT *
    FROM {{ ref('prep_flights') }}
),

airports AS (
    SELECT *
    FROM {{ ref('prep_airports') }}
),

route_stats AS (
    SELECT
        origin,
        dest,

        COUNT(*) AS total_flights,

        COUNT(DISTINCT tail_number) AS unique_airplanes,
        COUNT(DISTINCT airline) AS unique_airlines,

        round(AVG(actual_elapsed_time),2) AS avg_elapsed_time,
        round(AVG(arr_delay),2) AS avg_arr_delay,

        MAX(arr_delay) AS max_arr_delay,
        MIN(arr_delay) AS min_arr_delay,

        SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS total_cancelled,
        SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END) AS total_diverted

    FROM flights
    GROUP BY origin, dest
),

final AS (
    SELECT
        rs.origin,
        ao.name AS origin_airport_name,
        ao.city AS origin_city,
        ao.country AS origin_country,

        rs.dest,
        ad.name AS dest_airport_name,
        ad.city AS dest_city,
        ad.country AS dest_country,

        rs.total_flights,
        rs.unique_airplanes,
        rs.unique_airlines,
        rs.avg_elapsed_time,
        rs.avg_arr_delay,
        rs.max_arr_delay,
        rs.min_arr_delay,
        rs.total_cancelled,
        rs.total_diverted

    FROM route_stats rs

    LEFT JOIN airports ao
        ON rs.origin = ao.faa

    LEFT JOIN airports ad
        ON rs.dest = ad.faa
)

SELECT *
FROM final