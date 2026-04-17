WITH flights AS (
    SELECT *
    FROM {{ ref('prep_flights') }}
),

airports AS (
    SELECT *
    FROM {{ ref('prep_airports') }}
),

weather AS (
    SELECT *
    FROM {{ ref('prep_weather_daily') }}
),

--  Keep only airports that have weather data
selected_airports AS (
    SELECT DISTINCT airport_code AS faa
    FROM weather
),

-- Convert flight table into "airport perspective"
-- One flight becomes TWO rows:
-- 1 row for departure airport, 1 row for arrival airport

flights_union AS (

    -- Departure view (origin airport perspective)
    SELECT
        origin AS faa,
        flight_date AS date,
        dest AS connection_airport,
        tail_number,
        airline,
        cancelled,
        diverted,
        1 AS is_departure,
        0 AS is_arrival
    FROM flights

    UNION ALL

    -- Arrival view (destination airport perspective)
    SELECT
        dest AS faa,
        flight_date AS date,
        origin AS connection_airport,
        tail_number,
        airline,
        cancelled,
        diverted,
        0 AS is_departure,
        1 AS is_arrival
    FROM flights
),

-- Step 4: Aggregate flight activity per airport per day
flight_stats AS (
    SELECT
        f.faa,
        f.date,

        -- how many unique routes from this airport (departures)
        COUNT(DISTINCT CASE WHEN is_departure = 1 THEN connection_airport END)
            AS unique_departure_connections,

        -- how many unique routes into this airport (arrivals)
        COUNT(DISTINCT CASE WHEN is_arrival = 1 THEN connection_airport END)
            AS unique_arrival_connections,

        -- total flight records (both directions)
        COUNT(*) AS total_flights_planned,

        -- disruptions
        SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS total_cancelled,
        SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END) AS total_diverted,

        -- actual flights that happened
        SUM(CASE WHEN cancelled = 0 THEN 1 ELSE 0 END) AS total_actual_flights,

        -- operational diversity
        COUNT(DISTINCT tail_number) AS unique_airplanes,
        COUNT(DISTINCT airline) AS unique_airlines

    FROM flights_union f

    -- only keep airports that exist in weather dataset
    INNER JOIN selected_airports sa
        ON f.faa = sa.faa

    GROUP BY f.faa, f.date
),

-- add airport + weather 
final AS (
    SELECT
        fs.faa,
        a.name AS airport_name,
        a.city,
        a.country,

        fs.date,

        -- flight metrics
        fs.unique_departure_connections,
        fs.unique_arrival_connections,
        fs.total_flights_planned,
        fs.total_cancelled,
        fs.total_diverted,
        fs.total_actual_flights,
        fs.unique_airplanes,
        fs.unique_airlines,

        -- weather metrics
        w.min_temp_c,
        w.max_temp_c,
        w.precipitation_mm,
        w.max_snow_mm,
        w.avg_wind_direction,
        w.avg_wind_speed,
        w.avg_peakgust

    FROM flight_stats fs

    -- airport info
    LEFT JOIN airports a
        ON fs.faa = a.faa

    -- weather info (join by airport + date)
    LEFT JOIN weather w
        ON fs.faa = w.airport_code
       AND fs.date = w.date::DATE
)

SELECT *
FROM final
ORDER BY faa, date