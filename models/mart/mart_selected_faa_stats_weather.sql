WITH flights AS (
    SELECT *
    FROM prep_flights
),

airports AS (
    SELECT *
    FROM prep_airports
),

weather AS (
    SELECT *
    FROM prep_weather_daily
),

-- only airports that exist in weather table
selected_airports AS (
    SELECT DISTINCT airport_code AS faa
    FROM weather
),

-- Convert flights into airport-day level (departures + arrivals)
flights_union AS (

    -- departures
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

    -- arrivals
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

--  flight metrics per airport per day
flight_stats AS (
    SELECT
        f.faa,
        f.date,

        COUNT(DISTINCT CASE WHEN is_departure = 1 THEN connection_airport END)
            AS unique_departure_connections,

        COUNT(DISTINCT CASE WHEN is_arrival = 1 THEN connection_airport END)
            AS unique_arrival_connections,

        COUNT(*) AS total_flights_planned,

        SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS total_cancelled,
        SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END) AS total_diverted,

        SUM(CASE WHEN cancelled = 0 THEN 1 ELSE 0 END) AS total_actual_flights,

        COUNT(DISTINCT tail_number) AS unique_airplanes,
        COUNT(DISTINCT airline) AS unique_airlines

    FROM flights_union f

    INNER JOIN selected_airports sa
        ON f.faa = sa.faa

    GROUP BY f.faa, f.date
),

-- join with airports and weather
final AS (
    SELECT
        fs.faa,
        a.name AS airport_name,
        a.city,
        a.country,

        fs.date,

        fs.unique_departure_connections,
        fs.unique_arrival_connections,
        fs.total_flights_planned,
        fs.total_cancelled,
        fs.total_diverted,
        fs.total_actual_flights,
        fs.unique_airplanes,
        fs.unique_airlines,
        w.min_temp_c,
        w.max_temp_c,
        w.precipitation_mm,
        w.max_snow_mm,
        w.avg_wind_direction,
        w.avg_wind_speed,
        w.avg_peakgust

    FROM flight_stats fs

    LEFT JOIN airports a
        ON fs.faa = a.faa

    LEFT JOIN weather w
        ON fs.faa = w.airport_code
       AND fs.date = w.date::DATE
)

SELECT *
FROM final
ORDER BY faa, date