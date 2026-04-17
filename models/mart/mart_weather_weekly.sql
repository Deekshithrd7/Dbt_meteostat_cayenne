WITH weather AS (
    SELECT *
    FROM {{ ref('prep_weather_daily') }}
),

-- Add a weekly grouping column (week_start)
-- This converts each daily record into its corresponding week
weather_with_week AS (
    SELECT
        airport_code,
        date,

        -- group all dates into weeks (Monday-based default in Postgres)
        DATE_TRUNC('week', date) AS week_start,

        -- weather metrics at daily level
        min_temp_c,
        max_temp_c,
        precipitation_mm,
        max_snow_mm,
        avg_wind_direction,
        avg_wind_speed,
        avg_peakgust

    FROM weather
)

-- Aggregate daily weather into weekly airport-level metrics
SELECT
    airport_code,
    week_start,

    -- Temperature: use averages (represents weekly climate trend)
    AVG(min_temp_c) AS avg_min_temp,
    AVG(max_temp_c) AS avg_max_temp,

    --  Precipitation & Snow: use SUM (represents total accumulation)
    SUM(precipitation_mm) AS total_precipitation,
    SUM(max_snow_mm) AS total_snow,

    --  Wind speed: average behavior across the week
    AVG(avg_wind_speed) AS avg_wind_speed,

    -- Wind gust: take maximum extreme value in the week
    MAX(avg_peakgust) AS max_wind_gust,

    -- Wind direction: most frequently occurring direction in the week
    MODE() WITHIN GROUP (ORDER BY avg_wind_direction) AS dominant_wind_direction

FROM weather_with_week

-- Group by airport and week to build weekly time series
GROUP BY airport_code, week_start

-- Sort output for easy reading and analysis
ORDER BY airport_code, week_start