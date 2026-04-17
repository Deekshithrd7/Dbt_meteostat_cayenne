WITH departures AS (
		-- airport as departure
		SELECT origin, 
			COUNT(DISTINCT dest) AS nunique_to,
			COUNT(sched_dep_time) AS dep_planned,
			SUM(cancelled) AS dep_cancelled,
			SUM(diverted) AS dep_diverted,
		--	COUNT(sched_dep_time) - SUM(cancelled) AS dep_n_flights_calc,
			SUM(CASE WHEN cancelled = 0 THEN 1 END) AS dep_n_flights_calc,
			COUNT(DISTINCT tail_number) AS nunique_tail_number_from,
			COUNT(DISTINCT airline) AS nunique_airline_from
		FROM {{ref('prep_flights')}}
		GROUP BY origin
),
arrivals AS(
		-- airport as destination
		SELECT dest, 
			COUNT(DISTINCT origin) AS nunique_from,
			COUNT(sched_arr_time) AS arr_planned,
			SUM(cancelled) AS arr_cancelled,
			SUM(diverted) AS arr_diverted,
			--	COUNT(sched_arr_time) - SUM(cancelled) AS arr_n_flights_calc,
			SUM(CASE WHEN cancelled = 0 THEN 1 END) AS arr_n_flights_calc,
			COUNT(DISTINCT tail_number) AS nunique_tail_number_to,
			COUNT(DISTINCT airline) AS nunique_airline_to
		FROM {{ref('prep_flights')}}
		GROUP BY dest
),
total_stats AS (
		SELECT d.origin AS airport_code,
				nunique_to,
				nunique_from,
				d.dep_planned + a.arr_planned AS total_planned,
				d.dep_cancelled + a.arr_cancelled AS total_cancelled,
				d.dep_diverted + a.arr_diverted AS total_diverted,
				d.dep_n_flights_calc + a.arr_n_flights_calc AS total_n_flights_calc,
				a.nunique_tail_number_to,
				d.nunique_tail_number_from,
				a.nunique_airline_to,
				d.nunique_airline_from
		FROM departures d
		JOIN arrivals a
		ON d.origin = a.dest
)
SELECT ap.city, ap.country, ap.name, ts.*
FROM total_stats ts
JOIN {{ref('prep_airports')}} ap
ON ts.airport_code = ap.faa