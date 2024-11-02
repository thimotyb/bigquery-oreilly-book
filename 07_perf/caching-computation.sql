# Without Materialized view
WITH oneway AS (
  SELECT EXTRACT(date from start_date) AS rental_date,
  duration, start_station_name, end_station_name
  FROM
  `bigquery-public-data`.london_bicycles.cycle_hire
  WHERE start_station_name != end_station_name
)

SELECT
  rental_date, AVG(duration) AS avg_duration, 
  start_station_name, end_station_name
FROM oneway
WHERE rental_date BETWEEN '2015-01-01' AND '2015-01-07'
GROUP BY rental_date, start_station_name, end_station_name

# Using Materialized Views
# Materialized views must belong to the same project or organization as the tables they reference.
CREATE OR REPLACE MATERIALIZED VIEW ch07eu.oneway_rentals
AS
WITH oneway AS (
  SELECT EXTRACT(date from start_date) AS rental_date,
  duration, start_station_name, end_station_name
  FROM
  `ch07eu`.london_bicycles.cycle_hire
  WHERE start_station_name != end_station_name
)

SELECT
  rental_date, AVG(duration) AS avg_duration, 
  start_station_name, end_station_name
FROM oneway
GROUP BY rental_date, start_station_name, end_station_name
