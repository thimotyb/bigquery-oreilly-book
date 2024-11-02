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

# USING TEMPORARY TABLES
# Instead of:
WITH typical_trip AS (
SELECT
  start_station_name
  , end_station_name
  , APPROX_QUANTILES(duration, 10)[OFFSET(5)] AS typical_duration
  , COUNT(duration) AS num_trips
FROM
  `bigquery-public-data`.london_bicycles.cycle_hire
GROUP BY
  start_station_name, end_station_name
)

SELECT 
   EXTRACT (DATE FROM start_date) AS trip_date
   , APPROX_QUANTILES(duration / typical_duration, 10)[OFFSET(5)] AS ratio
   , COUNT(*) AS num_trips_on_day
FROM 
  `bigquery-public-data`.london_bicycles.cycle_hire AS hire
JOIN typical_trip AS trip
ON 
   hire.start_station_name = trip.start_station_name 
   AND hire.end_station_name = trip.end_station_name
   AND num_trips > 10
GROUP BY trip_date
HAVING num_trips_on_day > 10
ORDER BY ratio DESC
LIMIT 10

# Create a TEMP TABLE
CREATE OR REPLACE TABLE ch07eu.typical_trip AS
SELECT
  start_station_name
  , end_station_name
  , APPROX_QUANTILES(duration, 10)[OFFSET(5)] AS typical_duration
  , COUNT(duration) AS num_trips
FROM
  `bigquery-public-data`.london_bicycles.cycle_hire
GROUP BY
  start_station_name, end_station_name

SELECT 
   EXTRACT (DATE FROM start_date) AS trip_date
   , APPROX_QUANTILES(duration / typical_duration, 10)[OFFSET(5)] AS ratio
   , COUNT(*) AS num_trips_on_day
FROM 
  `bigquery-public-data`.london_bicycles.cycle_hire AS hire
JOIN ch07eu.typical_trip AS trip
ON 
   hire.start_station_name = trip.start_station_name 
   AND hire.end_station_name = trip.end_station_name
   AND num_trips > 10
GROUP BY trip_date
HAVING num_trips_on_day > 10
ORDER BY ratio DESC
LIMIT 10

