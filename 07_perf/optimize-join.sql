# A not-optimized self-join
WITH male_babies AS (
SELECT 
  name
  , number AS num_babies
FROM `bigquery-public-data`.usa_names.usa_1910_current
WHERE gender = 'M'
),
female_babies AS (
SELECT 
  name
  , number AS num_babies
FROM `bigquery-public-data`.usa_names.usa_1910_current
WHERE gender = 'F'
),
both_genders AS (
SELECT 
  name
  , SUM(m.num_babies) + SUM(f.num_babies) AS num_babies
  , SUM(m.num_babies) / (SUM(m.num_babies) + SUM(f.num_babies)) AS frac_male
FROM male_babies AS m
JOIN female_babies AS f
USING (name)
GROUP BY name
)
 
SELECT * FROM both_genders
WHERE frac_male BETWEEN 0.3 and 0.7
ORDER BY num_babies DESC
LIMIT 5

# Reorganize the query the read the input only once
WITH all_babies AS (
SELECT 
  name 
  , SUM(IF(gender = 'M', number, 0)) AS male_babies
  , SUM(IF(gender = 'F', number, 0)) AS female_babies
FROM `bigquery-public-data.usa_names.usa_1910_current`
GROUP BY name
),
 
both_genders AS (
SELECT
  name
  , (male_babies + female_babies) AS num_babies
  , SAFE_DIVIDE(male_babies, male_babies + female_babies) AS frac_male
FROM all_babies
WHERE male_babies > 0 AND female_babies > 0
)
 
SELECT * FROM both_genders
WHERE frac_male BETWEEN 0.3 and 0.7
ORDER BY num_babies desc
limit 5

# Reduce the amount of data being joined by grouping the data by name and gender early on
with all_names AS (
  SELECT name, gender, SUM(number) AS num_babies
  FROM `bigquery-public-data`.usa_names.usa_1910_current
  GROUP BY name, gender
),
 
male_names AS (
   SELECT name, num_babies
   FROM all_names
   WHERE gender = 'M'
),
 
female_names AS (
   SELECT name, num_babies
   FROM all_names
   WHERE gender = 'F'
),
 
ratio AS (
  SELECT 
    name
    , (f.num_babies + m.num_babies) AS num_babies
    , m.num_babies / (f.num_babies + m.num_babies) AS frac_male
  FROM male_names AS m
  JOIN female_names AS f
  USING (name)
)
 
SELECT * from ratio
WHERE frac_male BETWEEN 0.3 and 0.7
ORDER BY num_babies DESC
LIMIT 5

# Using a window function instead of a self-join
# you want to find the duration between a bike being dropped off and it being rented again;
# in other words, the duration that a bicycle stays at the station
SELECT
  bike_id
  , start_date
  , end_date
  , TIMESTAMP_DIFF(
       start_date, 
       LAG(end_date) OVER (PARTITION BY bike_id ORDER BY start_date),
       SECOND) AS time_at_station
FROM `bigquery-public-data`.london_bicycles.cycle_hire
LIMIT 5

WITH unused AS (
SELECT
  bike_id
  , start_station_name
  , start_date
  , end_date
  , TIMESTAMP_DIFF(start_date, LAG(end_date) OVER (PARTITION BY bike_id ORDER BY
start_date), SECOND) AS time_at_station
FROM `bigquery-public-data`.london_bicycles.cycle_hire
)
 
SELECT
  start_station_name
  , AVG(time_at_station) AS unused_seconds
FROM unused
GROUP BY start_station_name
ORDER BY unused_seconds ASC
LIMIT 5

# find the pair of stations between which our customers ride bicycles at the fastest pace.
# To compute the pace divide the duration of the ride by the distance between stations.
# Create a denormalized table with distances between stations and then compute the average pace
with denormalized_table AS (
  SELECT
    start_station_name
    , end_station_name
    , ST_DISTANCE(ST_GeogPoint(s1.longitude, s1.latitude),
                  ST_GeogPoint(s2.longitude, s2.latitude)) AS distance
    , duration
 FROM
    `bigquery-public-data`.london_bicycles.cycle_hire AS h
 JOIN
     `bigquery-public-data`.london_bicycles.cycle_stations AS s1
 ON h.start_station_id = s1.id
  JOIN
     `bigquery-public-data`.london_bicycles.cycle_stations AS s2
 ON h.end_station_id = s2.id
),
 
durations AS (
  SELECT
    start_station_name
    , end_station_name
    , MIN(distance) AS distance
    , AVG(duration) AS duration
    , COUNT(*) AS num_rides
  FROM
     denormalized_table
  WHERE
     duration > 0 AND distance > 0
  GROUP BY start_station_name, end_station_name
  HAVING num_rides > 100
)
 
SELECT
    start_station_name
    , end_station_name
    , distance
    , duration
    , duration/distance AS pace
FROM durations
ORDER BY pace ASC
LIMIT 5

# Alternatively, we can use the cycle_stations table to precompute the distance between every pair of stations (this is a self-join) 
# and then join it with the reduced-size table of average duration between stations
with distances AS (
  SELECT 
    a.id AS start_station_id
    , a.name AS start_station_name
    , b.id AS end_station_id
    , b.name AS end_station_name
    , ST_DISTANCE(ST_GeogPoint(a.longitude, a.latitude),
                ST_GeogPoint(b.longitude, b.latitude)) AS distance
 FROM
    `bigquery-public-data`.london_bicycles.cycle_stations a
 CROSS JOIN
     `bigquery-public-data`.london_bicycles.cycle_stations b
 WHERE a.id != b.id
),
 
durations AS (
  SELECT
    start_station_id
    , end_station_id
    , AVG(duration) AS duration
    , COUNT(*) AS num_rides
  FROM
    `bigquery-public-data`.london_bicycles.cycle_hire
  WHERE
    duration > 0
  GROUP BY start_station_id, end_station_id
  HAVING num_rides > 100
)
 
SELECT
    start_station_name
    , end_station_name
    , distance
    , duration
    , duration/distance AS pace
FROM distances
JOIN durations
USING (start_station_id, end_station_id)
ORDER BY pace ASC
LIMIT 5

