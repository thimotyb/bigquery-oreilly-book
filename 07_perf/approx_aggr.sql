SELECT 
  COUNT(DISTINCT repo_name) AS num_repos
FROM `bigquery-public-data`.github_repos.commits, UNNEST(repo_name) AS repo_name

SELECT 
  APPROX_COUNT_DISTINCT(repo_name) AS num_repos
FROM `bigquery-public-data`.github_repos.commits, UNNEST(repo_name) AS repo_name

SELECT 
  COUNT(DISTINCT bike_id) AS num_bikes
FROM `bigquery-public-data`.london_bicycles.cycle_hire


SELECT 
  APPROX_COUNT_DISTINCT(bike_id) AS num_bikes
 FROM `bigquery-public-data`.london_bicycles.cycle_hire

SELECT 
  APPROX_TOP_COUNT(bike_id, 5) AS num_bikes
FROM `bigquery-public-data`.london_bicycles.cycle_hire

SELECT 
  APPROX_TOP_SUM(start_station_name, duration, 5) AS num_bikes
FROM `bigquery-public-data`.london_bicycles.cycle_hire
WHERE duration > 0

WITH sketch AS (
SELECT
    HLL_COUNT.INIT(start_station_name) AS hll_start
   , HLL_COUNT.INIT(end_station_name) AS hll_end
FROM `bigquery-public-data`.london_bicycles.cycle_hire
)
 
SELECT 
  HLL_COUNT.MERGE(hll_start) AS distinct_start
  , HLL_COUNT.MERGE(hll_end) AS distinct_end
  , HLL_COUNT.MERGE(hll_both) AS distinct_station
FROM sketch, UNNEST([hll_start, hll_end]) AS hll_both

# Without HLL
SELECT
   APPROX_COUNT_DISTINCT(start_station_name) AS distinct_start
  , APPROX_COUNT_DISTINCT(end_station_name) AS distinct_end
  , APPROX_COUNT_DISTINCT(both_stations) AS distinct_station
FROM 
  `bigquery-public-data`.london_bicycles.cycle_hire
  , UNNEST([start_station_name, end_station_name]) AS both_stations
