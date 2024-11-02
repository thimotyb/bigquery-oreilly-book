# 1 sec, 3.51 GB
SELECT 
  start_station_name 
  , AVG(duration) AS avg_duration
FROM `bigquery-public-data`.london_bicycles.cycle_hire
WHERE EXTRACT(YEAR from start_date) = 2015 
GROUP BY start_station_name
ORDER BY avg_duration DESC
LIMIT 5

# Antipattern: create separate tables by year
# (Use partitioned tables and template tables 
# instead of manually splitting your data across multiple tables.!!!)
CREATE OR REPLACE TABLE ch07eu.cycle_hire_2015 AS (
  SELECT * FROM `bigquery-public-data`.london_bicycles.cycle_hire
  WHERE EXTRACT(YEAR from start_date) = 2015
)
SELECT 
  start_station_name 
  , AVG(duration) AS avg_duration
 FROM ch07eu.cycle_hire_2015
 GROUP BY start_station_name
 ORDER BY avg_duration DESC
 LIMIT 5

# It is possible to use wildcards and table suffixes to search for multiple years
SELECT 
  start_station_name 
  , AVG(duration) AS avg_duration
FROM `ch07eu.cycle_hire_*`
WHERE _TABLE_SUFFIX BETWEEN '2015' AND '2016'
GROUP BY start_station_name
ORDER BY avg_duration DESC
LIMIT 5

# Create a partitioned table
CREATE OR REPLACE TABLE ch07eu.cycle_hire_partitioned
   PARTITION BY DATE(start_date) AS
SELECT * FROM `bigquery-public-data`.london_bicycles.cycle_hire

# You can keep storage costs in check by specifying an expiration time for a partition and asking BigQuery to ensure 
# that users are always using a partition filter (and not querying the entire table by mistake)
CREATE OR REPLACE TABLE ch07eu.cycle_hire_partitioned
   PARTITION BY DATE(start_date) 
   OPTIONS(partition_expiration_days=1000,    
           require_partition_filter=true) AS
SELECT * FROM `bigquery-public-
data`.london_bicycles.cycle_hire

# query the partitioned table, making sure to use the partition column (start_date) in the filter clause
# Use BETWEEN and not WHERE to use partition scanning instead of full scanning
SELECT 
  start_station_name 
  , AVG(duration) AS avg_duration
FROM ch07eu.cycle_hire_partitioned
WHERE start_date BETWEEN '2015-01-01' AND '2015-12-31'
GROUP BY start_station_name
ORDER BY avg_duration DESC
LIMIT 5


