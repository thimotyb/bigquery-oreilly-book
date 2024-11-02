
CREATE OR REPLACE TABLE ch07eu.cycle_hire_clustered
   PARTITION BY DATE(start_date) 
   CLUSTER BY start_station_name, end_station_name   
AS ( 
 SELECT * FROM `bigquery-public-data`.london_bicycles.cycle_hire
)

SELECT 
  start_station_name
  , end_station_name
  , AVG(duration) AS duration
FROM ch07eu.cycle_hire_clustered
WHERE 
  start_station_name LIKE '%Kennington%'
  AND end_station_name LIKE '%Hyde%'    
GROUP BY start_station_name, end_station_name
