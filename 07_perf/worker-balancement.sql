# Limiting Large Sorts
SELECT 
  rental_id
  , ROW_NUMBER() OVER(ORDER BY end_date) AS rental_number
FROM `bigquery-public-data`.london_bicycles.cycle_hire
ORDER BY rental_number ASC
LIMIT 5

# it is possible to limit the large sorts and distribute them. 
# extract the date from the rentals and then sort trips within each day
# the sorting can be done on just a single day of data at a time
WITH rentals_on_day AS (
SELECT 
  rental_id
  , end_date
  , EXTRACT(DATE FROM end_date) AS rental_date
FROM `bigquery-public-data.london_bicycles.cycle_hire`
)
 
SELECT 
  rental_id
  , rental_date
  , ROW_NUMBER() OVER(PARTITION BY rental_date ORDER BY end_date) AS
rental_number_on_day
FROM rentals_on_day
ORDER BY rental_date ASC, rental_number_on_day ASC
LIMIT 5

# Data Skew ### VERY EXPENSIVE QUERY
SELECT 
  author.tz_offset, ARRAY_AGG(STRUCT(author, committer, subject, message, 
trailer, difference, encoding) ORDER BY author.date.seconds LIMIT 1000)
FROM `bigquery-public-data.github_repos.commits`
GROUP BY author.tz_offset
