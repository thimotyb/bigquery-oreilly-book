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
