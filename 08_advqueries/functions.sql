CREATE OR REPLACE FUNCTION ch08eu.dayOfWeek(x TIMESTAMP) AS
(
 ['Sun','Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']
    [ORDINAL(EXTRACT(DAYOFWEEK from x))]
);

   SELECT
     duration
     , ch08eu.dayOfWeek(start_date) AS start_day
   FROM 
     `bigquery-public-data`.london_bicycles.cycle_hire
