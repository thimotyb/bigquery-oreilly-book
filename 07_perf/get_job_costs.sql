SELECT
   job_id
   , query
   , user_email
   , total_bytes_processed
   , total_slot_ms
FROM `cegeka-gcp-awareness`.`region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE EXTRACT(YEAR FROM creation_time) = 2024
ORDER BY total_bytes_processed DESC
LIMIT 5
