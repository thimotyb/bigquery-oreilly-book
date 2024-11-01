bq mk --location=US \
      --default_table_expiration 3600 \
      --description "Database ch 5" \
      ch05

# Checking whether a dataset exists
#!/bin/bash
 bq_safe_mk() {
     dataset=$1
     exists=$(bq ls --dataset | grep -w $dataset)
     if [ -n "$exists" ]; then
        echo "Not creating $dataset since it already exists"
     else
        echo "Creating $dataset"
        bq mk $dataset
     fi
}
# this is how you call the function
bq_safe_mk ch05

####################
# Creating a dataset in a different project
bq mk --location=US \
       --default_table_expiration 3600 \
       --description "Database 5." \
       <projectname>:ch05

# Creating a Table
bq mk --table \
    --expiration 3600 \
    --description "One hour of data" \
     --label persistence:volatile \
     ch05.rentals_last_hour rental_id:STRING,duration:FLOAT
 
# Create a table with schema
bq mk --table \
    --expiration 3600 \
    --description "One hour of data" \
     --label persistence:volatile \
     ch05.rentals_last_hour schema.json

# Copying Dataset
bq cp ch04.old_table ch05.new_table
# Wait copy to complete to synch script
bq wait --fail_on_error job_id

# Backup and restore a dataset (within 7 days using time travel)
CREATE OR REPLACE TABLE dataset.table_restored
AS 
SELECT *
FROM dataset.table
FOR SYSTEM TIME AS OF 
  TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -1 DAY)

# Permanent Backup/Restore
# Backup
bq show --schema dataset.table. # schema.json
bq --format=json show dataset.table.  # tbldef.json
bq extract --destination_format=AVRO \
           dataset.table gs://.../data_*.avro # AVRO files

# Restore
bq load --source_format=AVRO \
    --time_partitioning_expiration ... \
    --time_partitioning_field ... \
    --time_partitioning_type ... \
    --clustering_fields ... \
    --schema ... \
    todataset.table_name \
    gs://.../data_*.avro

##################
# Loading and Inserting Data
bq insert ch05.rentals_last_hour data.json


