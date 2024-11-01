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


