#!/bin/bash
# If necessary create buckt and copy data from shell with
# gsutil cp college_scorecard.csv gs://bigquery-timo

LOC="--location US"
INPUT=gs://bigquery-timo/college_scorecard.csv

bq $LOC mk ch04 # okay if it fails
bq $LOC rm ch04.college_scorecard_gcs # replace

DEF=/tmp/college_scorecard_def.json

#############
This `awk` command processes input data (typically from a CSV file) as follows:

1. **`-F,`**: This sets the field separator to a comma (`,`), meaning that the input is expected to be in CSV format.

2. **`{ORS=","}`**: This sets the Output Record Separator to a comma. Normally, `awk` separates output records with a newline. Here, it will separate outputs with a comma instead.

3. **`{for (i=1; i <= NF; i++){ print $i":STRING"; }}`**: This loop iterates through each field in the current record. `NF` is the number of fields in the current record. For each field, it prints the field value followed by `:STRING`.

The overall effect is to take each line of input, print each field followed by `:STRING`, and separate the outputs with commas instead of newlines. If the input line has fields like `field1,field2,field3`, the output will be `field1:STRING,field2:STRING,field3:STRING`.
############
SCHEMA=$(gsutil cat $INPUT | head -1 | awk -F, '{ORS=","}{for (i=1; i <= NF; i++){ print $i":STRING"; }}' | sed 's/,$//g'| cut -b 4- )
echo $SCHEMA > /tmp/schema.txt

bq $LOC \
   mkdef \
   --source_format=CSV \
   --noautodetect \
   $INPUT \
   $SCHEMA \
  | sed 's/"skipLeadingRows": 0/"skipLeadingRows": 1/g' \
  | sed 's/"allowJaggedRows": false/"allowJaggedRows": true/g' \
  > $DEF


bq mk --external_table_definition=$DEF ch04.college_scorecard_gcs

