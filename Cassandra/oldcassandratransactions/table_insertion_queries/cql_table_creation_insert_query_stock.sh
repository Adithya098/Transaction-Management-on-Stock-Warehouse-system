#!/bin/bash

# Cassandra connection details
CQLSH_HOST="192.168.48.185"
CQLSH_PORT="9042"
KEYSPACE_NAME="teambtest"
TABLE_NAME="test_stock"
CSV_FILE="/home/stuproj/cs4224b/data/stock.csv"

# Temporary CSV file for cleaned data
CLEANED_CSV_FILE="/home/stuproj/cs4224b/data/cleaned_stock.csv"

# CQL statement for table creation (if it doesn't exist)
CREATE_TABLE_CQL="CREATE TABLE IF NOT EXISTS $KEYSPACE_NAME.$TABLE_NAME (
    s_w_id INT,
    s_i_id INT,
    s_quantity DECIMAL,
    s_ytd DECIMAL,
    s_order_cnt INT,
    s_remote_cnt INT,
    s_dist_01 VARCHAR,
    s_dist_02 VARCHAR,
    s_dist_03 VARCHAR,
    s_dist_04 VARCHAR,
    s_dist_05 VARCHAR,
    s_dist_06 VARCHAR,
    s_dist_07 VARCHAR,
    s_dist_08 VARCHAR,
    s_dist_09 VARCHAR,
    s_dist_10 VARCHAR,
    s_data TEXT,
    PRIMARY KEY ((s_w_id), s_i_id)
);"

# Run CQL command to create the table
cqlsh "$CQLSH_HOST" -p "$CQLSH_PORT" -e "$CREATE_TABLE_CQL"

# Check if the table creation command executed successfully
if [ $? -eq 0 ]; then
    echo "Table created successfully."
else
    echo "Error creating table."
    exit 1
fi

# Function to replace 'null' with an empty string in CSV
function replace_null {
    awk 'BEGIN {FS=OFS=","} {for (i=1; i<=NF; i++) if ($i == "null") $i="";} 1' "$CSV_FILE" > "$CLEANED_CSV_FILE"
}

# Replace 'null' values in the CSV file
replace_null

# CQL statement for data insertion from cleaned CSV
INSERT_DATA_CQL="COPY $KEYSPACE_NAME.$TABLE_NAME (s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_data) FROM '$CLEANED_CSV_FILE' WITH DELIMITER=',' AND HEADER=TRUE;"

# Run CQL command to insert data from cleaned CSV
cqlsh "$CQLSH_HOST" -p "$CQLSH_PORT" -e "$INSERT_DATA_CQL"

# Check if the data insertion command executed successfully
if [ $? -eq 0 ]; then
    echo "Data inserted successfully."
else
    echo "Error inserting data."
fi

# Clean up: Remove the temporary cleaned CSV file
rm -f "$CLEANED_CSV_FILE"

