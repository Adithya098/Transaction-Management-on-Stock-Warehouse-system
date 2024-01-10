#!/bin/bash

# Cassandra connection details
CQLSH_HOST="192.168.48.185"
CQLSH_PORT="9042"
KEYSPACE_NAME="teambtest"
TABLE_NAME="test_orders"
CSV_FILE="/home/stuproj/cs4224b/data/order.csv"

# Temporary CSV file for cleaned data
CLEANED_CSV_FILE="/home/stuproj/cs4224b/data/cleaned_order.csv"

# CQL statement for table creation (if it doesn't exist)
CREATE_TABLE_CQL="CREATE TABLE IF NOT EXISTS $KEYSPACE_NAME.$TABLE_NAME (
    o_w_id INT,
    o_d_id INT,
    o_id INT,
    o_c_id INT,
    o_carrier_id INT,
    o_ol_cnt DECIMAL,
    o_all_local DECIMAL,
    o_entry_d TIMESTAMP,
    PRIMARY KEY((o_id), o_d_id, o_w_id)
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
INSERT_DATA_CQL="COPY $KEYSPACE_NAME.$TABLE_NAME (o_w_id,o_d_id,o_id,o_c_id,o_carrier_id,o_ol_cnt,o_all_local,o_entry_d) FROM '$CLEANED_CSV_FILE' WITH DELIMITER=',' AND HEADER=TRUE;"

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

