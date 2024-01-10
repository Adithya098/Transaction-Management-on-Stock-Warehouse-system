#!/bin/bash

# Cassandra connection details
CQLSH_HOST="192.168.48.185"
CQLSH_PORT="9042"
KEYSPACE_NAME="teambtest"
TABLE_NAME="test_warehouse"
CSV_FILE="/home/stuproj/cs4224b/data/warehouse.csv"

# CQL statement for table creation (if it doesn't exist)
CREATE_TABLE_CQL="CREATE TABLE IF NOT EXISTS $KEYSPACE_NAME.$TABLE_NAME (
    W_ID INT PRIMARY KEY,
    w_NAME VARCHAR,
    W_STREET1 VARCHAR,
    W_STREET2 VARCHAR,
    W_CITY VARCHAR,
    W_STATE VARCHAR,
    W_ZIP VARCHAR,
    W_TAX DECIMAL,
    W_YTD DECIMAL
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

# CQL statement for data insertion from CSV
INSERT_DATA_CQL="COPY $KEYSPACE_NAME.$TABLE_NAME FROM '$CSV_FILE' WITH DELIMITER=',' AND HEADER=TRUE;"

# Run CQL command to insert data from CSV
cqlsh "$CQLSH_HOST" -p "$CQLSH_PORT" -e "$INSERT_DATA_CQL"

# Check if the data insertion command executed successfully
if [ $? -eq 0 ]; then
    echo "Data inserted successfully."
else
    echo "Error inserting data."
fi
