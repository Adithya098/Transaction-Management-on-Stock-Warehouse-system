#!/bin/bash

# Cassandra connection details
CQLSH_HOST="192.168.48.185"
CQLSH_PORT="9042"
KEYSPACE_NAME="teambtest"
TABLE_NAME="test_district"
CSV_FILE="/home/stuproj/cs4224b/data/district.csv"

# CQL statement for table creation (if it doesn't exist)
CREATE_TABLE_CQL="CREATE TABLE IF NOT EXISTS $KEYSPACE_NAME.$TABLE_NAME (
    D_W_ID INT,
    D_ID INT,
    D_NAME VARCHAR,
    D_STREET1 VARCHAR,
    D_STREET2 VARCHAR,
    D_CITY VARCHAR,
    D_STATE VARCHAR,
    D_ZIP VARCHAR,
    D_TAX DECIMAL,
    D_YTD DECIMAL,
    D_NEXT_O_ID INT,
    PRIMARY KEY((D_ID),D_W_ID)
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
INSERT_DATA_CQL="COPY $KEYSPACE_NAME.$TABLE_NAME (D_W_ID, D_ID, D_NAME, D_STREET1, D_STREET2, D_CITY, D_STATE, D_ZIP, D_TAX, D_YTD, D_NEXT_O_ID) FROM '$CSV_FILE' WITH DELIMITER=',' AND HEADER=TRUE;"

# Run CQL command to insert data from CSV
cqlsh "$CQLSH_HOST" -p "$CQLSH_PORT" -e "$INSERT_DATA_CQL"

# Check if the data insertion command executed successfully
if [ $? -eq 0 ]; then
    echo "Data inserted successfully."
else
    echo "Error inserting data."
fi
