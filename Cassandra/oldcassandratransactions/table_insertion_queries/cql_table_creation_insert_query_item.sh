#!/bin/bash

# Cassandra connection details
CQLSH_HOST="192.168.48.185"
CQLSH_PORT="9042"
KEYSPACE_NAME="teambtest"
TABLE_NAME="item"
CSV_FILE="/home/stuproj/cs4224b/data/item.csv"

# Define the path for the cleaned CSV file
CLEANED_CSV_FILE="/home/stuproj/cs4224b/data/cleaned_item.csv"

# CQL statement for table creation (if it doesn't exist)
CREATE_TABLE_CQL="CREATE TABLE IF NOT EXISTS $KEYSPACE_NAME.$TABLE_NAME (
    i_id INT,
    i_name VARCHAR,
    i_price DECIMAL,
    i_im_id INT,
    i_data TEXT,
    PRIMARY KEY (i_id)
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

# Perform data cleaning using 'sed' to replace 'null' with an empty string
sed 's/null//g' "$CSV_FILE" > "$CLEANED_CSV_FILE"

# CQL statement for data insertion from cleaned CSV
INSERT_DATA_CQL="COPY $KEYSPACE_NAME.$TABLE_NAME (i_id,i_name,i_price,i_im_id,i_data) FROM '$CLEANED_CSV_FILE' WITH DELIMITER=',' AND HEADER=TRUE;"

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

