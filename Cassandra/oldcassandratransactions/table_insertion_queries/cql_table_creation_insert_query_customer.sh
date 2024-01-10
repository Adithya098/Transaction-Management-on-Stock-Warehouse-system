#!/bin/bash

# Cassandra connection details
CQLSH_HOST="192.168.48.194"
CQLSH_PORT="9042"
KEYSPACE_NAME="teambneworder"
TABLE_NAME="test_customer1"
CSV_FILE="/home/stuproj/cs4224b/data/customer.csv"

# CQL statement for table creation (if it doesn't exist)
CREATE_TABLE_CQL="CREATE TABLE IF NOT EXISTS $KEYSPACE_NAME.$TABLE_NAME (
    c_w_id INT,
    c_d_id INT,
    c_id INT,
    c_first VARCHAR,
    c_middle VARCHAR,
    c_last VARCHAR,
    c_street1 VARCHAR,
    c_street2 VARCHAR,
    c_city VARCHAR,
    c_state VARCHAR,
    c_zip VARCHAR,
    c_phone VARCHAR,
    c_since TIMESTAMP,
    c_credit VARCHAR,
    c_credit_lim DECIMAL,
    c_discount DECIMAL,
    c_balance DECIMAL,
    c_ytd_payment FLOAT,
    c_payment_cnt INT,
    c_delivery_cnt int,
    c_data VARCHAR,
    PRIMARY KEY ((c_id),c_d_id,c_w_id)
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

function replace_null {
    awk 'BEGIN {FS=OFS=","} {for (i=1; i<=NF; i++) if ($i == "null") $i="";} 1' "$CSV_FILE" > "$CLEANED_CSV_FILE"
}

# Replace 'null' values in the CSV file
replace_null

# CQL statement for data insertion from CSV
INSERT_DATA_CQL="COPY $KEYSPACE_NAME.$TABLE_NAME (C_W_ID, C_D_ID,C_ID, C_FIRST,C_MIDDLE,C_LAST, C_STREET1, C_STREET2, C_CITY, C_STATE, C_ZIP,C_PHONE,C_SINCE ,C_CREDIT,C_CREDIT_LIM,C_DISCOUNT,C_BALANCE, C_YTD_PAYMENT,C_PAYMENT_CNT,C_DELIVERY_CNT,C_DATA) FROM '$CSV_FILE' WITH DELIMITER=',';"

# Run CQL command to insert data from CSV
cqlsh "$CQLSH_HOST" -p "$CQLSH_PORT" -e "$INSERT_DATA_CQL"

# Check if the data insertion command executed successfully
if [ $? -eq 0 ]; then
    echo "Data inserted successfully."
else
    echo "Error inserting data."
fi

