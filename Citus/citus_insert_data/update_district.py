import os
os.environ['OPENBLAS_NUM_THREADS'] = '1'
os.environ['PGPORT'] = '5098'
os.environ['PGUSER'] = 'cs4224b'
os.environ['PGDATABASE'] = 'project'
os.environ['PGDATA'] = '/temp/teamb-data'
os.environ['PGHOST'] = 'localhost'  # Assuming you are connecting locally


import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# PostgreSQL database connection parameters
# db_params = {
#     "database": "project",
#     "user": "cs4224b",
#     "port": "5098"
# }

# CSV file path
csv_file = "data/district.csv"
db_params = {
    "host": "localhost",
    "database": "project",
    "user": "cs4224b",
    "password": "",
    "port": "5098"
}
# Create a PostgreSQL database connection
#column_names = ["w_id", "w_name", "w_street1", "w_street2", "w_city", "w_state", "w_zip", "w_tax", "w_ytd"]
column_names=["d_w_id", "d_id", "d_name","d_street_1","d_street_2", "d_city", "d_state", "d_zip", "d_tax", "d_ytd", "d_next_o_id"]
#column_names=["c_w_id", "c_d_id", "c_id", "c_first", "c_middle", "c_last", "c_street_1", "c_street_2", "c_city", "c_state", "c_zip", "c_phone", "c_since", "c_credit", "c_credit_lim", "c_discount", "c_balance", "c_ytd_payment", "c_payment_cnt", "c_delivery_cnt", "c_data"]
#column_names=["o_w_id", "o_d_id", "o_id", "o_c_id", "o_carrier_id", "o_ol_cnt", "o_all_local", "o_entry_d"]
#column_names=["i_id", "i_name", "i_price", "i_im_id", "i_data"]
#column_names=["ol_w_id", "ol_d_id", "ol_o_id", "ol_number", "ol_i_id", "ol_delivery_d", "ol_amount", "ol_supply_w_id", "ol_quantity", "ol_dist_info"]
#column_names=["s_w_id","s_i_id","s_quantity","s_ytd","s_order_cnt","s_remote_cnt","s_dist_01","s_dist_02","s_dist_03","s_dist_04","s_dist_05","s_dist_06","s_dist_07","s_dist_08","s_dist_09","s_dist_10","s_data"]
try:
    conn = psycopg2.connect(
        dbname="project",
        user="cs4224b",
        port="5098"
    )
except psycopg2.Error as e:
    print("Error connecting to the PostgreSQL database:", e)
    exit(1)

# Read the CSV file into a Pandas DataFrame
try:
    df = pd.read_csv(csv_file, header=None, names=column_names)
except FileNotFoundError:
    print("CSV file not found:", csv_file)
    conn.close()
    exit(1)

# Create a SQLAlchemy engine
engine = create_engine(f'postgresql+psycopg2://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}')

# Insert the data from the DataFrame into the PostgreSQL table
try:
  
    df.to_sql("test_districts", engine, if_exists="append", index=False)
    print(df)
    conn.commit()
    print("Data inserted successfully.")
except Exception as e:
    print("Error inserting data into PostgreSQL:", e)

# Close the database connection
conn.close()
