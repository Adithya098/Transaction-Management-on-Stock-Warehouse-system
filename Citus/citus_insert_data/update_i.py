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
csv_file = "data/item.csv"
db_params = {
    "host": "localhost",
    "database": "project",
    "user": "cs4224b",
    "password": "",
    "port": "5098"
}
# Create a PostgreSQL database connection
column_names=["i_id", "i_name", "i_price", "i_im_id", "i_data"]
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
  
    df.to_sql("test_items", engine, if_exists="append", index=False)
    print(df)
    conn.commit()
    print("Data inserted successfully.")
except Exception as e:
    print("Error inserting data into PostgreSQL:", e)

# Close the database connection
conn.close()
