import psycopg2

def create_tables_for_connection(conn):    
    cur = conn.cursor()
    cur.execute("""CREATE TABLE test_warehouses(
    w_id INT PRIMARY KEY,
    w_name VARCHAR(10),
    w_street_1 VARCHAR(20),
    w_street_2 VARCHAR(20),
    w_city VARCHAR(20),
    w_state CHAR(2),
    w_zip CHAR(9),
    w_tax DECIMAL(4, 4),
    w_ytd DECIMAL(12, 2))""")
    
    cur.execute("""SELECT create_distributed_table('test_warehouses','w_id')""")
    
    cur.execute("""CREATE TABLE test_districts (
    D_W_ID INT,
    D_ID INT,
    D_NAME VARCHAR(10),
    D_STREET_1 VARCHAR(20),
    D_STREET_2 VARCHAR(20),
    D_CITY VARCHAR(20),
    D_STATE CHAR(2),
    D_ZIP CHAR(9),
    D_TAX DECIMAL(4, 4),
    D_YTD DECIMAL(12, 2),
    D_NEXT_O_ID INT,
    PRIMARY KEY (D_W_ID, D_ID),
    FOREIGN KEY (D_W_ID) REFERENCES test_warehouses(W_ID))""")
    
    cur.execute("""SELECT create_distributed_table('test_districts','d_w_id')""")
    
    cur.execute(""" CREATE TABLE test_customers(
    C_W_ID INT,
    C_D_ID INT,
    C_ID INT,
    C_FIRST VARCHAR(16),
    C_MIDDLE CHAR(2),
    C_LAST VARCHAR(16),
    C_STREET_1 VARCHAR(20),
    C_STREET_2 VARCHAR(20),
    C_CITY VARCHAR(20),
    C_STATE CHAR(2),
    C_ZIP CHAR(9),
    C_PHONE CHAR(16),
    C_SINCE TIMESTAMP,
    C_CREDIT CHAR(2),
    C_CREDIT_LIM DECIMAL(12,2),
    C_DISCOUNT DECIMAL(5,4),
    C_BALANCE DECIMAL(12,2),
    C_YTD_PAYMENT FLOAT,
    C_PAYMENT_CNT INT,
    C_DELIVERY_CNT INT,
    C_DATA VARCHAR(500),
    PRIMARY KEY (C_W_ID, C_D_ID, C_ID),
    FOREIGN KEY (C_W_ID, C_D_ID) REFERENCES test_districts(D_W_ID, D_ID))""")

    cur.execute("""SELECT create_distributed_table('test_customers','c_w_id')""")

    cur.execute("""
    CREATE TABLE test_orders (
    O_W_ID INT,
    O_D_ID INT,
    O_ID INT,
    O_C_ID INT,
    O_CARRIER_ID INT CHECK (O_CARRIER_ID BETWEEN 1 AND 10),
    O_OL_CNT DECIMAL(2,0),
    O_ALL_LOCAL DECIMAL(1,0),
    O_ENTRY_D TIMESTAMP,
    PRIMARY KEY(O_W_ID, O_D_ID, O_ID),
    FOREIGN KEY(O_W_ID, O_D_ID, O_C_ID) REFERENCES test_customers(C_W_ID, C_D_ID, C_ID)
    )""")   
    
    cur.execute("""SELECT create_distributed_table('test_orders','o_w_id')""")
    
    cur.execute("""
                CREATE TABLE test_items (
    "i_id" INT PRIMARY KEY,
    "i_name" VARCHAR(24),
    "i_price" DECIMAL(5,2),
    "i_im_id" INT,
    "i_data" VARCHAR(50)
    )""")
    
    cur.execute("""SELECT create_reference_table('test_items')""")
    
    cur.execute("""
                CREATE TABLE test_order_lines (
    ol_w_id INT,
    ol_d_id INT,
    ol_o_id INT,
    ol_number INT,
    ol_i_id INT,
    ol_delivery_d TIMESTAMP,
    ol_amount DECIMAL(7,2),
    ol_supply_w_id INT,
    ol_quantity DECIMAL(2,0),
    ol_dist_info CHAR(24),
    PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number),
    FOREIGN KEY (ol_w_id, ol_d_id, ol_o_id) REFERENCES test_orders(o_w_id, o_d_id, o_id));""")
    
    cur.execute("""SELECT create_distributed_table('test_order_lines','ol_w_id')""")
    
    cur.execute("""
    CREATE TABLE test_stocks (
    S_W_ID INT,
    S_I_ID INT,
    S_QUANTITY DECIMAL(4,0),
    S_YTD DECIMAL(8,2),
    S_ORDER_CNT INT,
    S_REMOTE_CNT INT,
    S_DIST_01 CHAR(24),
    S_DIST_02 CHAR(24),
    S_DIST_03 CHAR(24),
    S_DIST_04 CHAR(24),
    S_DIST_05 CHAR(24),
    S_DIST_06 CHAR(24),
    S_DIST_07 CHAR(24),
    S_DIST_08 CHAR(24),
    S_DIST_09 CHAR(24),
    S_DIST_10 CHAR(24),
    S_DATA VARCHAR(50),
    PRIMARY KEY (S_W_ID, S_I_ID),
    FOREIGN KEY (S_W_ID) REFERENCES test_warehouses(W_ID));""")
    
    cur.execute("""SELECT create_distributed_table('test_stocks','s_w_id')""")
    conn.commit()

import sys
sys.path.append('../citus_transactions')
from connection import get_connection
#from create_tables import create_tables_for_connection
if __name__ == "__main__":
    conn = get_connection("project")
    create_tables_for_connection(conn)
