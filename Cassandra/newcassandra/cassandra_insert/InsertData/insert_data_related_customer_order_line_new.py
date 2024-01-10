from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from create_conn import get_conn_details
import pandas as pd

cluster, session = get_conn_details()

column_names = ["OL_W_ID", "OL_D_ID", "OL_O_ID", "OL_NUMBER", "OL_I_ID", "OL_DELIVERY_D", "OL_AMOUNT", "OL_SUPPLY_W_ID", "OL_QUANTITY", "OL_DIST_INFO"]
ol_data = pd.read_csv("/home/stuproj/cs4224b/data/order-line.csv", names=column_names, header=None)
# ol_data = pd.read_csv("C:\\Users\\User\\Desktop\\Sem3\\Distributed database\\Project\\project_files\\data_files\\order-line.csv", names=column_names, header=None)

ol_data.drop(columns=["OL_DELIVERY_D", "OL_AMOUNT", "OL_SUPPLY_W_ID", "OL_QUANTITY", "OL_DIST_INFO"], inplace=True)

import pandas as pd
headers = ["o_w_id", "o_d_id", "o_id", "o_c_id", "o_carrier_id", "o_ol_cnt", "ol_all_local", "o_entry_d"]
order = pd.read_csv("/home/stuproj/cs4224b/data/order.csv", names=headers)
# order = pd.read_csv("C:\\Users\\User\\Desktop\\Sem3\\Distributed database\\Project\\project_files\\data_files\\order.csv", names=headers)

data = pd.merge(ol_data, order, how='inner', left_on=['OL_W_ID', 'OL_D_ID', 'OL_O_ID'], right_on=["o_w_id", "o_d_id", "o_id"])

data.drop(columns=["o_w_id", "o_d_id", "o_id", "o_carrier_id", "o_ol_cnt", "ol_all_local", "o_entry_d"], inplace=True)

batch = BatchStatement()

insert_statement = session.prepare("INSERT INTO Related_Customer_Order_Line_Table (W_ID, D_ID, O_ID, OL_NUMBER, I_ID, C_ID) VALUES (?, ?, ?, ?, ?, ?)")

batch_size = 100

for start in range(0, len(data), batch_size):
    end = min(start + batch_size, len(data))
    
    batch = BatchStatement()
    
    for index, row in data[start:end].iterrows():
        batch.add(insert_statement, (row['OL_W_ID'], row['OL_D_ID'], row['OL_O_ID'], row['OL_NUMBER'], row['OL_I_ID'], row["o_c_id"]))
    
    session.execute(batch)

# Close the Cassandra session and the cluster connection
session.shutdown()
cluster.shutdown()