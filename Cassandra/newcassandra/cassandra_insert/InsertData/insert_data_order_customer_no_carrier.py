from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from create_conn import get_conn_details
import pandas as pd
from datetime import datetime

cluster, session = get_conn_details()

order_columns = ["O_W_ID", "O_D_ID", "O_ID", "O_C_ID", "O_CARRIER_ID", "O_OL_CNT", "O_ALL_LOCAL", "O_ENTRY_D"]
data = pd.read_csv("/home/stuproj/cs4224b/data/order.csv", names=order_columns, header=None)
# data = pd.read_csv("C:\\Users\\User\\Desktop\\Sem3\\Distributed database\\Project\\project_files\\data_files\\order.csv", names=order_columns, header=None)
data.drop(columns=[ "O_OL_CNT", "O_ALL_LOCAL", "O_ENTRY_D"], axis=1, inplace=True)

data = data[pd.isnull(data['O_CARRIER_ID'])]
data.drop(columns=[ "O_CARRIER_ID"], axis=1, inplace=True)

insert_statement = session.prepare("INSERT INTO Order_Customer_No_Carrier (W_ID, D_ID, O_ID, C_ID) VALUES (?, ?, ?, ?)")

batch = BatchStatement()


batch_size = 100

for start in range(0, len(data), batch_size):
    end = min(start + batch_size, len(data))
    
    batch = BatchStatement()
    
    for index, row in data[start:end].iterrows():
        # carrier_id = -1
        # if(row['O_CARRIER_ID']!='null' and not pd.isna(row['O_CARRIER_ID'])):
        #     carrier_id = int(row['O_CARRIER_ID'])
        
        # entry_date_str = row['O_ENTRY_D']
        # if entry_date_str and isinstance(entry_date_str, str):
        #     entry_date = datetime.strptime(entry_date_str, '%Y-%m-%d %H:%M:%S.%f')
        # else:
        #     entry_date = None 
        batch.add(insert_statement, (row['O_W_ID'], row['O_D_ID'], row['O_ID'], row['O_C_ID']))
    
    session.execute(batch)

# Close the Cassandra session and the cluster connection
session.shutdown()
cluster.shutdown()