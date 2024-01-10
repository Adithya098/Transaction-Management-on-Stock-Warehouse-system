from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from create_conn import get_conn_details
import pandas as pd
from datetime import datetime


# cluster, session = get_conn_details()

column_names = ["OL_W_ID", "OL_D_ID", "OL_O_ID", "OL_NUMBER", "OL_I_ID", "OL_DELIVERY_D", "OL_AMOUNT", "OL_SUPPLY_W_ID", "OL_QUANTITY", "OL_DIST_INFO"]
df_order_lines = pd.read_csv("/home/stuproj/cs4224b/data/order-line.csv", names=column_names, header=None)
item_column_names = ['I_ID', 'I_NAME', 'I_PRICE', 'I_IM_ID', 'I_DATA']
df = pd.read_csv("/home/stuproj/cs4224b/data/item.csv", names=item_column_names, header=None)
data = df_order_lines.merge(df, left_on='OL_I_ID', right_on='I_ID', how='inner')
data.drop(columns=['I_ID', 'I_PRICE', 'I_IM_ID', 'I_DATA'], inplace=True)

cluster, session = get_conn_details()

insert_statement = session.prepare("INSERT INTO Order_Line (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO, I_NAME) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

batch = BatchStatement()


batch_size = 100

for start in range(0, len(data), batch_size):
    end = min(start + batch_size, len(data))
    
    batch = BatchStatement()
    
    
    for index, row in data[start:end].iterrows():
        delivery_date_str = row['OL_DELIVERY_D']
        if delivery_date_str and isinstance(delivery_date_str, str):
            delivery_date = datetime.strptime(delivery_date_str, '%Y-%m-%d %H:%M:%S.%f')
        else:
            delivery_date = None 
        batch.add(insert_statement, (row['OL_W_ID'], row['OL_D_ID'], row['OL_O_ID'], row['OL_NUMBER'], row['OL_I_ID'], delivery_date, row['OL_AMOUNT'], row['OL_SUPPLY_W_ID'], row['OL_QUANTITY'], row['OL_DIST_INFO'], row['I_NAME']))
    
    session.execute(batch)

# Close the Cassandra session and the cluster connection
session.shutdown()
cluster.shutdown()