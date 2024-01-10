from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from create_conn import get_conn_details
import pandas as pd
from datetime import datetime

cluster, session = get_conn_details()

order_columns = ["O_W_ID", "O_D_ID", "O_ID", "O_C_ID", "O_CARRIER_ID", "O_OL_CNT", "O_ALL_LOCAL", "O_ENTRY_D"]
orders_data = pd.read_csv("/home/stuproj/cs4224b/data/order.csv", names=order_columns, header=None)
customer_columns = ["C_W_ID", "C_D_ID", "C_ID", "C_FIRST", "C_MIDDLE", "C_LAST", "C_STREET_1", "C_STREET_2", "C_CITY", "C_STATE", "C_ZIP", "C_PHONE", "C_SINCE", "C_CREDIT", "C_CREDIT_LIM", "C_DISCOUNT", "C_BALANCE", "C_YTD_PAYMENT", "C_PAYMENT_CNT", "C_DELIVERY_CNT", "C_DATA"]
customers_data = pd.read_csv("/home/stuproj/cs4224b/data/customer.csv", names=customer_columns, header=None)
data = pd.merge(orders_data, customers_data, how='inner', left_on=['O_W_ID', 'O_D_ID', 'O_C_ID'], right_on=["C_W_ID", "C_D_ID", "C_ID"])
data.drop(columns=["C_W_ID", "C_D_ID", "C_ID", "C_STREET_1", "C_STREET_2", "C_CITY", "C_STATE", "C_ZIP", "C_PHONE", "C_SINCE", "C_CREDIT", "C_CREDIT_LIM", "C_DISCOUNT", "C_BALANCE", "C_YTD_PAYMENT", "C_PAYMENT_CNT", "C_DELIVERY_CNT", "C_DATA"], axis=1, inplace=True)


insert_statement = session.prepare("INSERT INTO Order_Customer (O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D, C_FIRST, C_MIDDLE, C_LAST) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

batch = BatchStatement()


batch_size = 100

for start in range(0, len(data), batch_size):
    end = min(start + batch_size, len(data))
    
    batch = BatchStatement()
    
    for index, row in data[start:end].iterrows():
        carrier_id = -1
        if(row['O_CARRIER_ID']!='null' and not pd.isna(row['O_CARRIER_ID'])):
            carrier_id = int(row['O_CARRIER_ID'])
        
        entry_date_str = row['O_ENTRY_D']
        if entry_date_str and isinstance(entry_date_str, str):
            entry_date = datetime.strptime(entry_date_str, '%Y-%m-%d %H:%M:%S.%f')
        else:
            entry_date = None 
        batch.add(insert_statement, (row['O_W_ID'], row['O_D_ID'], row['O_ID'], row['O_C_ID'], carrier_id, row['O_OL_CNT'], row['O_ALL_LOCAL'], entry_date, row['C_FIRST'], row['C_MIDDLE'], row['C_LAST']))
    
    session.execute(batch)

# Close the Cassandra session and the cluster connection
session.shutdown()
cluster.shutdown()