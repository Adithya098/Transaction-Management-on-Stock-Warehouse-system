from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from create_conn import get_conn_details
import pandas as pd

cluster, session = get_conn_details()
customer_columns = ["C_W_ID", "C_D_ID", "C_ID", "C_FIRST", "C_MIDDLE", "C_LAST", "C_STREET_1", "C_STREET_2", "C_CITY", "C_STATE", "C_ZIP", "C_PHONE", "C_SINCE", "C_CREDIT", "C_CREDIT_LIM", "C_DISCOUNT", "C_BALANCE", "C_YTD_PAYMENT", "C_PAYMENT_CNT", "C_DELIVERY_CNT", "C_DATA"]

# data = pd.read_csv("/home/stuproj/cs4224b/data/customer.csv", names=customer_columns, header=None)


warehouse_columns = ["W_ID", "W_NAME", "W_STREET_1", "W_STREET_2", "W_CITY", "W_STATE", "W_ZIP", "W_TAX", "W_YTD"]
warehouse_data = pd.read_csv("/home/stuproj/cs4224b/data/warehouse.csv", names=warehouse_columns, header=None)
warehouse_data.drop(columns=["W_STREET_1", "W_STREET_2", "W_CITY", "W_STATE", "W_ZIP", "W_TAX", "W_YTD"], inplace=True)

district_columns = ["D_W_ID", "D_ID", "D_NAME", "D_STREET_1", "D_STREET_2", "D_CITY", "D_STATE", "D_ZIP", "D_TAX", "D_YTD", "D_NEXT_O_ID"]
district_data = pd.read_csv("/home/stuproj/cs4224b/data/district.csv", names=district_columns, header=None)
district_data.drop(columns=["D_STREET_1", "D_STREET_2", "D_CITY", "D_STATE", "D_ZIP", "D_TAX", "D_YTD", "D_NEXT_O_ID"], inplace=True)

customer = pd.read_csv("/home/stuproj/cs4224b/data/customer.csv", names=customer_columns, header=None)
customer.drop(columns=["C_STREET_1", "C_STREET_2", "C_CITY", "C_STATE", "C_ZIP", "C_PHONE", "C_SINCE", "C_CREDIT", "C_CREDIT_LIM", "C_DISCOUNT", "C_YTD_PAYMENT", "C_PAYMENT_CNT", "C_DELIVERY_CNT", "C_DATA"], inplace=True)

customer_warehouse = customer.merge(warehouse_data, left_on='C_W_ID', right_on='W_ID', how='inner')
customer_warehouse.drop(columns=["W_ID"], inplace=True)

data = customer_warehouse.merge(district_data, left_on=['C_W_ID', 'C_D_ID'], right_on=['D_W_ID', 'D_ID'], how='inner')
data.drop(columns=["D_W_ID", "D_ID"], inplace=True)



insert_statement = session.prepare("INSERT INTO Customer_Balance_Table (W_ID, D_ID, C_ID, C_BALANCE, C_FIRST, C_MIDDLE, C_LAST, D_NAME, W_NAME) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")

batch = BatchStatement()


batch_size = 50

for start in range(0, len(data), batch_size):
    end = min(start + batch_size, len(data))
    
    batch = BatchStatement()
    
    for index, row in data[start:end].iterrows():
        batch.add(insert_statement, (row['C_W_ID'], row['C_D_ID'], row['C_ID'], row['C_BALANCE'], row['C_FIRST'], row['C_MIDDLE'], row['C_LAST'], row['D_NAME'], row['W_NAME']))
    
    session.execute(batch)

# # Close the Cassandra session and the cluster connection
session.shutdown()
cluster.shutdown()