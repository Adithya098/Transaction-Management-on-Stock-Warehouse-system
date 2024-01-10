from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from create_conn import get_conn_details
import pandas as pd

cluster, session = get_conn_details()

column_names = ["S_W_ID", "S_I_ID", "S_QUANTITY", "S_YTD", "S_ORDER_CNT", "S_REMOTE_CNT", "S_DIST_01", "S_DIST_02", "S_DIST_03", "S_DIST_04", "S_DIST_05", "S_DIST_06", "S_DIST_07", "S_DIST_08", "S_DIST_09", "S_DIST_10", "S_DATA"]
df_stocks = pd.read_csv("/home/stuproj/cs4224b/data/stock.csv", names=column_names, header=None)
item_column_names = ['I_ID', 'I_NAME', 'I_PRICE', 'I_IM_ID', 'I_DATA']
df = pd.read_csv("/home/stuproj/cs4224b/data/item.csv", names=item_column_names, header=None)
data = df_stocks.merge(df, left_on='S_I_ID', right_on='I_ID', how='inner')
data.drop(columns=['I_ID', 'I_IM_ID', 'I_DATA'], inplace=True)


insert_statement = session.prepare("INSERT INTO Stock_Item (S_W_ID, S_I_ID, S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10, S_DATA, I_NAME, I_PRICE) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

batch = BatchStatement()


batch_size = 50

for start in range(0, len(data), batch_size):
    end = min(start + batch_size, len(data))
    
    batch = BatchStatement()
    
    for index, row in data[start:end].iterrows():
        batch.add(insert_statement, (row['S_W_ID'], row['S_I_ID'], row['S_QUANTITY'], row['S_YTD'], row['S_ORDER_CNT'], row['S_REMOTE_CNT'], row['S_DIST_01'], row['S_DIST_02'], row['S_DIST_03'], row['S_DIST_04'], row['S_DIST_05'], row['S_DIST_06'], row['S_DIST_07'], row['S_DIST_08'], row['S_DIST_09'], row['S_DIST_10'], row['S_DATA'], row['I_NAME'], row['I_PRICE']))
    
    session.execute(batch)

# Close the Cassandra session and the cluster connection
session.shutdown()
cluster.shutdown()