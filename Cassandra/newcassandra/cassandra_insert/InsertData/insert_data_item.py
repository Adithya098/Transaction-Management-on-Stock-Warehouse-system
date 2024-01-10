from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from create_conn import get_conn_details
import pandas as pd

# Connect to your Cassandra cluster
# cluster = Cluster(['192.168.48.184','192.168.48.185','192.168.48.192', '192.168.48.193', '192.168.48.194'])
# session = cluster.connect()
cluster, session = get_conn_details()

# Use an existing keyspace or create a new one
column_names = ['I_ID', 'I_NAME', 'I_PRICE', 'I_IM_ID', 'I_DATA']
df = pd.read_csv("/home/stuproj/cs4224b/data/item.csv", names=column_names, header=None)
# Prepare an INSERT statement for your Cassandra table
insert_statement = session.prepare("INSERT INTO Item (I_ID, I_NAME, I_PRICE, I_IM_ID, I_DATA) VALUES (?, ?, ?, ?, ?)")



batch = BatchStatement()
batch_size = 100
# Loop through the DataFrame and insert data into Cassandra

for start in range(0, len(df), batch_size):
    end = min(start + batch_size, len(df))

    batch = BatchStatement()

    for index, row in df[start:end].iterrows():
        batch.add(insert_statement, (row['I_ID'], row['I_NAME'], row['I_PRICE'], row['I_IM_ID'], row['I_DATA']))

    session.execute(batch)

# for index, row in df.iterrows():
#     batch.add(insert_statement, (row['I_ID'], row['I_NAME'], row['I_PRICE'], row['I_IM_ID'], row['I_DATA']))

# session.execute(batch)
session.shutdown()
cluster.shutdown()