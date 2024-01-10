from cassandra.cluster import Cluster
from datetime import datetime
import time
 
# Create a connection to the Cassandra cluster
cluster = Cluster(['192.168.48.185'])
session = cluster.connect('teambtest')

def check_stock_threshold(session, w_id, d_id, threshold, l):
    select_query = f"SELECT D_NEXT_O_ID FROM test_district WHERE D_W_ID = {w_id} AND D_ID = {d_id};"
    result_1 = session.execute(select_query)
    N = result_1.one().d_next_o_id
    N = int(N)
    start = N - l
 
    query_2 = session.prepare("SELECT ol_i_id FROM test_order_line WHERE ol_d_id = ? AND ol_w_id = ? AND ol_o_id >= ? AND ol_o_id <= ? ALLOW FILTERING;")
    result_2 = session.execute(query_2, [d_id, w_id, start, N-1])
    vals = [row.ol_i_id for row in result_2]
    query_3 = session.prepare("SELECT count(*) FROM test_stock WHERE s_w_id = ? AND s_i_id IN ? AND s_quantity < ? ALLOW FILTERING ;")
    result_3 = session.execute(query_3, [w_id, vals, threshold])
    count = result_3.one().count
    print(f"The number of items where S_QUANTITY < T {count}")
 

start_time = time.time()
 
# Call the function
check_stock_threshold(session,1,1,14,20)
 
end_time = time.time()
execution_time = end_time - start_time
print(f"Execution Time: {execution_time} seconds")
# Close the connection to the Cassandra cluster
session.shutdown()
cluster.shutdown()
