from cassandra.cluster import Cluster
from datetime import datetime
import time 
# Create a connection to the Cassandra cluster
cluster = Cluster(['192.168.48.185'])
session = cluster.connect('teambtest')
 
def retrieve_data_from_cassandra(session,c_w_id, c_d_id, c_id):
    select_query1 = f"SELECT c_first, c_middle, c_last, c_balance FROM test_customer1 WHERE c_w_id = {c_w_id} AND c_d_id = {c_d_id} AND c_id = {c_id};"
    result1 = session.execute(select_query1)
    for row in result1:
        name = f"{row.c_first} {row.c_middle} {row.c_last}"
        balance = row.c_balance
        print(f"\nCustomer Name: {name}\nBalance: {balance}\n")
 
    select_query2 = f"SELECT o_id, o_entry_d, o_carrier_id FROM test_orders where o_w_id = {c_w_id} AND o_d_id = {c_d_id} AND o_c_id = {c_id} ALLOW FILTERING;"
    result2 = session.execute(select_query2)
    latest_order = None
    for row in result2:
        if latest_order is None or row.o_entry_d > latest_order.o_entry_d:
            latest_order = row
 
    print("\nCustomers last orders 1:", latest_order.o_id, latest_order.o_entry_d, latest_order.o_carrier_id)
 
    select_query4 = session.prepare(
        "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM test_order_line WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? ALLOW FILTERING;"
    )
 
    # Execute the query with the provided parameters
    res = session.execute(select_query4, [c_w_id, c_d_id, c_id])
    print("\n")
    # Process the results
    for row in res:
        item_id, supply_w_id, ol_quantity, ol_amount, ol_delivery_d = row.ol_i_id, row.ol_supply_w_id, row.ol_quantity, row.ol_amount, row.ol_delivery_d
        print(f"item details: {item_id}, {supply_w_id}, {ol_quantity}, {ol_amount}, {ol_delivery_d}")
 
 
start_time = time.time()
retrieve_data_from_cassandra(session,5,2,1)

end_time = time.time()
execution_time = end_time - start_time
print(f"Execution Time: {execution_time} seconds")
 
session.shutdown()
cluster.shutdown()
