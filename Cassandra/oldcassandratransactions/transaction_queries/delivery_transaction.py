from cassandra.cluster import Cluster
from datetime import datetime
import time
 
def update_cassandra_data(cluster, session, w_id, CARRIER_ID):
    start_time = time.time()
    for i in range(1, 11):
        order_data = list(session.execute("SELECT * FROM test_orders ALLOW FILTERING"))
        o_ids = []
        o_c_id = None  # Initialize o_c_id variable
        for row in order_data:
            if row.o_carrier_id is None and row.o_w_id == w_id and row.o_d_id == i:
                o_ids.append(row.o_id)
        if o_ids:
            N = min(o_ids)
            c_id1 = None
            for row in order_data:
                if row.o_id == N:
                    c_id1 = row.o_c_id
                    break
            update_query = session.prepare("UPDATE test_orders SET o_carrier_id = ? WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?")
            try:
                session.execute(update_query, (CARRIER_ID, w_id, i, N))
            except Exception as e:
                print(f"An error occurred for O_CARRIER_ID of District {i}: {e}")
 
            current_time = datetime.now()
            order_line_items = session.execute(f"SELECT * FROM test_order_line WHERE OL_W_ID={w_id} AND OL_D_ID={i} AND OL_O_ID={N} ALLOW FILTERING")
            for order_line in order_line_items:
                if order_line:
                    ol_number = order_line.ol_number  # Assuming ol_number is the column name
                   # ol_i_id = order_line.ol_i_id
                    update_query1 = session.prepare("UPDATE test_order_line SET OL_DELIVERY_D = ? WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ? AND OL_NUMBER = ?")
 
                    try:
                        session.execute(update_query1, (current_time, w_id, i, N, ol_number))
                    except Exception as e:
                        print(f"An error occurred: {e}")
                else:
                    print("No matching row found in test_order_line table.")
 
            # Additional step (d)
            customer_data = session.execute(f"SELECT * FROM test_customer1 WHERE c_w_id={w_id} AND c_d_id={i} AND c_id={c_id1}")
            for row in customer_data:
                ol_amount_sum = 0
 
                # Retrieve all order items for the order with ID N
                order_line_items = session.execute(f"SELECT * FROM test_order_line WHERE OL_W_ID={w_id} AND OL_D_ID={i} AND OL_O_ID={N} ALLOW FILTERING")
 
                # Sum the OL_AMOUNT for each item in the order
                for order_item in order_line_items:
                    ol_amount_sum += order_item.ol_amount
 
                current_balance_value = row.c_balance
                updated_balance = current_balance_value + ol_amount_sum
                current_delivery_cnt = row.c_delivery_cnt
                updated_delivery_cnt = current_delivery_cnt + 1
                try:
                    session.execute("UPDATE test_customer1 SET c_balance = %s, c_delivery_cnt = %s WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s",
                                    (updated_balance, updated_delivery_cnt, w_id, i, c_id1))
                except Exception as e:
                    print(f"An error occurred while updating customer data: {e}")
 
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution Time: {execution_time} seconds")
 
# Example function call
# w_id = int(input("Warehouse number W ID: "))
# CARRIER_ID = int(input("Carrier ID:"))
 

session = cluster.connect('teambtest')  # Replace 'teamb' with your keyspace name
 
#update_cassandra_data(cluster, session, w_id, CARRIER_ID)
 
# Close the connection to the Cassandra cluster
cluster.shutdown()
