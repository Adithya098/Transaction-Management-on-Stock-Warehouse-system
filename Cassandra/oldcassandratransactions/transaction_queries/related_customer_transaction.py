from cassandra.cluster import Cluster
import time
from cassandra.policies import RoundRobinPolicy
import time
from datetime import datetime


policy = RoundRobinPolicy()
# Connect to the Cassandra cluster
cluster = Cluster(
    contact_points=['192.168.48.185', '192.168.48.192', '192.168.48.193', '192.168.48.194', '192.168.48.184'],
    load_balancing_policy=policy
)
session = cluster.connect('teambtest')  # Replace 'cs4224b' with your keyspace name
def fetch_related_customers(cluster,session, c_w_id, c_d_id, c_id):
    main_orders = {}
    rows = session.execute(""" SELECT o_id FROM test_orders WHERE o_c_id = %s AND o_w_id = %s AND o_d_id = %s ALLOW FILTERING """, (c_id, c_w_id, c_d_id))
    main_customers = [row[0] for row in rows]

    order_lines_dict = {}
    res = []
    for customer in main_customers:
        prepared = session.prepare(""" SELECT ol_o_id, ol_i_id FROM test_order_line WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? ALLOW FILTERING """)
        rows = session.execute(prepared, (c_w_id, c_d_id, customer))
        for row in rows:
            res.append(row)

    all_items = set()
    for row in res:
        order_id = row[0]
        item = row[1]
        if item not in all_items:
            all_items.add(item)
        if order_id not in main_orders:
            main_orders[order_id] = [item]
        else:
            main_orders[order_id].append(item)

    for order in main_orders.keys():
        if len(main_orders[order]) < 2:
            del main_orders[order]

    res1 = []
    rows1 = session.execute("SELECT ol_w_id, ol_d_id, ol_o_id, ol_i_id FROM test_order_line")

    for row in rows1:
        if row.ol_w_id != c_w_id and row.ol_i_id in all_items:
            res1.append(row)

    orders_from_other_warehouses = {}
    for row in res1:
        identifier = f"{row[0]}_{row[1]}_{row[2]}"
        item = row[3]
        if identifier not in orders_from_other_warehouses:
            orders_from_other_warehouses[identifier] = [item]
        else:
            orders_from_other_warehouses[identifier].append(item)

    related_orders = []
    for order in main_orders.keys():
        for other_orders in orders_from_other_warehouses.keys():
            count = 0
            for item in main_orders[order]:
                if item in orders_from_other_warehouses[other_orders]:
                    count += 1
                    if count >= 2:
                        if other_orders not in related_orders:
                            related_orders.append(other_orders)
                            break

    related_customers = []
    for order in related_orders:
        identifiers = order.split("_")
        w_id = identifiers[0]
        d_id = identifiers[1]
        o_id = identifiers[2]

        prepared = session.prepare("SELECT o_c_id FROM test_orders WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?")
        rows = session.execute(prepared, (int(w_id), int(d_id), int(o_id)))
        row = rows.one()

        if row:
            customer_id = f"{w_id}_{d_id}_{row.o_c_id}"
            if customer_id not in related_customers:
                print(f"{w_id},{d_id},{row.o_c_id}")

# Example function call
c_w_id = 2  # Replace with the desired C_W_ID
c_d_id = 3  # Replace with the desired C_D_ID
c_id = 2274  # Replace with the desired C_ID
start_time=time.time()
# Call the Top-Balance Transaction function
fetch_related_customers(cluster,session, c_w_id, c_d_id, c_id)

end_time = time.time()
execution_time = end_time - start_time
print(f"Total execution time: {execution_time} seconds")


# Close the connection to the Cassandra cluster
session.shutdown()
cluster.shutdown()


