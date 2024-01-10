from cassandra.cluster import Cluster
import time 
from datetime import datetime
from cassandra.policies import RoundRobinPolicy
import logging
logging.basicConfig(filename='popular.log', level=logging.DEBUG)

def get_last_orders(session, w_id, d_id, l):
    # Read the current D NEXT O ID value
    current_order_id_query = f"SELECT d_next_o_id FROM test_district WHERE d_w_id = {w_id} AND d_id = {d_id};"
    current_order_id_row = session.execute(current_order_id_query).one()
    current_order_id = current_order_id_row.d_next_o_id

    # Calculate the next order number
    next_order_id = current_order_id + 1

    first_order_number = next_order_id - l

    # Get the order identifiers for the last L orders (S)
    last_orders_query = f"SELECT o_id, o_entry_d, o_c_id FROM test_orders WHERE o_w_id = {w_id} AND o_d_id = {d_id} AND o_id >= {first_order_number} AND o_id < {next_order_id} ALLOW FILTERING;"
    last_orders = session.execute(last_orders_query)
    last_order_data = [(order.o_id, order.o_entry_d, order.o_c_id) for order in last_orders]

    print(f"District identifier (W ID, D ID): ({w_id}, {d_id})")
    print(f"Number of last orders to be examined L: {l}")
    print("")

    # Initialize dictionaries to store popular items and their appearance in orders
    popular_items = {}
    total_orders = len(last_order_data)

    for order_id, entry_d, c_id in last_order_data:
        print(f"Order number O ID: {order_id}")
        print(f"Entry date and time O ENTRY D: {entry_d}")
        print(f"Name of customer who placed this order (C FIRST, C MIDDLE, C LAST):")

        # Fetch customer name
        customer_query = f"SELECT c_first, c_middle, c_last FROM test_customer1 WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_id = {c_id};"
        customer_data = session.execute(customer_query).one()
        print(f"C FIRST: {customer_data.c_first}")
        print(f"C MIDDLE: {customer_data.c_middle}")
        print(f"C LAST: {customer_data.c_last}")
        # For each popular item in Px
        # Get the order-lines for this order (Ix)
        order_lines_query = f"SELECT ol_i_id, ol_quantity FROM test_order_line WHERE ol_o_id = {order_id} AND ol_w_id = {w_id} AND ol_d_id = {d_id} ALLOW FILTERING;"
        order_lines = session.execute(order_lines_query)
        order_lines_data = [(order_line.ol_i_id, order_line.ol_quantity) for order_line in order_lines]

        # Initialize the set of popular items (Px) as an empty list
        popular_items_for_order = []

        # Step 3 (b): Determine the subset of popular items (Px)
        for i, (item_id, quantity) in enumerate(order_lines_data):
            is_popular = True
            for j, (other_item_id, other_quantity) in enumerate(order_lines_data):
                if i != j and other_quantity > quantity:
                    is_popular = False
                    break
            if is_popular:
                popular_items_for_order.append((item_id, quantity))

        if popular_items_for_order:
            print("Popular items in this order:")
            for item_id, quantity in popular_items_for_order:
                # Fetch item name
                item_query = f"SELECT i_name FROM test_item WHERE i_id = {item_id};"
                item_data = session.execute(item_query).one()
                if item_data:
                    item_name = item_data.i_name
                    print(f"Item name I NAME: {item_name}")
                    print(f"Quantity ordered OL QUANTITY: {quantity}")
                else:
                    print("Item not found (i_id: {item_id})")

                # Update the popular items dictionary
                if item_id in popular_items:
                    popular_items[item_id] += 1
                else:
                    popular_items[item_id] = 1

        print("")

    # Calculate the percentage of orders that contain each popular item
    print("Percentage of examined orders that contain each popular item:")
    for item_id, appearance_count in popular_items.items():
        # Fetch item name
        item_query = f"SELECT i_name FROM test_item WHERE i_id = {item_id};"
        item_data = session.execute(item_query).one()
        if item_data:
            item_name = item_data.i_name
            percentage = (appearance_count / total_orders) * 100
            print(f"Item name I NAME: {item_name}")
            print(f"Percentage: {percentage:.2f}%")

def main():
    policy = RoundRobinPolicy()
    # Connect to the Cassandra cluster
    cluster = Cluster(
       contact_points=['192.168.48.185', '192.168.48.192', '192.168.48.193', '192.168.48.194', '192.168.48.184'],
       load_balancing_policy=policy
)
    session = cluster.connect('teambtest')
   # get_last_orders(session, 1, 5, 34)
    
    session.shutdown()
    cluster.shutdown()

if __name__ == "__main__":
    main()
