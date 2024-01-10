from create_conn import get_conn_details
from cassandra.cluster import Cluster
from datetime import datetime
import time

def order_status_transaction(session, C_W_ID, C_D_ID, C_ID):
    try:
        # Fetch customer details
        select_query1 = f"SELECT c_first, c_middle, c_last, c_balance FROM customers WHERE c_w_id = {C_W_ID} AND c_d_id = {C_D_ID} AND c_id = {C_ID};"
        customer = session.execute(select_query1)
        if customer:
            for row in customer:
                print("{row.c_first}, {row.c_middle}, {row.c_last}")
                print(f"{row.c_balance}")

                # Fetch the last order for the customer
                last_order_oid = session.execute("SELECT MAX(O_ID) FROM order_customer WHERE O_W_ID = %s AND O_D_ID = %s AND O_C_ID = %s", (C_W_ID, C_D_ID, C_ID))
                latest_order = session.execute("SELECT o_entry_d, o_carrier_id FROM order_customer WHERE O_W_ID = %s AND O_D_ID = %s AND O_C_ID = %s AND O_ID= %s", (C_W_ID, C_D_ID, C_ID, last_order_oid.one()[0]))

                for row in latest_order:
                    print(f"{last_order_oid.one()[0]}, {row.o_entry_d}, {row.o_carrier_id}")

                # Fetch order line details for the last order
                order_lines = session.execute("SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM order_line WHERE OL_W_ID = %s AND OL_D_ID = %s AND OL_O_ID = %s", (C_W_ID, C_D_ID, last_order_oid.one()[0]))
                for line in order_lines:
                    print(f"{line.ol_i_id}")
                    print(f"{line.ol_supply_w_id}")
                    print(f"{line.ol_quantity}")
                    print(f"{line.ol_amount}")
                    print(f"{line.ol_delivery_d}")

        else:
            print("Customer not found.")

    except Exception as e:
        print("An error occurred:", e)

#if __name__ == "__main__":
#    cluster, session = get_conn_details()
#    start_time = time.time()
#    order_status_transaction(session, 1, 1, 5)
#    end_time = time.time()
#    execution_time = end_time - start_time
#    print(f"\nExecution Time: {execution_time} seconds")
#    session.shutdown()
#    cluster.shutdown()

