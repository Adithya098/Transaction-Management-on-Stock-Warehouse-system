from create_conn import get_conn_details
import time

def related_customers_transaction(session, c_w_id, c_d_id, c_id):

    # cur.execute("""SELECT o_id from test_orders
    #     WHERE o_c_id=%s AND o_w_id=%s AND o_d_id=%s
    #     """,(c_id,c_w_id,c_d_id))
    print(f"{c_w_id},{c_d_id},{c_id}")

    customer_orders_query = session.prepare("SELECT o_id from Order_Customer WHERE o_c_id=? AND o_w_id=? AND o_d_id=?")
    result = session.execute(customer_orders_query, (c_id, c_w_id, c_d_id))

    #print(f"The result is {result}")
    #for row in result:
    #   print(f"The row is {row.o_id}")
    o_id_list = []

    for row in result:
        o_id_list.append(row.o_id)

    #Query to get all the order_lines related to a customer
    order_lines_query = session.prepare("SELECT ol_o_id, ol_i_id from Order_Line WHERE ol_w_id=? AND ol_d_id=? AND ol_o_id IN ?")
    result = session.execute(order_lines_query, (c_w_id, c_d_id, tuple(o_id_list)))
    #print(f"The result is {result}")

    all_items = set()
    main_orders = {}

    for row in result:
        order_id = row[0]
        item = row[1]
        if all_items not in all_items:
            all_items.add(item)
        if order_id not in main_orders:
            main_orders[order_id] = [item]
        else:
            main_orders[order_id].append(item)

    #print(f"main orders is {main_orders}")

    #Query to get all the order_lines from warehouses that are not the same as the customer's warehouse based on the customer's items
    #order_lines_query = session.prepare("SELECT ol_o_id, ol_i_id from Related_Customer_Order_Line_Table WHERE w_id!=? AND i_id IN ?")
    #result = session.execute(order_lines_query, (c_w_id, tuple(all_items)))
    #print(f"all items are {all_items}")
    orders_from_other_warehouses = {}
    for w_id in range(1, 11):
        #print(f"w_id here is {w_id}")
        if w_id == c_w_id:
            continue
        order_lines_query = session.prepare("SELECT w_id, d_id, o_id, i_id from Related_Customer_Order_Line_Table WHERE w_id=? AND i_id IN ?")
        result = session.execute(order_lines_query, (w_id, tuple(all_items)))
        for row in result:
            identifier = f"{row.w_id}_{row.d_id}_{row.o_id}"
            item = row[3]
            if identifier not in orders_from_other_warehouses:
                orders_from_other_warehouses[identifier] = [item]
            else:
                orders_from_other_warehouses[identifier].append(item)
    #print(len(orders_from_other_warehouses))
    related_orders = []
    #print(f"main orders is {main_orders}")
    for order in main_orders.keys():
        for other_orders in orders_from_other_warehouses.keys():
            if len(set(main_orders[order]).intersection(orders_from_other_warehouses[other_orders]))>=2:
                if other_orders not in related_orders:
                    related_orders.append(other_orders)
    #print(f"Related orders size is {len(related_orders)}")

    related_customers = []
    for order in related_orders:
        identifiers = str.split(order, "_")
        w_id = identifiers[0]
        d_id = identifiers[1]
        o_id = identifiers[2]
        #print(f"the details are {w_id}, {d_id}, {o_id}")

        customer_id_query = session.prepare("SELECT o_c_id FROM Order_Customer where o_w_id=? AND o_d_id=? AND o_id=?")
        res = (session.execute(customer_id_query, (int(w_id), int(d_id), int(o_id)))).one()
        #print(f"The res is {res}")
        customer_id = f"{w_id}_{d_id}_{res.o_c_id}"
        if customer_id not in related_customers:
            print(f"{w_id},{d_id},{res[0]}")
            related_customers.append(customer_id)

if __name__ == "__main__":
    cluster, session = get_conn_details()
    start = time.time()
    related_customers_transaction(session,1,1,1584)
    print(f"Here the value is {time.time()-start}")
    session.shutdown()
    cluster.shutdown()
