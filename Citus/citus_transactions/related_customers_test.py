import psycopg2
import time

def related_customer_transaction(c_w_id, c_d_id,c_id,conn):
    
    cur = conn.cursor()
    main_orders={} 

    cur.execute("""SELECT o_id from test_orders 
        WHERE o_c_id=%s AND o_w_id=%s AND o_d_id=%s
        """,(c_id,c_w_id,c_d_id))
    print(f"{c_w_id},{c_d_id},{c_id}")
    main_customers=cur.fetchall()
    
    cur.execute("""SELECT ol_o_id, ol_i_id from test_order_lines 
        WHERE ol_w_id=%s AND ol_d_id=%s AND ol_o_id IN %s""", (c_w_id, c_d_id, tuple(main_customers)))
    
 #   print(f"main customer orders {main_customers}") 
    
    # cur.execute("""SELECT ol_o_id, ol_i_id from test_order_lines WHERE (ol_w_id, ol_d_id, ol_o_id) IN(
    #     SELECT ol_w_id, ol_d_id, ol_o_id FROM test_order_lines where ol_w_id=%s AND
    #     ol_d_id=%s AND ol_o_id IN %s GROUP BY (ol_w_id, ol_d_id, ol_o_id)
    #     HAVING COUNT(ol_o_id)>=2)""", (c_w_id, c_d_id, tuple(main_customers)))
    
    res = cur.fetchall()
    
    all_items = set()
    
    #Hashmap that stores the information about order id and the list of items in that order for the given customer
    for row in res:
        order_id = row[0]
        item = row[1]
        if all_items not in all_items:
            all_items.add(item)
        if order_id not in main_orders:
            main_orders[order_id] = [item]
        else:
            main_orders[order_id].append(item)
    
    for order in main_orders.keys():
        if len(main_orders[order])<2:
            del main_orders[order]
            
        
    
    # for order in main_customers:
    #     cur.execute("""SELECT ol_i_id from test_order_lines 
    #     WHERE ol_w_id=%s AND ol_d_id=%s AND ol_o_id=%s""",
    #     (c_w_id,c_d_id,order[0]))
    #     items=cur.fetchall()
    #     if order[0] not in main_orders:
    #         main_orders[order[0]] = []
    #     for item in items:
    #         if not main_orders:
    #             main_orders[order[0]] = [item[0]]
    #         else:
    #             main_orders[order[0]].append(item[0])
    
    # cur.execute("""SELECT o_id, o_w_id, o_d_id, o_c_id FROM test_orders WHERE o_w_id<>%s""", (c_w_id, ))
    # related_customers = set()
    # all_orders = cur.fetchall()
    
    #Select only the order line items which contain the items present in the given customer items
    cur.execute("""SELECT ol_w_id, ol_d_id, ol_o_id, ol_i_id
            FROM test_order_lines WHERE ol_w_id<>%s AND ol_i_id IN %s""", (c_w_id, tuple(all_items)))
    
    res = cur.fetchall()
    orders_from_other_warehouses = {}
    #Hashmap that stores the information about order id and the list of items for orders from other warehouses
    for row in res:
        identifier = f"{row[0]}_{row[1]}_{row[2]}"
        item = row[3]
        if identifier not in orders_from_other_warehouses:
            orders_from_other_warehouses[identifier] = [item]
        else:
            orders_from_other_warehouses[identifier].append(item)            
    related_orders = []
    
    #loop over the 2 hash maps and find if the intersection of items is >=2
    for order in main_orders.keys():
        for other_orders in orders_from_other_warehouses.keys():
            if len(set(main_orders[order]).intersection(orders_from_other_warehouses[other_orders]))>=2:
                if other_orders not in related_orders:
                    related_orders.append(other_orders)
    
    #Get customer id for the selected orders based on the identifier
    related_customers = []
    for order in related_orders:
        identifiers = str.split(order, "_")
        w_id = identifiers[0]
        d_id = identifiers[1]
        o_id = identifiers[2]
        
        cur.execute("SELECT o_c_id FROM test_orders where o_w_id = %s AND o_d_id = %s AND o_id = %s",
                    (w_id, d_id, o_id))
        res = cur.fetchone()
        #print(f"{w_id},{d_id},{res[0]}")
        customer_id = f"{w_id}_{d_id}_{res[0]}"
        if customer_id not in related_customers:
            print(f"{w_id},{d_id},{res[0]}")
            related_customers.append(customer_id)
#conn = psycopg2.connect(
#     host="localhost",
#     dbname="project",
#     user="cs4224b",
#     password="",
#     port="5098"
#     )
#start=time.time()
#related_customer_transaction(2,3,2274,conn)
#end=time.time()
#print(f"the time taken is {end-start}")
