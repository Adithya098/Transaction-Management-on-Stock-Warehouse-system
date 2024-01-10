import psycopg2
import time

def related_customer_transaction(c_w_id, c_d_id,c_id,conn):
  cur = conn.cursor()
  main_orders={} 
   #get all item ID for main customer 
  cur.execute("""SELECT o_id from test_orders 
     WHERE o_c_id=%s AND o_w_id=%s AND o_d_id=%s
     """,(c_id,c_w_id,c_d_id))
  main_customers=cur.fetchall()
  for order in main_customers:
    cur.execute("""SELECT ol_i_id from test_order_lines 
    WHERE ol_w_id=%s AND ol_d_id=%s AND ol_o_id=%s""",
    (c_w_id,c_d_id,order[0]))
    items=cur.fetchall()
  # get list of customers whose w_id is different
  cur.execute("""SELECT  c_id,c_w_id,c_d_id
  FROM test_customers
  WHERE c_w_id<>%s
  """,(c_w_id,))
  customers=cur.fetchall()

  #check for each customer if they have atleast 2 item_id in common with the main customer
  for customer in customers:
     cur.execute("""SELECT o_id from test_orders
     WHERE o_c_id=%s AND o_w_id=%s AND o_d_id=%s
     """,(customer[0],customer[1],customer[2]))
     customerid = f"{customer[0]} {customer[1]} {customer[2]}"
     relatedorders=cur.fetchall()
     if customerid not in main_orders:
        main_orders[customerid] = {}
     for rorder in relatedorders:
        cur.execute("""SELECT ol_i_id
        FROM test_order_lines
        WHERE ol_w_id=%s AND ol_d_id=%s AND ol_o_id=%s
        """,(customer[1],customer[2],rorder[0]))
        item_ids=cur.fetchall()
        if rorder[0] not in main_orders[customerid]:
           main_orders[customerid][rorder[0]] = []
        for item in item_ids:
           if not main_orders[customerid]:
             main_orders[customerid][rorder[0]] = [item[0]]
           else:
                main_orders[customerid][rorder[0]].append(item[0])

    # Iterate through the inner dictionary (orders)
  for order, item in main_orders.items():
        print(f"  Order: {order}")

        # Iterate through the items in the order
        for i in item:
            print(f"    Item: {item}") 
    #print(main_orders)
#     count=0
  #     if item_id[0] in items:
   #            count=count+1
    #           if(count==2):
     #             print(customer[0],",",customer[1],",",customer[2])
      #            break;
  cur.close()
  conn.close()
conn = psycopg2.connect(
    host="localhost",
    dbname="project",
    user="cs4224b",
    password="",
    port="5098"
    )
#R,2,4,1027
start=time.time()
related_customer_transaction(2,4,1027,conn)
end=time.time()
print(end-start)
