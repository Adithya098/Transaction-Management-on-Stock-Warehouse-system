import psycopg2

"""conn = psycopg2.connect(
    host="localhost",
    dbname="project",
    user="cs4224b",
    password="",
    port="5098"
    )"""

def popular_item_transaction(W_ID, D_ID,L,conn):
    cur = conn.cursor()
    print(f"{W_ID},{D_ID}")
    print(L)
    cur.execute("SELECT D_NEXT_O_ID FROM test_districts WHERE D_W_ID = %s AND D_ID = %s;",(W_ID,D_ID))
    N = cur.fetchone()[0]
    start = N - L
    N=N-1
    cur.execute("""SELECT O_ID,O_C_ID,O_ENTRY_D FROM test_orders WHERE O_D_ID = %s AND O_W_ID = %s
                AND O_ID BETWEEN %s AND %s""", (D_ID, W_ID, start, N))
    orders = cur.fetchall()
    popular_item_names={}
    total_orders=len(orders)
    order_items={}
    for order in orders:
        cur.execute("""SELECT C_FIRST||' '|| C_MIDDLE||' '|| C_LAST FROM test_customers
                    WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s """, (W_ID, D_ID, order[1]))
        customer=cur.fetchone()
        cur.execute("""SELECT I_ID, I_NAME, OL_QUANTITY FROM test_order_lines
        JOIN test_items ON OL_I_ID = I_ID
        WHERE OL_W_ID = %s AND OL_D_ID = %s AND OL_O_ID = %s
        """, (W_ID, D_ID, order[0]))
        items = cur.fetchall()
        print(f"{order[0]},{order[2]},{customer[0]}")
        max_val = max(item[2] for item in items)
        for row in items:
            item_id,item_name=row[0],row[1]
            pitem_id=row[0]          
            if row[2] == max_val:
                print(f"{row[1]},{row[2]}")
                popular_item_names[pitem_id]=item_name
            order_id=order[0]
            if not order_items:
               order_items[order_id] = [item_id]
            elif order_id not in order_items:
                order_items[order_id] = [item_id]
            else:
                order_items[order_id].append(item_id)
                    #if item_id in occurance:
                 #  occurance[item_id]=1+occurance[item_id]
                #else:
    popular_item_counts = {}
# Iterate through the popular item IDs
    for pitem_id in popular_item_names.keys():
    # Initialize a count for the current popular item
       count = 0
    # Iterate through each order and its item IDs
       for order_id, item_id in order_items.items():
         #print("----",item_id) 
         if pitem_id in item_id:
             count += 1
       popular_item_counts[popular_item_names[pitem_id]] = count
    # print("The percentage of orders that contain the popular item ")
    for popular_item_names[pitem_id], count in popular_item_counts.items():
           print(f"{popular_item_names[pitem_id]} : {(count/total_orders)*100}")          
    cur.close()

#popular_item_transaction(1,1,5,conn)
