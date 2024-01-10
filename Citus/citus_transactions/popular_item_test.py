import psycopg2


conn = psycopg2.connect(
    host="localhost",
    dbname="project",
    user="cs4224b",
    password="",
    port="5098"
    )
def popular_item_transaction_test(W_ID, D_ID,L,conn):
    # conn = psycopg2.connect(        
    #     host="localhost",
    #     dbname="project",
    #     user="cs4224b",
    #     password="",
    #     port="5098"
    # )
    cur = conn.cursor()
    print("-------------------------------------------------------------------------")
    print(f"District identifier (W_ID, D_ID): ({W_ID}, {D_ID})")
    print(f"Number of last orders to be examined L: {L}")

    cur.execute("SELECT D_NEXT_O_ID FROM test_districts WHERE D_W_ID = %s AND D_ID = %s;",(W_ID,D_ID))
    N = cur.fetchone()[0]
    start = N - L
    N=N-1
    # Create a range of recent order IDs within the specified range (N - L, N]
    cur.execute("""SELECT O_ID, O_C_ID, O_ENTRY_D FROM test_orders WHERE O_D_ID = %s AND O_W_ID = %s
                AND O_ID BETWEEN %s AND %s""", (D_ID, W_ID, start, N-1))
    orders = cur.fetchall()
    # S = range(N - L + 1, N + 1)
    popular_item_names = {}
    item_hash = {}
    for x in orders:        
        cur.execute("""SELECT C_FIRST, C_MIDDLE, C_LAST FROM test_customers 
                    WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s """, (W_ID, D_ID, x[1]))
        c_first, c_middle, c_last = cur.fetchone()
        print(x[0], x[2])
        print(c_first, c_middle, c_last)
        
        cur.execute("""SELECT I_ID, I_NAME, OL_QUANTITY FROM test_order_lines JOIN test_items ON OL_I_ID = I_ID
            WHERE OL_W_ID = %s AND OL_D_ID = %s AND OL_O_ID = %s""", (W_ID, D_ID, x[0]))
        items = cur.fetchall()
        max_val  = 0
        for row in items:
            max_val = max(max_val, row[2])
        
        for row in items:
            if row[2] == max_val:
                print(row[1], row[2])
                popular_item_names[row[0]] = row[1]
                if row[0] in item_hash:
                    item_hash[row[0]] = 1+item_hash[row[0]]
                else:
                    item_hash[row[0]] = 1
    
    total_number = len(orders)
    for i_id, i_name in popular_item_names.items():
        occurances = item_hash[i_id]
        percentage = (occurances/total_number)*100
        print(f"{i_name}, {percentage}")
    # Fetch popular items for the current order (x)
    #     cur.execute("""SELECT ol_i_id, ol_quantity FROM test_order_lines
	# WHERE ol_o_id = %s
	# AND ol_quantity = (
    # 	SELECT MAX(ol_quantity)
    # 	FROM test_order_lines
    # 	WHERE ol_o_id = %s
	# );""",(x,x))
    #     popular_items = cur.fetchall()

    #     # Calculate the total quantity ordered for the current order (x)
    #     cur.execute("""
    #         SELECT SUM(OL_QUANTITY)
    #         FROM test_order_lines
    #         WHERE OL_W_ID = %s AND OL_D_ID = %s AND OL_O_ID = %s
    #     """, (W_ID, D_ID, x))

    #     total_quantity = cur.fetchone()[0]

    #     # Calculate the percentage of each popular item in the current order (x)
    #     for j in popular_items:
    #         ol_i_id=j[0]
    #         ol_quantity=j[1]
    #         percentage= round((j[1]/total_quantity)*100);

    #     cur.execute("""SELECT c.C_FIRST || ' ' || c.C_MIDDLE || ' ' || c.C_LAST AS Customer_Name, o.O_ENTRY_D AS entryID FROM   
    #     test_customers c
    #     JOIN test_orders o ON c.c_w_id = o.o_w_id AND c.c_d_id = o.o_d_id AND c.c_id = o.o_c_id
    #     WHERE c.c_w_id= %s and c.c_d_id=%s and o.o_id=%s;""",(W_ID,D_ID,x))
    #     result=cur.fetchall()[0]
    #     cur.execute("""SELECT I_NAME from test_items where I_ID=%s""",(j[0],))
    #     item_name=cur.fetchone()[0]
    #     name= result[0]
    #     date = result[1].strftime('%d-%m-%y %H:%M:%S')
        
    #     print("POPULAR-------------------------->",date,x,name,item_name,ol_quantity,percentage)
         
    cur.close()

popular_item_transaction_test(1,1,5,conn)
