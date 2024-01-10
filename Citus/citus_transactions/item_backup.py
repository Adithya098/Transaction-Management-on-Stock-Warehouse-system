import psycopg2

conn = psycopg2.connect(
    host="localhost",
    dbname="project",
    user="cs4224b",
    password="",
    port="5098"
    )

def popular_item_transaction(W_ID, D_ID,L,conn):
    cur = conn.cursor()
    print("-------------------------------------------------------------------------")
    print(f"District identifier (W_ID, D_ID): ({W_ID}, {D_ID})")
    print(f"Number of last orders to be examined L: {L}")

    cur.execute("SELECT D_NEXT_O_ID FROM test_districts WHERE D_W_ID = %s AND D_ID = %s;",(W_ID,D_ID))
    N = cur.fetchone()[0]
    N=N-1

    # Create a range of recent order IDs within the specified range (N - L, N]
    S = range(N - L + 1, N + 1)
    for x in S:
        # Fetch popular items for the current order (x)
        cur.execute("""SELECT ol_i_id,ol_quantity FROM test_order_lines
	WHERE ol_o_id = %s
	AND ol_quantity = (
    	SELECT MAX(ol_quantity)
    	FROM test_order_lines
    	WHERE ol_o_id = %s
	);""",(x,x))
        popular_items = cur.fetchall()

        # Calculate the total quantity ordered for the current order (x)
        cur.execute("""
            SELECT SUM(OL_QUANTITY)
            FROM test_order_lines
            WHERE OL_W_ID = %s AND OL_D_ID = %s AND OL_O_ID = %s
        """, (W_ID, D_ID, x))

        total_quantity = cur.fetchone()[0]

        # Calculate the percentage of each popular item in the current order (x)
        for j in popular_items:
            ol_i_id=j[0]
            ol_quantity=j[1]
            percentage= round((j[1]/total_quantity)*100);

        cur.execute("""SELECT c.C_FIRST || ' ' || c.C_MIDDLE || ' ' || c.C_LAST AS Customer_Name, o.O_ENTRY_D AS entryID FROM   
        test_customers c
        JOIN test_orders o ON c.c_w_id = o.o_w_id AND c.c_d_id = o.o_d_id AND c.c_id = o.o_c_id
        WHERE c.c_w_id= %s and c.c_d_id=%s and o.o_id=%s;""",(W_ID,D_ID,x))
        result=cur.fetchall()[0]
        cur.execute("""SELECT I_NAME from test_items where I_ID=%s""",(j[0],))
        item_name=cur.fetchone()[0]
        name= result[0]
        date = result[1].strftime('%d-%m-%y %H:%M:%S')
        
        print("POPULAR-------------------------->",date,x,name,item_name,ol_quantity,percentage)
         
    cur.close()

#popular_item_transaction(2,2,10,conn)
