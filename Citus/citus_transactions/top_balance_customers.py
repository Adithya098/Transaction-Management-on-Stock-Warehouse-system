import psycopg2
#import time
#conn = psycopg2.connect(
#        host="localhost",
#        dbname="project",
#        user="cs4224b",
#        password="",
#        port="5098" 
#        )

def top_balance_transaction(conn):
    cur = conn.cursor()
    #cur.execute("""SELECT C.C_FIRST, C.C_MIDDLE, C.C_LAST, C.C_BALANCE
    #,W.W_NAME , D.D_NAME FROM test_customers C
    #JOIN test_districts D ON C.C_W_ID = D.D_W_ID AND C.C_D_ID = D.D_ID
    #JOIN test_warehouses W ON C.C_W_ID = W.W_ID
    #ORDER BY C_BALANCE DESC
    #LIMIT 10;
    #""")
    #cur.execute("""WITH TopCustomers AS (
    #SELECT C.C_FIRST, C.C_MIDDLE, C.C_LAST, C.C_BALANCE, C.C_W_ID, C.C_D_ID
    #FROM test_customers C ORDER BY C.C_BALANCE DESC LIMIT 10)
    #SELECT TC.C_FIRST, TC.C_MIDDLE, TC.C_LAST, TC.C_BALANCE, W.W_NAME, D.D_NAME
    #FROM TopCustomers TC JOIN test_districts D ON TC.C_W_ID = D.D_W_ID AND TC.C_D_ID = D.D_ID
    #JOIN test_warehouses W ON TC.C_W_ID = W.W_ID;
    #""")
    cur.execute("""WITH Top_Customers AS (
    SELECT C_FIRST, C_MIDDLE, C_LAST, C_BALANCE, C_W_ID, C_D_ID
    FROM test_customers ORDER BY C_BALANCE DESC LIMIT 10)
    SELECT C.C_FIRST, C.C_MIDDLE, C.C_LAST, C.C_BALANCE, W.W_NAME, D.D_NAME
    FROM Top_Customers C JOIN test_warehouses W ON C.C_W_ID = W.W_ID JOIN test_districts D ON C.C_W_ID = D.D_W_ID AND C.C_D_ID = D.D_ID""")
    rows = cur.fetchall()
    # print("Top 10 Customers with outstanding balance:")
    for row in rows:
    	print(row[0],",",row[1],",",row [2],",",row[3])
    cur.close()
#start= time.time()
#top_balance_transaction(conn)
#end = time.time()
#print(f"the time taken is {end-start}")
#conn.close()
