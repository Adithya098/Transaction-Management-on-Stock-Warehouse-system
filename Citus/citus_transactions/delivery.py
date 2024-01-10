import psycopg2
import os
from datetime import datetime
def delivery_transaction(
    w_id, carrier_id, conn
):  
    cur = conn.cursor()    
    for i in range(1, 11):
        #print("hi hi")
        cur.execute("""SELECT o_id, o_c_id FROM test_orders WHERE o_w_id = %s AND o_d_id = %s AND o_carrier_id IS NULL ORDER BY o_id""", (w_id, i))
        res = cur.fetchone()
        #print(f"The res is {res}")
        if res == None:
            continue
        #print(f"reached here")
        N = res[0]
        c_id = res[1]
        #if N == None:
        #    continue
        
        cur.execute("""UPDATE test_orders SET o_carrier_id = %s WHERE o_w_id = %s AND o_d_id = %s AND o_id = %s""", (carrier_id, w_id, i, N))
        # print(f"res is {res}")
        # print("delivery_transaction: c_id = ", c_id)
        
        now = datetime.now()
        time_field = now.strftime("%Y-%m-%d %H:%M:%S")
        #print(f"The time field is {time_field}")
        cur.execute("""UPDATE test_order_lines SET ol_delivery_d = %s WHERE ol_w_id = %s AND ol_d_id = %s
                    AND ol_o_id = %s""", (time_field, w_id, i, N))
        #cur.execute("SELECT ol_delivery_d, FROM test_order_lines where ol_w_id = %s AND ol_d_id = %s AND ol_o_id = %s", (w_id, i, N))
        #cur.execute("SELECT o_carrier_id from test_orders WHERE o_w_id = %s AND o_d_id = %s AND o_id = %s", (w_id, i, N))
        #res = cur.fetchone()[0]
        #print(f"carrier id is {res}")
        cur.execute("SELECT SUM(ol_amount) FROM test_order_lines where ol_w_id = %s AND ol_d_id = %s AND ol_o_id = %s", (w_id, i, N))
        amount = cur.fetchone()[0]
        cur.execute("""UPDATE test_customers SET c_balance = c_balance + %s, c_delivery_cnt = c_delivery_cnt + 1
                    WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s""", (amount, w_id, i, c_id))

    conn.commit()
    cur.close()
    # conn.close()

#conn = psycopg2.connect(
#        host="localhost",
#        dbname="project",
#        user="cs4224b",
#        password="",
#        port="5098"
#        )
#delivery_transaction(10, 1, conn)
