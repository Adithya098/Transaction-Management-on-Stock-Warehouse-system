import psycopg2

def order_status_transaction(
    w_id, d_id, c_id, conn):
        
    cur = conn.cursor()
    cur.execute("""SELECT c_first, c_middle, c_last, c_balance FROM test_customers WHERE
                c_w_id = %s AND c_d_id = %s AND c_id = %s""", (w_id, d_id, c_id))
    
    c_first, c_middle, c_last, c_balance = cur.fetchone()
    # print(f"cust details {c_first}, {c_middle}, {c_last}, {c_balance}")
    print(f"{c_first}, {c_middle}, {c_last}, {c_balance}")
    cur.execute("""SELECT o_id, o_entry_d, o_carrier_id FROM test_orders where o_w_id = %s AND o_d_id = %s
                AND o_c_id = %s ORDER BY o_entry_d DESC LIMIT 1""", (w_id, d_id, c_id))
    
    o_id, o_entry_d, o_carrier_id = cur.fetchone()
    print(f"{o_id}, {o_entry_d}, {o_carrier_id}")
    # print(f"order details {o_id}, {o_entry_d}, {o_carrier_id}")
    
    cur.execute("""SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM test_order_lines
                WHERE ol_w_id = %s AND ol_d_id = %s AND ol_o_id = %s""", (w_id, d_id, o_id))
    
    res = cur.fetchall()
    
    for row in res:
        item_id, supply_w_id, ol_quantity, ol_amount, ol_delivery_d = row
        # print(f"The item details are {item_id}, {supply_w_id}, {ol_quantity}, {ol_amount} and {ol_delivery_d}")
        print(f"{item_id}, {supply_w_id}, {ol_quantity}, {ol_amount}, {ol_delivery_d}")
    
    
    conn.commit()
    cur.close()
    # conn.close()
    
# order_status_transaction(1, 1, 1)
