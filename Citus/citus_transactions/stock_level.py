import psycopg2

def stock_level_transaction(
    w_id,
    d_id,
    threshold,
    l,
    conn
):    
    cur = conn.cursor()
    cur.execute("""SELECT d_next_o_id FROM test_districts WHERE d_w_id = %s AND d_id = %s""", (w_id, d_id))
    N = cur.fetchone()[0]
    N = int(N)
    start = N - l
    cur.execute("""SELECT DISTINCT(ol_i_id) FROM test_order_lines WHERE ol_d_id = %s AND ol_w_id = %s
                AND ol_o_id BETWEEN %s AND %s""", (d_id, w_id, start, N-1))
    
    rows = cur.fetchall()
    vals = []
    for row in rows:
        vals.append(row[0])
    
    cur.execute("""SELECT count(*) FROM test_stocks WHERE s_w_id = %s AND s_i_id IN %s AND s_quantity< %s"""
                , (w_id, tuple(vals), threshold))
    
    count = cur.fetchone()[0]
    # print(f"The number of items is {count}")
    print(f"{count}")
    cur.close()
    # conn.close()

# stock_level_transaction(1, 1, 50, 5)
