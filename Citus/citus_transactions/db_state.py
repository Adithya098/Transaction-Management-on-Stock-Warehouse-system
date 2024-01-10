import psycopg2
import csv

def report_db_state(conn):
    cur = conn.cursor()
    cur.execute("select sum(w_ytd) from test_warehouses")
    w_ytd = cur.fetchone()[0]
    
    cur.execute("select sum(d_ytd), sum(d_next_o_id) from test_districts")
    res = cur.fetchone()
    d_ytd = res[0]
    d_next_o_id = res[1]
    
    cur.execute("select sum(C_BALANCE), sum(C_YTD_PAYMENT), sum(C_PAYMENT_CNT), sum(C_DELIVERY_CNT) from test_customers")
    res = cur.fetchone()
    c_balance = res[0]
    c_ytd_payment = res[1]
    c_payment_cnt = res[2]
    c_delivery_cnt = res[3]
    
    cur.execute("select max(O_ID), sum(O_OL_CNT) from test_orders")
    res = cur.fetchone()
    o_id = res[0]
    o_ol_cnt = res[1]
    
    cur.execute("select sum(OL_AMOUNT), sum(OL_QUANTITY) from test_order_lines")
    res = cur.fetchone()
    ol_amount = res[0]
    ol_quantity = res[1]
    
    cur.execute("select sum(S_QUANTITY), sum(S_YTD), sum(S_ORDER_CNT), sum(S_REMOTE_CNT) from test_stocks")
    res = cur.fetchone()
    s_quantity = res[0]
    s_ytd = res[1]
    s_order_cnt = res[2]
    s_remote_cnt = res[3]
    
    data = [w_ytd, d_ytd, d_next_o_id, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt,
            o_id, o_ol_cnt, ol_amount, ol_quantity, s_quantity, s_ytd, s_order_cnt, s_remote_cnt]
    
    with open('dbstate.csv', 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        for item in data:
            csv_writer.writerow([item])
      
    #print(f"""The vals are {w_ytd}, {d_ytd}, {d_next_o_id}, {c_balance}, {c_ytd_payment}, {c_payment_cnt}
    #      {c_delivery_cnt}, {o_id}, {o_ol_cnt}, {ol_amount}, {ol_quantity}, {s_quantity}, {s_ytd}, 
    #      {s_order_cnt}, {s_remote_cnt}""")
conn = psycopg2.connect(
     host="localhost",
     dbname="project",
     user="cs4224b",
     password="",
     port="5098"
     )
report_db_state(conn)
