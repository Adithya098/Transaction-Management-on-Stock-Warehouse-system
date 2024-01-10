from create_conn import get_conn_details
import time 
from datetime import datetime

def check_stock_threshold(session, w_id, d_id, threshold, l):
    try:
       select_query = f"SELECT D_NEXT_O_ID FROM districts WHERE D_W_ID = {w_id} AND D_ID = {d_id};"
       result_1 = session.execute(select_query)
       N = result_1.one().d_next_o_id
       N = int(N)
       start = N - l

       query_2 = session.execute("""SELECT ol_i_id FROM order_line WHERE ol_d_id = %s AND ol_w_id = %s AND ol_o_id >= %s AND ol_o_id <= %s;""",(d_id, w_id, start, N-1))
       #result_2 = session.execute(query_2, [d_id, w_id, start, N-1])
       item_ids = set(row.ol_i_id for row in query_2)
       count= 0
       for item_id in item_ids:
          query_3 = session.execute("""SELECT s_quantity FROM stock_item WHERE s_w_id = %s AND s_i_id = %s ;""",(w_id, item_id))
          #result_3 = session.execute(query_3, [w_id, item_id])
          if query_3.one().s_quantity < threshold:
             count += 1
       print(f"{count}")

    except Exception as e:
       print("An error occurred:", e)

 
#if __name__ == "__main__":
#    cluster, session = get_conn_details()
#    start_time = time.time()
#    check_stock_threshold(session, 1,3,20,9)
#    end_time = time.time()
#    execution_time = end_time - start_time
#    print(f"Execution Time: {execution_time} seconds")
#    session.shutdown()
#    cluster.shutdown()
