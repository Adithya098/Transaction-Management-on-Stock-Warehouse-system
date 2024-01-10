from create_conn import get_conn_details
import time

def popular_items_transaction(session, w_id, d_id, L):

    print(f"{w_id}, {d_id}")
    print(f"{L}")
    next_o_id_query = session.prepare("SELECT D_NEXT_O_ID from Districts WHERE D_W_ID=? AND D_ID=?")
    result = (session.execute(next_o_id_query, (w_id, d_id))).one()
    N = result.d_next_o_id
    start = N - int(L)

    order_details_query = session.prepare("SELECT O_ID, O_C_ID, O_ENTRY_D, C_FIRST, C_MIDDLE, C_LAST FROM Order_Customer WHERE O_D_ID = ? AND O_W_ID = ? AND O_ID>=? AND O_ID<=?")
    result = session.execute(order_details_query, (d_id, w_id, start, N-1))

    popular_item_names = {}
    item_hash = {}
    i = 0
    for x in result:
        i = i+1
        print(f"{x.o_id}, {x.o_entry_d}")
        print(f"{x.c_first}, {x.c_middle}, {x.c_last}")
        query = session.prepare("SELECT OL_I_ID, OL_QUANTITY, I_NAME FROM Order_Line WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?")
        items = session.execute(query, (w_id, d_id, x.o_id))
        max_val = 0
        all_items = list(items)
        for row in all_items:
            max_val = max(max_val, int(row.ol_quantity))

        for row in all_items:
            if int(row.ol_quantity) == max_val:
                print(f"{row.ol_i_id}, {row.i_name}")
                popular_item_names[row.ol_i_id] = row.i_name
                if row.ol_i_id in item_hash:
                    item_hash[row.ol_i_id] = 1+item_hash[row.ol_i_id]
                else:
                    item_hash[row.ol_i_id] = 1
        #print(f"The popular items are {popular_item_names}")

    total_number = len(all_items)

    for i_id, i_name in popular_item_names.items():
        occurances = item_hash[i_id]
        percentage = (occurances/total_number)*100
        print(f"{i_name}, {percentage}")


if __name__ == "__main__":
    cluster, session = get_conn_details()
    start = time.time()
    popular_items_transaction(session, 1, 1, 10)
    print(f"Here the value is {time.time()-start}")
    session.shutdown()
    cluster.shutdown()
