from create_conn import get_conn_details
from datetime import datetime
import time


def delivery_transaction(session, w_id, carrier_id):
    # start_time = time.time()
    w_name_select_query = session.prepare("SELECT W_NAME FROM Warehouses WHERE W_ID=?")

    result_warehouse = (session.execute(w_name_select_query, (w_id,))).one()
    w_name = result_warehouse.w_name
    for d_id in range(1,11):

        d_name_select_query = session.prepare("SELECT D_NAME FROM Districts WHERE D_W_ID=? AND D_ID=?")

        result_district = (session.execute(d_name_select_query, (w_id,d_id))).one()
        d_name = result_district.d_name
        # start_time_min_order_id_query = time.time()
        min_order_select_query = session.prepare("SELECT O_ID,C_ID FROM Order_Customer_No_Carrier WHERE W_ID=? AND D_ID=? order by O_ID asc limit 1")

        min_order_result = (session.execute(min_order_select_query, (w_id,d_id))).one()
        
        if(min_order_result==None):
            continue
            
        min_o_id = min_order_result.o_id
        print(min_o_id)
        c_id_for_min_o_id = min_order_result.c_id
        print(c_id_for_min_o_id)
        # end_time_min_order_id_query = time.time()
        # execution__min_order_id_time = end_time_min_order_id_query - start_time_min_order_id_query
        # print("time taken to execute the min_order_id_query transaction: ",execution__min_order_id_time)

        # start_time_min_customer_id_query = time.time()
        # min_order_select_query = session.prepare("SELECT O_C_ID FROM Order_Customer WHERE O_W_ID=? AND O_D_ID=? AND O_ID=?")

        # c_id_for_min_o_id = (session.execute(min_order_select_query, (w_id,d_id,min_o_id))).one().o_c_id
        # end_time_min_cust_id_query = time.time()
        # execution__min_cust_id_time = end_time_min_cust_id_query - start_time_min_customer_id_query
        # print("time taken to execute the min_customer_id_query transaction: ",execution__min_cust_id_time)

        # start_time_update_carrier_id_query = time.time()
        update_min_o_id_query = session.prepare("UPDATE Order_Customer SET O_CARRIER_ID=? WHERE O_W_ID=? AND O_D_ID=? AND O_ID=?")
        session.execute(update_min_o_id_query, (carrier_id, w_id, d_id, min_o_id))

        del_entry_no_carrier_query = session.prepare("DELETE from Order_Customer_No_Carrier where W_ID=? AND D_ID=? AND O_ID=?")
        session.execute(del_entry_no_carrier_query, (w_id, d_id, min_o_id))

        # end_time_update_carrier_id_query = time.time()
        # execution__min_carrier_id_time = end_time_update_carrier_id_query - start_time_update_carrier_id_query
        # print("time taken to execute the min_customer_id_query transaction: ",execution__min_carrier_id_time)

        # start_time_order_line_id_query = time.time()
        ol_select_query = session.prepare("SELECT OL_NUMBER,OL_AMOUNT from Order_Line WHERE OL_W_ID=? AND OL_D_ID=? AND OL_O_ID=?")
        ol_result = (session.execute(ol_select_query, (w_id, d_id, min_o_id)))

        ol_delivery_d = datetime.now()

        # update_ol_for_min_o_id_query = session.prepare("UPDATE Order_Line SET OL_DELIVERY_D=? WHERE OL_W_ID=? AND OL_D_ID=? AND OL_O_ID=?")
        # session.execute(update_ol_for_min_o_id_query, (ol_delivery_d, w_id, d_id, min_o_id))

        # end_time_order_line_id_query = time.time()
        # execution__min_order_line_id_time = end_time_order_line_id_query - start_time_order_line_id_query
        # print("time taken to execute the select order line transaction: ",execution__min_order_line_id_time)

        # ol_sum = result_order_line.one().ol_sum
        
        sum_ol_amount = 0
        # start_time_order_line_id_query = time.time()
        for order_line in ol_result:
            sum_ol_amount = sum_ol_amount + order_line.ol_amount
            update_ol_for_min_o_id_query = session.prepare("UPDATE Order_Line SET OL_DELIVERY_D=? WHERE OL_W_ID=? AND OL_D_ID=? AND OL_O_ID=? AND OL_NUMBER=?")
            session.execute(update_ol_for_min_o_id_query, (ol_delivery_d, w_id, d_id, min_o_id, order_line.ol_number))                


        # ol_sum_query = session.prepare("SELECT SUM(OL_AMOUNT) as SUM_ALL from Order_Line WHERE OL_W_ID=? AND OL_D_ID=? AND OL_O_ID=?")
        # all_ol_sum = (session.execute(ol_sum_query, (w_id, d_id, min_o_id)))[0].sum_all

        select_customer_query = session.prepare("SELECT C_BALANCE,C_DELIVERY_CNT,C_FIRST, C_MIDDLE, C_LAST from Customers WHERE C_W_ID=? AND C_D_ID=? AND C_ID=?")
        result = (session.execute(select_customer_query, (w_id, d_id, c_id_for_min_o_id))).one()
        c_balance = result.c_balance
        c_delivery_cnt = result.c_delivery_cnt
        c_first = result.c_first
        c_middle = result.c_middle
        c_last = result.c_last
        new_c_balance = c_balance + sum_ol_amount
        new_c_delivery_cnt = c_delivery_cnt + 1

        update_customer_query = session.prepare("UPDATE Customers SET C_BALANCE=?,C_DELIVERY_CNT=? WHERE C_W_ID=? AND C_D_ID=? AND C_ID=?")
        session.execute(update_customer_query, (new_c_balance, new_c_delivery_cnt, w_id, d_id, c_id_for_min_o_id))

        # customer_balance_update_query = session.prepare("UPDATE Customer_Balance_Table SET C_BALANCE=? WHERE C_W_ID=? AND C_D_ID=? AND C_ID=?")
        # session.execute(customer_balance_update_query, (new_c_balance, w_id, d_id, c_id_for_min_o_id))

        customer_del_balance_update_query = session.prepare("DELETE from Customer_Balance_Table WHERE W_ID=? AND C_BALANCE=? AND D_ID=? AND C_ID=?")
        session.execute(customer_del_balance_update_query, (w_id, c_balance, d_id, c_id_for_min_o_id))

        # customer_ins_balance_update_query = session.prepare("INSERT INTO Customer_Balance_Table WHERE C_W_ID=? AND C_BALANCE=? AND C_D_ID=? AND C_ID=?")
        # session.execute(customer_ins_balance_update_query, (w_id, new_c_balance, d_id, c_id_for_min_o_id))

        customer_ins_balance_update_query = session.prepare("INSERT INTO Customer_Balance_Table (W_ID, D_ID, C_ID, C_BALANCE, C_FIRST, C_MIDDLE, C_LAST, D_NAME, W_NAME) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
        session.execute(customer_ins_balance_update_query, (w_id, d_id, c_id_for_min_o_id, new_c_balance, c_first, c_middle, c_last, d_name, w_name))
        
    # end_time = time.time()
    # execution_time = end_time - start_time
    # print("time taken to execute the delivery transaction: ",execution_time)




# if __name__ == "__main__":
#     cluster, session = get_conn_details()
#     delivery_transaction(session, 2, 778)
#     session.shutdown()
#     cluster.shutdown()
