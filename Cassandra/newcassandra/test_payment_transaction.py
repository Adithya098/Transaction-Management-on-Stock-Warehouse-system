from create_conn import get_conn_details
import time


def payment_transaction(session, c_w_id, c_d_id, c_id, payment):
    start_time = time.time()

    #Step 1
    w_ytd_select_query = session.prepare("SELECT W_YTD,W_STREET_1,W_STREET_2,W_CITY,W_STATE,W_ZIP,W_NAME FROM Warehouses WHERE W_ID=?")

    result_warehouse = (session.execute(w_ytd_select_query, (c_w_id,))).one()
    w_ytd = result_warehouse.w_ytd
    new_w_ytd = w_ytd+payment

    w_ytd_update_query = session.prepare("UPDATE Warehouses SET W_YTD=? WHERE W_ID=?")

    session.execute(w_ytd_update_query, (new_w_ytd, c_w_id))

    #Step 2
    d_ytd_select_query = session.prepare("SELECT D_YTD,D_STREET_1,D_STREET_2,D_CITY,D_STATE,D_ZIP,D_NAME FROM Districts WHERE D_W_ID=? AND D_ID=?")

    result_district = (session.execute(d_ytd_select_query, (c_w_id,c_d_id))).one()
    d_ytd = result_district.d_ytd
    new_d_ytd = d_ytd+payment

    d_ytd_update_query = session.prepare("UPDATE Districts SET D_YTD=? WHERE D_W_ID=? AND D_ID=?")

    session.execute(d_ytd_update_query, (new_d_ytd, c_w_id, c_d_id))

    #Step 3
    customer_select_query = session.prepare("SELECT C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT FROM Customers WHERE C_W_ID=? AND C_D_ID=? AND C_ID=?")
    result_customer = (session.execute(customer_select_query, (c_w_id, c_d_id, c_id))).one()
    c_balance = result_customer.c_balance
    c_ytd_payment = result_customer.c_ytd_payment
    c_payment_cnt = result_customer.c_payment_cnt
    # result.c_first
    # result.c_middle
    # result.c_last
    # result.c_street_1
    # result.c_street_2
    # result.c_city
    # result.c_state
    # result.c_zip
    # result.c_phone
    # result.c_since
    # result.c_credit
    # result.c_credit_lim
    # result.c_discount
    new_c_balance = c_balance - payment
    new_c_ytd_payment = c_ytd_payment + float(payment)
    new_c_payment_cnt = c_payment_cnt + 1
    customer_update_query = session.prepare("UPDATE Customers SET C_BALANCE=?, C_YTD_PAYMENT=?, C_PAYMENT_CNT=? WHERE C_W_ID=? AND C_D_ID=? AND C_ID=?")
    session.execute(customer_update_query, (new_c_balance, new_c_ytd_payment, new_c_payment_cnt, c_w_id, c_d_id, c_id))

    customer_del_balance_update_query = session.prepare("DELETE from Customer_Balance_Table WHERE W_ID=? AND C_BALANCE=? AND D_ID=? AND C_ID=?")
    session.execute(customer_del_balance_update_query, (c_w_id, c_balance, c_d_id, c_id))

    customer_ins_balance_update_query = session.prepare("INSERT INTO Customer_Balance_Table (W_ID, D_ID, C_ID, C_BALANCE, C_FIRST, C_MIDDLE, C_LAST, D_NAME, W_NAME) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
    session.execute(customer_ins_balance_update_query, (c_w_id, c_d_id, c_id, new_c_balance, result_customer.c_first, result_customer.c_middle, result_customer.c_last, result_district.d_name,result_warehouse.w_name))

    # print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
    # print("Results")
    # print(f"Customers identifier (C_W_ID, C_D_ID, C_ID): {(c_w_id, c_d_id, c_id)}")
    # print(f"Customers name (C_FIRST, C_MIDDLE, C_LAST): {(result_customer.c_first, result_customer.c_middle, result_customer.c_last)}")
    # print(f"Customers address (C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP): {(result_customer.c_street_1, result_customer.c_street_2, result_customer.c_city, result_customer.c_state, result_customer.c_zip)}")
    # print(f"Customers address (C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE): {(result_customer.c_phone, result_customer.c_since, result_customer.c_credit, result_customer.c_credit_lim, result_customer.c_discount,new_c_balance)}")
    # print("----------------------------------------------------------------------------------------")
    # print(f"Warehouses address (W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP): {(result_warehouse.w_street_1, result_warehouse.w_street_2, result_warehouse.w_city, result_warehouse.w_state, result_warehouse.w_zip)}")
    # print("----------------------------------------------------------------------------------------")
    # print(f"Districts address (D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP): {(result_district.d_street_1, result_district.d_street_2, result_district.d_city, result_district.d_state, result_district.d_zip)}")
    # print("----------------------------------------------------------------------------------------")
    # print(f"Payment amount PAYMENT: {payment}")

    print(f"{(c_w_id, c_d_id, c_id)}")
    print(f"{(result_customer.c_first, result_customer.c_middle, result_customer.c_last)}")
    print(f"{(result_customer.c_street_1, result_customer.c_street_2, result_customer.c_city, result_customer.c_state, result_customer.c_zip)}")
    print(f"{(result_customer.c_phone, result_customer.c_since, result_customer.c_credit, result_customer.c_credit_lim, result_customer.c_discount,new_c_balance)}")
    print(f"{(result_warehouse.w_street_1, result_warehouse.w_street_2, result_warehouse.w_city, result_warehouse.w_state, result_warehouse.w_zip)}")
    print(f"{(result_district.d_street_1, result_district.d_street_2, result_district.d_city, result_district.d_state, result_district.d_zip)}")
    print(f"{payment}")

    end_time = time.time()
    execution_time = end_time - start_time
    print("time taken to execute the payment transaction: ",execution_time)


if __name__ == "__main__":
    cluster, session = get_conn_details()
    payment_transaction(session, 1, 1, 1584, 5000)
    session.shutdown()
    cluster.shutdown()
