from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from create_conn import get_conn_details
import pandas as pd
import csv

# Connect to your Cassandra cluster
# cluster = Cluster(['192.168.48.184','192.168.48.185','192.168.48.192', '192.168.48.193', '192.168.48.194'])
# session = cluster.connect()
# cluster, session = get_conn_details()

def db_state(session):

    w_select_query = session.prepare("SELECT sum(W_YTD) as sum_w_ytd from Warehouses")
    w_ytd_sum = (session.execute(w_select_query)).one().sum_w_ytd

    d_select_query = session.prepare("SELECT sum(D_YTD) as sum_d_ytd,sum(D_NEXT_O_ID) as sum_next_o_id from Districts")
    d_result = (session.execute(d_select_query)).one()
    d_ytd_sum = d_result.sum_d_ytd
    d_next_o_id_sum = d_result.sum_next_o_id

    c_select_query = session.prepare("SELECT sum(C_BALANCE) as sum_c_balance, sum(C_YTD_PAYMENT) as sum_c_ytd_payment, sum(C_PAYMENT_CNT) as sum_c_payment_cnt, sum(C_DELIVERY_CNT) as sum_c_delivery_cnt from Customers")
    c_result = (session.execute(c_select_query)).one()
    sum_c_balance = c_result.sum_c_balance
    sum_c_ytd_payment = c_result.sum_c_ytd_payment
    sum_c_payment_cnt = c_result.sum_c_payment_cnt
    sum_c_delivery_cnt = c_result.sum_c_delivery_cnt

    o_select_query = session.prepare("SELECT max(O_ID) as max_o_id,sum(O_OL_CNT) as sum_o_ol_cnt from Order_Customer")
    o_result = (session.execute(o_select_query)).one()
    max_o_id = o_result.max_o_id
    sum_o_ol_cnt = o_result.sum_o_ol_cnt

    sum_ol_amount_total = 0
    sum_ol_quantity_total = 0
    for w_id in range(1,11):
        for d_id in range(1,11):
            ol_select_query = session.prepare("SELECT sum(OL_AMOUNT) as sum_ol_amount, sum(OL_QUANTITY) as sum_ol_quantity from Order_Line where ol_w_id=? and ol_d_id=?")
            ol_result = (session.execute(ol_select_query, (w_id, d_id))).one()
            sum_ol_amount = ol_result.sum_ol_amount
            sum_ol_quantity = ol_result.sum_ol_quantity
            sum_ol_amount_total = sum_ol_amount_total + sum_ol_amount
            sum_ol_quantity_total = sum_ol_quantity_total + sum_ol_quantity

    sum_s_quantity_total = 0
    sum_s_ytd_total = 0
    sum_s_order_cnt_total = 0
    sum_s_remote_cnt_total = 0
    for w_id in range(1,11):
        s_select_query = session.prepare("SELECT sum(S_QUANTITY) as sum_s_quantity, sum(S_YTD) as sum_s_ytd, sum(S_ORDER_CNT) as sum_s_order_cnt, sum(S_REMOTE_CNT) as sum_s_remote_cnt from Stock_Item where s_w_id=?")
        s_result = (session.execute(s_select_query, (w_id,))).one()
        sum_s_quantity = s_result.sum_s_quantity
        sum_s_ytd = s_result.sum_s_ytd
        sum_s_order_cnt = s_result.sum_s_order_cnt
        sum_s_remote_cnt = s_result.sum_s_remote_cnt
        sum_s_quantity_total = sum_s_quantity_total + sum_s_quantity
        sum_s_ytd_total = sum_s_ytd_total + sum_s_ytd
        sum_s_order_cnt_total = sum_s_order_cnt_total + sum_s_order_cnt
        sum_s_remote_cnt_total = sum_s_remote_cnt_total + sum_s_remote_cnt

    data = [w_ytd_sum, d_ytd_sum, d_next_o_id_sum, sum_c_balance, sum_c_ytd_payment, sum_c_payment_cnt, sum_c_delivery_cnt,
            max_o_id, sum_o_ol_cnt, sum_ol_amount_total, sum_ol_quantity_total, sum_s_quantity_total, sum_s_ytd_total, sum_s_order_cnt_total, sum_s_remote_cnt_total]

    with open('dbstate.csv', 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        for item in data:
            csv_writer.writerow([item])


if __name__ == "__main__":
    cluster, session = get_conn_details()
    db_state(session)
    session.shutdown()
    cluster.shutdown()


    
