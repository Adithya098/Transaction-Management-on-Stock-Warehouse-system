import sys
import numpy as np
import psycopg2
import pandas as pd
import os
import fileinput


from new_order import new_order_transaction
from order_status import order_status_transaction
from payment import payment_transaction
from delivery import delivery_transaction
import time
from stock_level import stock_level_transaction
from top_balance_customers import top_balance_transaction
from popular_item import popular_item_transaction
from related_customers_test import related_customer_transaction
from update_metrics_summary import update_metrics_summary
from connection import get_connection
#from db_state import report_db_state

transactions = ['N', 'P', 'D', 'O', 'S', 'I', 'T', 'R']

if __name__ == "__main__":
    args = sys.argv
    path = args[1]
    os.environ['TESTING'] = '0'
    client = os.path.basename(path)
    #print(f"The client is {client}")
    conn = get_connection("project")
    
    fi = iter(fileinput.input(path))
    N_trans_time = 0
    N_trans_count = 0
    
    P_trans_time = 0
    P_trans_count = 0

    D_trans_time = 0
    D_trans_count = 0

    O_trans_time = 0
    O_trans_count = 0

    S_trans_time = 0
    S_trans_count = 0

    I_trans_time = 0
    I_trans_count = 0

    T_trans_time = 0
    T_trans_count = 0

    R_trans_time = 0
    R_trans_count = 0
    
    h_map = {
        N_trans_count := 0,
        N_trans_val := 0,
    }


    latencies = []
    start_time = time.time()
    for line in fi:
        transaction_start_time = time.time()
        vals = line.split(',')
        t_type = vals[0].strip()
        #print(f"The t type is {t_type}")
        try:
            if t_type == 'N':
                c_id = vals[1]
                w_id = vals[2]
                d_id = vals[3]
                n_lines = int(vals[4])
                ol_i_ids = []
                ol_supply_w_ids = []
                ol_quantities = []            
                for _ in range(n_lines):
                    next_line = next(fi).strip()
                    ol_i_id, ol_supply_w_id, ol_quantity = next_line.split(",")
                    ol_i_ids.append(int(ol_i_id))
                    ol_supply_w_ids.append(int(ol_supply_w_id))
                    ol_quantities.append(int(ol_quantity))
                new_order_transaction(w_id, d_id, c_id, n_lines, ol_i_ids, ol_supply_w_ids, ol_quantities, conn)
                N_trans_time = N_trans_time + ((time.time()-transaction_start_time)*1000)
                N_trans_count = N_trans_count + 1           
            elif t_type == 'P':
                c_w_id, c_d_id, c_id, payment = vals[1:]
                c_w_id = int(c_w_id)
                c_d_id = int(c_d_id)
                c_id = int(c_id)
                payment = float(payment)
                payment_transaction(c_w_id, c_d_id, c_id, payment, conn)
                P_trans_time = P_trans_time + ((time.time()-transaction_start_time)*1000)
                P_trans_count = P_trans_count + 1
                
            elif t_type == 'D':
                w_d_id, payment = vals[1:]
                w_d_id = int(w_d_id)
                payment = float(payment)  
                delivery_transaction(w_d_id, payment, conn) 
                D_trans_time = D_trans_time + ((time.time()-transaction_start_time)*1000)
                D_trans_count = D_trans_count + 1
                            
            elif t_type == 'O':
                C_W_ID, C_D_ID, C_ID = vals[1:]
                C_W_ID = int(C_W_ID)
                C_D_ID = int(C_D_ID)
                C_ID = int(C_ID)
                order_status_transaction(C_W_ID, C_D_ID, C_ID, conn)
                O_trans_time = O_trans_time + ((time.time()-transaction_start_time)*1000)
                O_trans_count = O_trans_count + 1
                            
            elif t_type == 'S':
                W_ID, D_ID,T,L = vals[1:]
                W_ID = int(W_ID)
                D_ID = int(W_ID)
                T = int(T)
                L = int(L)
                stock_level_transaction(W_ID, D_ID, T, L, conn)
                S_trans_time = S_trans_time + ((time.time()-transaction_start_time)*1000)
                S_trans_count = S_trans_count + 1
           
            elif t_type == 'I':
                W_ID,D_ID,L = vals[1:]
                W_ID = int(W_ID)
                D_ID = int(D_ID)
                L = int(L)
                popular_item_transaction(W_ID, D_ID, L, conn)
                I_trans_time = I_trans_time + ((time.time()-transaction_start_time)*1000)
                I_trans_count = I_trans_count + 1
            elif t_type == 'T':
                top_balance_transaction(conn)
                T_trans_time = T_trans_time + ((time.time()-transaction_start_time)*1000)
                T_trans_count = T_trans_count + 1
            elif t_type == 'R':
                W_ID,D_ID,C_ID= vals[1:]
                W_ID = int(W_ID)
                D_ID = int(D_ID)
                C_ID = int(C_ID)
                related_customer_transaction(W_ID,D_ID,C_ID,conn)
                R_trans_time = R_trans_time + ((time.time()-transaction_start_time)*1000)
                R_trans_count = R_trans_count + 1
            else:
                continue
            
            transaction_time = time.time()-transaction_start_time
            latencies.append(transaction_time*1000)
        except Exception as e:
            print(f"""We have an exception here for client {client} and the transaction is {t_type} and the exception is {e}""")
            conn.rollback()
            if t_type == 'N':
                print("inside the retry block for N")
                new_order_transaction(w_id, d_id, c_id, n_lines, ol_i_ids, ol_supply_w_ids, ol_quantities, conn)
            elif t_type == 'P':
                print("inside the retry blk for P")
                payment_transaction(c_w_id, c_d_id, c_id, payment, conn)
            elif t_type == 'D':
                print("inside the retry blk for D")
                delivery_transaction(w_d_id, payment, conn) 
            elif t_type == 'O':
                print("inside the retry blk for O")
                order_status_transaction(C_W_ID, C_D_ID, C_ID, conn)
            elif t_type == 'S':
                print("inside the retry blk for S")
                stock_level_transaction(W_ID, D_ID, T, L, conn)
            elif t_type == 'I':
                print("inside the retry blk for I")
                popular_item_transaction(W_ID, D_ID, L, conn)
            elif t_type == 'T':
                print("inside the retry blk for T")
                top_balance_transaction(conn)
            elif t_type == 'R':
                print("inside the retry blk for R")
                related_customer_transaction(W_ID,D_ID,C_ID,conn) 
            # conn = get_connection()

    end_time = time.time()
    diff = end_time-start_time
    overall_time = round(diff, 2)
    throughput = round(len(latencies) / overall_time, 2)
    average_latency = round(np.mean(latencies), 2)
    median_latency = round(np.median(latencies), 2)
    percentile_95_latency = round(np.percentile(latencies, 95), 2)
    percentile_99_latency = round(np.percentile(latencies, 99), 2)
    print(f"Done for client {client}")
    # print(f"number of transactions is {len(latencies)}")
    # print(f"overall time is {overall_time}")
    # print(f"throughput is {throughput}")
    # print(f"average_latency is {average_latency}")
    # print(f"median_latency is {median_latency}")
    # print(f"percentile_95_latency is {percentile_95_latency}")
    # print(f"percentile_99_latency is {percentile_99_latency}")
    print(f"avg time for N trans is {N_trans_time/N_trans_count} and count is {N_trans_count}")
    print(f"avg time for P trans is {P_trans_time/P_trans_count} and count is {P_trans_count}")
    print(f"avg time for D trans is {D_trans_time/D_trans_count} and count is {D_trans_count}")
    print(f"avg time for O trans is {O_trans_time/O_trans_count} and count is {O_trans_count}")
    print(f"avg time for S trans is {S_trans_time/S_trans_count} and count is {S_trans_count}")
    print(f"avg time for I trans is {I_trans_time/I_trans_count} and count is {I_trans_count}")
    print(f"avg time for T trans is {T_trans_time/T_trans_count} and count is {T_trans_count}")
    print(f"avg time for R trans is {R_trans_time/R_trans_count} and count is {R_trans_count}")

    csv_file_path = "clients.csv"
    data_to_append = pd.DataFrame({"client": [client], "number_of_executed_transactions": [len(latencies)],
                                   "total_transaction_execution_time": [overall_time], "transaction_throughput": [throughput],
                                   "average_transaction_latency": [average_latency], "95th_percentile_transaction_latency":[percentile_95_latency],
                                   "99th_percentile_transaction_latency": [percentile_99_latency]})
    
    file_exists = os.path.exists(csv_file_path)
    data_to_append.to_csv(csv_file_path, mode='a', header=not file_exists, index=False)
    update_metrics_summary()
    #report_db_state(conn)
    conn.close()
