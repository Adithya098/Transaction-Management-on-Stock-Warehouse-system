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
from update_metrics_summary import update_metrics_summary
from top_balance_customers import top_balance_transaction
from  related_customers import related_customer_transaction
from popular_item import popular_item_transaction
transactions = ['N', 'P', 'D', 'O', 'S', 'I', 'T', 'R']

if __name__ == "__main__":
    args = sys.argv
    client = args[1]
    print(f"The client is {client}")
    conn = psycopg2.connect(
    host="localhost",
    dbname="project",
    user="cs4224b",
    password="",
    port="5098"
    )
    
    num_success = 0
    num_error = 0
    num_transactions = 0

    latencies = []
    start_time = time.time()
    for line in sys.stdin:
       transaction_start_time = time.time()
       num_transactions = num_transactions+1
       vals = line.split(',')
       t_type = vals[0]
     
       if t_type == 'I':
          W_ID, D_ID,L = vals[1:]
          W_ID = int(W_ID)
          D_ID = int(W_ID)
          L = int(L)
          popular_item_transaction(W_ID, D_ID, L, conn)

       elif t_type == 'T':
          top_balance_transaction(conn)

       elif t_type == 'R':
          W_ID, D_ID,C_ID= vals[1:]
          W_ID = int(W_ID)
          D_ID = int(W_ID)
          C_ID = int(C_W_ID)
          related_customer_transaction(C_ID,W_ID,D_ID, conn)
       else:
            continue
    
    conn.close()
