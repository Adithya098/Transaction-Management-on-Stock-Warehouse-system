import sys
import time
from cassandra.cluster import Cluster
from decimal import Decimal
from cassandra.policies import RoundRobinPolicy
import logging
# Import your transaction functions here
from new_order_transaction import new_order_transaction
from payment_transaction import payment_transaction
from delivery_transaction import update_cassandra_data
from popular_transaction import get_last_orders
from order_status import retrieve_data_from_cassandra
from stock_level_transaction import check_stock_threshold
from top_balance_transaction import top_balance_transaction
from related_customer_transaction import fetch_related_customers

logging.basicConfig(filename='driver.log', level=logging.DEBUG)

def main():
    # Initialize the Cassandra cluster
    policy = RoundRobinPolicy()
    # Connect to the Cassandra cluster
    cluster = Cluster(
       contact_points=['192.168.48.185', '192.168.48.192', '192.168.48.193', '192.168.48.194', '192.168.48.184'],
       load_balancing_policy=policy
)
    session = cluster.connect("teambtest")  # Replace "your_keyspace" with your keyspace name

    # Define a dictionary to map transaction types to their corresponding functions
    transaction_functions = {
       'N': new_order_transaction,
       'P': payment_transaction,
       'D': update_cassandra_data,
       'O': retrieve_data_from_cassandra,
       'S': check_stock_threshold,
       'I': get_last_orders,
       'T': top_balance_transaction,
       'R': fetch_related_customers
    }
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

    latencies = []

    try:
        start_time = time.time()
        for line in sys.stdin:
            transaction_start_time = time.time()
            vals = line.strip().split(',')
            t_type = vals[0]
            
            if t_type in transaction_functions:
                transaction_function = transaction_functions[t_type]
                try:
                    # Extract parameters from the input
                    parameters = vals[1:]

                    if t_type == 'P':
                        w_id, d_id, c_id, payment = vals[1:]
                        w_id = int(w_id)
                        d_id = int(d_id)
                        c_id = int(c_id)
                        payment = Decimal(payment)  # Use float() for payment                        
                        transaction_function(cluster, session, w_id, d_id, c_id, payment)
                        P_trans_time = P_trans_time + ((time.time()-transaction_start_time)*1000)
                        P_trans_count = P_trans_count + 1

                    elif t_type == 'D':
                        w_id, CARRIER_ID = vals[1:]
                        w_id = int(w_id)
                        CARRIER_ID = int(CARRIER_ID)
                        transaction_function(cluster, session, w_id, CARRIER_ID)
                        D_trans_time = D_trans_time + ((time.time()-transaction_start_time)*1000)
                        D_trans_count = D_trans_count + 1

                    elif t_type == 'O':
                        w_id, d_id, c_id = vals[1:]
                        w_id = int(w_id)
                        d_id = int(d_id)
                        c_id = int(c_id)
                        transaction_function(cluster, session, w_id, d_id, c_id)
                        O_trans_time = O_trans_time + ((time.time()-transaction_start_time)*1000)
                        O_trans_count = O_trans_count + 1

                    elif t_type == 'S':
                        w_id, d_id, threshold, l = vals[1:]
                        w_id = int(w_id)
                        d_id = int(d_id)
                        threshold = int(threshold)
                        l = int(l)
                        transaction_function(session, w_id, d_id, threshold, l)
                        S_trans_time = S_trans_time + ((time.time()-transaction_start_time)*1000)
                        S_trans_count = S_trans_count + 1

                    elif t_type == 'I':
                        w_id, d_id, l = vals[1:]
                        w_id = int(w_id)
                        d_id = int(d_id)
                        l = int(l)
                        transaction_function(cluster, session, w_id, d_id, l)
                        I_trans_time = I_trans_time + ((time.time()-transaction_start_time)*1000)
                        I_trans_count = I_trans_count + 1

                    elif t_type == 'T':
                        transaction_function(cluster, session)
                        T_trans_time = T_trans_time + ((time.time()-transaction_start_time)*1000)
                        T_trans_count = T_trans_count + 1

                    elif t_type == 'R':
                        w_id, d_id, c_id = vals[1:]
                        w_id = int(w_id)
                        d_id = int(d_id)
                        c_id = int(c_id)
                        transaction_function(cluster, session, w_id, d_id, c_id)
                        R_trans_time = R_trans_time + ((time.time()-transaction_start_time)*1000)
                        R_trans_count = R_trans_count + 1

                    transaction_time = time.time() - transaction_start_time
                    latencies.append(transaction_time * 1000)
                except Exception as e:
                    print(f"Error executing transaction {t_type}: {str(e)}")
            else:
                print(f"Unknown transaction type: {t_type}")

        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Total execution time: {execution_time} seconds") 
        overall_time = round(end_time - start_time, 2)
        throughput = round(len(latencies) / overall_time, 2)
        average_latency = round(sum(latencies) / len(latencies), 2)

        print(f"Overall execution time: {overall_time} seconds")
        print(f"Transaction throughput: {throughput} transactions per second")
        print(f"Average transaction latency: {average_latency} ms")
 
        print(f"avg time for N trans is {N_trans_time/N_trans_count} and count is {N_trans_count}")
        print(f"avg time for P trans is {P_trans_time/P_trans_count} and count is {P_trans_count}")
        print(f"avg time for D trans is {D_trans_time/D_trans_count} and count is {D_trans_count}")
        print(f"avg time for O trans is {O_trans_time/O_trans_count} and count is {O_trans_count}")
        print(f"avg time for S trans is {S_trans_time/S_trans_count} and count is {S_trans_count}")
        print(f"avg time for I trans is {I_trans_time/I_trans_count} and count is {I_trans_count}")
        print(f"avg time for T trans is {T_trans_time/T_trans_count} and count is {T_trans_count}")
        print(f"avg time for R trans is {R_trans_time/R_trans_count} and count is {R_trans_count}")
       
    finally:
        cluster.shutdown()
        session.shutdown()

if __name__ == "__main__":
    main()

