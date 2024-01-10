from cassandra.cluster import Cluster
from decimal import Decimal
import os
import time
from datetime import datetime
from cassandra.policies import RoundRobinPolicy
import logging
logging.basicConfig(filename='payment.log', level=logging.DEBUG)

# Create a connection to the Cassandra cluster
policy = RoundRobinPolicy()
# Connect to the Cassandra cluster
cluster = Cluster(
    contact_points=['192.168.48.185', '192.168.48.192', '192.168.48.193', '192.168.48.194', '192.168.48.184'],
    load_balancing_policy=policy
)
session = cluster.connect('teambtest')  # Replace 'teambtest' with your keyspace name

def payment_transaction(session, c_w_id, c_d_id, c_id, payment):

    # Update the W_YTD value in the Warehouse table
    get_wytd_query = session.prepare("SELECT w_ytd FROM test_warehouse WHERE w_id = ?")
    wytd_result = session.execute(get_wytd_query.bind([c_w_id]))
    current_wytd = wytd_result[0].w_ytd if wytd_result else Decimal('0.0')
    new_wytd = current_wytd + payment
    update_warehouse_query = session.prepare("UPDATE test_warehouse SET w_ytd = ? WHERE w_id = ?")
    session.execute(update_warehouse_query.bind([new_wytd, c_w_id]))

    # Update the D_YTD value in the District table
    get_dytd_query = session.prepare("SELECT d_ytd FROM test_district WHERE d_w_id = ? AND d_id = ?")
    dytd_result = session.execute(get_dytd_query.bind([c_w_id, c_d_id]))
    current_dytd = dytd_result[0].d_ytd if dytd_result else Decimal('0.0')
    new_dytd = current_dytd + payment
    update_district_query = session.prepare("UPDATE test_district SET d_ytd = ? WHERE d_w_id = ? AND d_id = ?")
    session.execute(update_district_query.bind([new_dytd, c_w_id, c_d_id]))

    # Retrieve customer's current information
    get_customer_query = session.prepare("""
        SELECT c_first, c_middle, c_last, c_payment_cnt, c_street1, c_street2, c_city, c_state, c_zip,
               c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment
        FROM test_customer1
        WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?
    """)

    customer_result = session.execute(get_customer_query.bind([c_w_id, c_d_id, c_id]))
    customer = customer_result[0]  # Assuming there's only one matching customer

    # Calculate new values
    current_balance = customer.c_balance
    new_balance = current_balance - payment
    new_ytd_payment = customer.c_ytd_payment + float(payment)
    new_payment_cnt = customer.c_payment_cnt + 1

    # Update the customer's information
    update_customer_query = session.prepare("""
        UPDATE test_customer1
        SET c_balance = ?, c_ytd_payment = ?, c_payment_cnt = ?
        WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?
    """)

    session.execute(update_customer_query.bind([new_balance, new_ytd_payment, new_payment_cnt, c_w_id, c_d_id, c_id]))

    # Retrieve and display customer information
    customer_query = session.prepare("""
        SELECT c_w_id, c_d_id, c_id, c_first, c_middle, c_last, c_street1, c_street2, c_city, c_state, c_zip,
               c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance
        FROM test_customer1
        WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?
    """)

    customer_result = session.execute(customer_query.bind([c_w_id, c_d_id, c_id]))
    customer = customer_result[0]  # Assuming there's only one matching customer

    print("\nCustomer's Information:")
    print(f"C W ID: {customer.c_w_id}")
    print(f"C D ID: {customer.c_d_id}")
    print(f"C ID: {customer.c_id}")
    print(f"C FIRST: {customer.c_first}")
    print(f"C MIDDLE: {customer.c_middle}")
    print(f"C LAST: {customer.c_last}")
    print(f"C STREET 1: {customer.c_street1}")
    print(f"C STREET 2: {customer.c_street2}")
    print(f"C CITY: {customer.c_city}")
    print(f"C STATE: {customer.c_state}")
    print(f"C ZIP: {customer.c_zip}")
    print(f"C PHONE: {customer.c_phone}")
    print(f"C SINCE: {customer.c_since}")
    print(f"C CREDIT: {customer.c_credit}")
    print(f"C CREDIT LIM: {customer.c_credit_lim}")
    print(f"C DISCOUNT: {customer.c_discount}")
    print(f"C BALANCE: {customer.c_balance}")

    # Retrieve and display warehouse information
    warehouse_query = session.prepare("""
        SELECT w_street1, w_street2, w_city, w_state, w_zip
        FROM test_warehouse
        WHERE w_id = ?
    """)
    warehouse_result = session.execute(warehouse_query.bind([c_w_id]))
    warehouse = warehouse_result[0]  # Assuming there's only one matching warehouse

    print("\nWarehouse's Information:")
    print(f"W STREET 1: {warehouse.w_street1}")
    print(f"W STREET 2: {warehouse.w_street2}")
    print(f"W CITY: {warehouse.w_city}")
    print(f"W STATE: {warehouse.w_state}")
    print(f"W ZIP: {warehouse.w_zip}")

    # Retrieve and display district information
    district_query = session.prepare("""
        SELECT d_street1, d_street2, d_city, d_state, d_zip
        FROM test_district
        WHERE d_w_id = ? AND d_id = ?
    """)
    district_result = session.execute(district_query.bind([c_w_id, c_d_id]))
    district = district_result[0]  # Assuming there's only one matching district

    print("\nDistrict's Information:")
    print(f"D STREET 1: {district.d_street1}")
    print(f"D STREET 2: {district.d_street2}")
    print(f"D CITY: {district.d_city}")
    print(f"D STATE: {district.d_state}")
    print(f"D ZIP: {district.d_zip}")

    # Display payment amount
    print("\nPayment amount (PAYMENT):", payment)


#start_time = time.time()

# Call the payment_transaction function
#payment_transaction(session, 5, 1, 1, 10)

#end_time = time.time()
#execution_time = end_time - start_time
#print(f"Total execution time: {execution_time} seconds")

session.shutdown()
cluster.shutdown()
