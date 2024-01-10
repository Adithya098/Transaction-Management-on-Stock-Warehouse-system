from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import time
from datetime import datetime
import logging
from cassandra.policies import RoundRobinPolicy

logging.basicConfig(filename='cassandra.log', level=logging.DEBUG)

def top_balance_transaction(session):
    # Define the CQL query to retrieve customers with outstanding balance
    query = """
        SELECT c_first, c_middle, c_last, c_balance, c_w_id, c_d_id
        FROM test_customer1;
    """

    # Execute the query to get customers with outstanding balance
    result = session.execute(query)

    # Collect all results and sort them by c_balance in descending order
    #sorted_customers = sorted(result, key=lambda customer: customer.c_balance, reverse=True)

    # Get the top 10 customers
    #top_10_customers = sorted_customers[:10]
    # Filter and sort the data in Python
    filtered_data = [row for row in result if row.c_balance is not None]
    sorted_data = sorted(filtered_data, key=lambda c: c.c_balance, reverse=True)
    top_10 = sorted_data[:10]
    # Print the results
    print("Top-10 Customers by Outstanding Balance:")
    for customer in top_10:
        w_name_query = session.prepare("SELECT w_name FROM test_warehouse WHERE w_id = ?")
        w_name_result = session.execute(w_name_query.bind((customer.c_w_id,)))

        d_name_query = session.prepare("SELECT d_name FROM test_district WHERE d_w_id = ? and d_id = ?")
        d_name_result = session.execute(d_name_query.bind((customer.c_w_id, customer.c_d_id)))

        w_name = w_name_result[0].w_name if w_name_result else ""
        d_name = d_name_result[0].d_name if d_name_result else ""

        print("Name:", customer.c_first, customer.c_middle, customer.c_last)
        print("Outstanding Balance:", customer.c_balance)
        print("Warehouse Name:", w_name)
        print("District Name:", d_name)
        print()

policy = RoundRobinPolicy()
# Connect to the Cassandra cluster
cluster = Cluster(
    contact_points=['192.168.48.185', '192.168.48.192', '192.168.48.193', '192.168.48.194', '192.168.48.184'],
    load_balancing_policy=policy
)
session = cluster.connect('teambtest')  # Replace teambtest with your keyspace name

start_time=time.time()
# Call the Top-Balance Transaction function
top_balance_transaction(session)

end_time = time.time()
execution_time = end_time - start_time
print(f"Total execution time: {execution_time} seconds")
# Close the Cassandra session and cluster when done
session.shutdown()
cluster.shutdown()
