from create_conn import get_conn_details

def top_balance_customer(session):
    customers_list = []
    for w_id in range(1, 11):
        top_balance_query = session.prepare("SELECT w_id, d_id, c_id, c_first, c_middle, c_last, w_name, d_name, c_balance from Customer_Balance_Table WHERE w_id=? ORDER BY c_balance DESC LIMIT 10")
        result = session.execute(top_balance_query, [w_id])
        for row in result:
            identifier = f"{row.w_id}_{row.d_id}_{row.c_id}_{row.c_first}_{row.c_middle}_{row.c_last}_{row.w_name}_{row.d_name}"
            t = (identifier, row.c_balance)
            customers_list.append(t)
    #print(f"The list is {list}")
    sorted_list = sorted(customers_list, key=lambda x: x[1], reverse=True)

    # Get the top 10 values
    top_10 = sorted_list[:10]
    for item in top_10:
        id = item[0]
        w_id, d_id, c_id, c_first, c_middle, c_last, w_name, d_name = id.split("_")
        print(f"{c_first}, {c_middle}, {c_last}")
        print(f"{item[1]}")
        print(f"{w_name}")
        print(f"{d_name}")

if __name__ == "__main__":
    cluster, session = get_conn_details()
    top_balance_customer(session)
    session.shutdown()
    cluster.shutdown()
