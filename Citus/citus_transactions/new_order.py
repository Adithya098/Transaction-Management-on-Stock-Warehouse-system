import psycopg2
import datetime
import os

def new_order_transaction(
    w_id,
    d_id,
    c_id,
    num_items,
    item_numbers,
    supplier_warehouses,
    quantities,
    conn):
    
    # conn = psycopg2.connect(
    # host="localhost",
    # dbname="project",
    # user="cs4224b",
    # password="",
    # port="5098"
    # )
    
    cur = conn.cursor()
    cur.execute("SELECT d_next_o_id, d_tax FROM test_districts WHERE d_w_id = %s AND d_id = %s", (w_id, d_id))
    res = cur.fetchone()
    N = res[0]
    d_tax = res[1]
    # print(f"N is {N}")
    
    cur.execute("UPDATE test_districts SET d_next_o_id = d_next_o_id + 1 WHERE d_w_id = %s AND d_id = %s", (w_id, d_id))
    
    all_local = 1
    for warehouse in supplier_warehouses:
        if int(warehouse)!=int(w_id):
            all_local = 0
            break
    
    date = datetime.datetime.now()

    cur.execute("INSERT INTO test_orders VALUES (%s, %s, %s, %s, NULL, %s, %s, %s)", (w_id, d_id, N, c_id, num_items, all_local, date)) 
    item_names = []
    item_amounts = []
    stock_quantities = []
    total_amount = 0
    for item in range(num_items):
        stock_district = "S_DIST_" + str(d_id).zfill(2)
        cur.execute("SELECT s_quantity, %s FROM test_stocks WHERE s_i_id = %s AND s_w_id = %s",
                    (stock_district, item_numbers[item], supplier_warehouses[item]))
        response = cur.fetchone()
        # print(f" Response is {response[0]} and {response[1]}")
        
        S = int(response[0])
        district = response[1]
        adjusted_quantity = S - int(quantities[item])
        if adjusted_quantity < 10:
            adjusted_quantity += 100
        stock_quantities.append(adjusted_quantity)
        
        remote = 0
        if int(supplier_warehouses[item])!= int(w_id):
            remote = 1
        cur.execute("""UPDATE test_stocks SET s_quantity = %s, s_ytd = s_ytd + %s, s_order_cnt = s_order_cnt + 1,
                    s_remote_cnt = s_remote_cnt + %s WHERE s_i_id = %s AND s_w_id = %s""",
                    (adjusted_quantity, quantities[item], remote, item_numbers[item], supplier_warehouses[item]))
        cur.execute("SELECT i_price, i_name FROM test_items WHERE i_id = %s",(item_numbers[item],))
        response = cur.fetchone()
        price = response[0]
        name = response[1]
        item_names.append(name)
        item_price = price * quantities[item]
        item_amounts.append(item_price)
        total_amount = total_amount + item_price
        
        cur.execute("""INSERT INTO test_order_lines VALUES (%s, %s, %s, %s, %s, null, %s, %s, %s, %s)""",
            (w_id, d_id, N, item+1, item_numbers[item], item_price, supplier_warehouses[item], quantities[item], district))
    
    cur.execute("SELECT w_tax FROM test_warehouses WHERE w_id = %s", (w_id,))
    w_tax = cur.fetchone()[0]
    
    #cur.execute("SELECT d_tax FROM test_districts WHERE d_id = %s AND d_w_id = %s", (d_id, w_id))
    #d_tax = cur.fetchone()[0]
    
    cur.execute("SELECT c_last, c_credit, c_discount FROM test_customers WHERE c_id = %s AND c_d_id = %s AND c_w_id = %s", (c_id, d_id, w_id))
    res = cur.fetchone()
    c_last, c_credit, c_discount = res

    # print(f"total is {total_amount} dtax is {d_tax} wtax is {w_tax} discount is {c_discount}")
    total_amount = total_amount * (1 + d_tax + w_tax) * (1 - c_discount)    
    
    # print(f"The customer identifier is {w_id}, {d_id}, {c_id}, last name is {c_last}, credit is {c_credit}, discount is {c_discount}")
    print(f"{w_id}, {d_id}, {c_id}, {c_last}, {c_credit}, {c_discount}")

    # print(f"The warehouse tax is {w_tax}, district tax is {d_tax}")
    print(f"{w_tax}, {d_tax}")

    # print(f"The order number is {N} and date {date}")
    print(f"{N}, {date}")

    # print(f"The number of items is {num_items}, total amount is {total_amount}")
    print(f"{num_items}, {total_amount}")

    for item in range(num_items):
        print(f"""{item_numbers[item]}, {item_names[item]}, {supplier_warehouses[item]}, {quantities[item]}, {item_amounts[item]}, {item_amounts[item]}, {stock_quantities[item]}""")
        # print(f"""The item number is {item_numbers[item]}, item name is {item_names[item]}, supplier warehouse is {supplier_warehouses[item]}, 
        #         quantity is {quantities[item]}, item price is {item_amounts[item]}, item amount is {item_amounts[item]},
        #         stock quantity is {stock_quantities[item]}""")

    testing = os.environ.get('TESTING')
    if testing!='1':        
        conn.commit()
        
    cur.close()
    # conn.close()
