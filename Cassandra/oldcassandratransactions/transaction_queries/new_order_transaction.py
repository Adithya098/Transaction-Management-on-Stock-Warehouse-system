# New Order Transaction processes a new customer order.
# from cassandra.cluster import Cluster
from datetime import datetime
import time

# Create a connection to the Cassandra cluster
# cluster = Cluster(['192.168.48.184'])
# session = cluster.connect('teambtest')  # Replace 'teamb' with your keyspace name

#Inputs for Transaction
# w_id = int(input("Warehouse number: "))
# d_id = int(input("District number: "))
# c_id = int(input("Customer number: "))

# num_items = int(input("Number of Items to be ordered: "))

# item_numbers = list(map(lambda x: int(x),(input("Items numbers for all orders: ")).split(',')))

# supplier_warehouses = list(map(lambda x: int(x),(input("Supplier warehouses for all orders: ")).split(',')))

# quantities = list(map(lambda x: int(x),(input("Quantities for all orders: ")).split(',')))

def new_order_transaction(session, w_id, d_id, c_id, num_items, item_numbers, supplier_warehouses, quantities):

    start_time = time.time()
    #Step1. Let N denote value of the next available order number D_NEXT_O_ID for district (W_ID,D_ID) and select d_tax
    d_next_o_id_select_query = session.prepare("SELECT d_next_o_id,d_tax FROM test_district WHERE d_w_id=? AND d_id=?")

    try:
        result = (session.execute(d_next_o_id_select_query, (w_id, d_id))).one()
        d_next_o_id = result.d_next_o_id
        d_tax = result.d_tax
        # print(f"Selected orderId: {d_next_o_id} for (warehouseId,districtId) {(w_id, d_id)}")
    except Exception as e:
        print(f"An error {e} occurred while getting orderId for (warehouseId,districtId) {(w_id, d_id)}")

    new_d_next_o_id = d_next_o_id+1

    #Step2. Update the district (W_ID,D_ID) by incrementing D_NEXT_O_ID by one
    d_next_o_id_update_query = session.prepare("UPDATE test_district SET d_next_o_id=? WHERE d_w_id=? AND d_id=?")

    try:
        session.execute(d_next_o_id_update_query, (new_d_next_o_id, w_id, d_id))
        # print(f"New orderId: {new_d_next_o_id} set for (warehouseId,districtId) {(w_id, d_id)}")
    except Exception as e:
        print(f"An error {e} occurred while setting new orderId for (warehouseId,districtId) {(w_id, d_id)}")

    #Step3. Create a new order with 
    # O_ID = N
    # O_D_ID = D_ID
    # O_W_ID = W_ID
    # O_C_ID = C_ID
    # O_ENTRY_D = Current date and time
    # O_CARRIER_ID = null
    # O_OL_CNT = NUM_ITEMS
    # O_ALL_LOCAL = 0 if there exists some i ∈ [1,NUM_ITEMS] such that SUPPLIER_WAREHOUSE[i] != W_ID; otherwise, O_ALL_LOCAL = 1
    
    o_entry_d = datetime.now()
    o_all_local = 1

    for i in supplier_warehouses:
        if(i!=w_id):
            o_all_local = 0
            break
    
    new_order_insert_query = session.prepare("INSERT INTO test_orders (o_id, o_d_id, o_w_id, o_all_local, o_c_id, o_carrier_id, o_entry_d, o_ol_cnt) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")

    try:
        session.execute(new_order_insert_query, (new_d_next_o_id, d_id, w_id, o_all_local, c_id, None, o_entry_d, num_items))
        # print(f"New order created for (Warehouse number, District number, Order number): {(w_id, d_id, new_d_next_o_id)}")
    except Exception as e:
        print(f"An error {e} occurred while creating new order for (Warehouse number, District number, Order number): {(w_id, d_id, new_d_next_o_id)}")
    
    item_names = []
    item_amounts = []
    stock_quantities = []
    #Step4: Initialize TOTAL_AMOUNT = 0
    total_amount = 0

    #Step5: For i = 1 to NUM ITEMS
    for i in range(num_items):

        #(a) Let S QUANTITY denote the stock_quantity for item ITEM_NUMBER[i] and warehouse SUPPLIER_WAREHOUSE[i]

        stock_district = "s_dist_" + str(d_id).zfill(2)
        stock_info_select_query = session.prepare(f"SELECT s_quantity,{stock_district},s_ytd,s_order_cnt,s_remote_cnt FROM test_stock WHERE s_w_id=? AND s_i_id=?")
        try:
            result = (session.execute(stock_info_select_query, (supplier_warehouses[i], item_numbers[i]))).one()
            stock_quantity = result.s_quantity
            # s_dist_info = result.{stock_district}
            s_dist_info = getattr(result, stock_district)
            stock_ytd = result.s_ytd
            stock_order_cnt = result.s_order_cnt
            stock_remote_cnt = result.s_remote_cnt

            # print(f"Selected (s_quantity,s_dist_info,s_ytd,s_order_cnt,s_remote_cnt): {(stock_quantity,s_dist_info,stock_ytd,stock_order_cnt,stock_remote_cnt)} for (supplier_warehouse,item_number) {(supplier_warehouses[i], item_numbers[i])}")
        except Exception as e:
            print(f"An error {e} occurred while getting (s_quantity,s_info) for (supplier_warehouse,item_number) {(supplier_warehouses[i], item_numbers[i])}")

        #(b) ADJUSTED_QTY = S_QUANTITY − QUANTITY [i]

        adjusted_qty = stock_quantity - quantities[i]

        #(c) If ADJUSTED_QTY < 10, then set ADJUSTED_QTY = ADJUSTED_QTY + 100
        if(adjusted_qty<10):
            adjusted_qty = adjusted_qty+100

        stock_quantities.append(adjusted_qty)
        #(d) Update the stock for (ITEM_NUMBER[i], SUPPLIER_WAREHOUSE[i]) as follows:
        # • Update S_QUANTITY to ADJUSTED_QTY
        # • Increment S_YTD by QUANTITY[i]
        # • Increment S_ORDER_CNT by 1
        # • Increment S_REMOTE_CNT by 1 if SUPPLIER_WAREHOUSE[i] != W_ID
        stock_ytd = stock_ytd + quantities[i]
        stock_order_cnt = stock_order_cnt+1
        if(supplier_warehouses[i] != w_id):
            stock_remote_cnt = stock_remote_cnt+1

        stock_info_update_query = session.prepare("UPDATE test_stock SET s_quantity=?, s_ytd=?, s_order_cnt=?, s_remote_cnt=? WHERE s_w_id=? AND s_i_id=?")

        try:
            session.execute(stock_info_update_query, (adjusted_qty, stock_ytd, stock_order_cnt, stock_remote_cnt, supplier_warehouses[i], item_numbers[i]))
            # print(f"Stock table info updated with (s_quantity,s_ytd,s_order_cnt,s_remote_cnt) : {(adjusted_qty, stock_ytd, stock_order_cnt, stock_remote_cnt)} for (supplier_warehouse,item_number) {(supplier_warehouses[i], item_numbers[i])}")
        except Exception as e:
            print(f"An error {e} occurred while setting updating stock info for (supplier_warehouse,item_number) {(supplier_warehouses[i], item_numbers[i])}")

        #(e) ITEM_AMOUNT = QUANTITY[i] × I_PRICE, where I_PRICE is the price of ITEM_NUMBER[i]
        item_info_select_query = session.prepare("SELECT i_price,i_name FROM test_item WHERE i_id=?")

        try:
            result = (session.execute(item_info_select_query, (item_numbers[i],))).one()
            item_price = result.i_price
            item_name = result.i_name
            # print(f"Selected price: {item_price} for (itemId) {(item_numbers[i])}")
        except Exception as e:
            print(f"An error {e} occurred while getting price for (itemId) {(item_numbers[i])}")

        item_names.append(item_name)
        item_amount = item_price * quantities[i]
        item_amounts.append(item_amount)
        #(f) TOTAL_AMOUNT = TOTAL_AMOUNT + ITEM_AMOUNT
        total_amount = total_amount + item_amount

        #(g) Create a new order-line

        new_order_line_insert_query = session.prepare("INSERT INTO test_order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d, ol_dist_info) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

        try:
            session.execute(new_order_line_insert_query, (new_d_next_o_id, d_id, w_id, i+1, item_numbers[i], supplier_warehouses[i], quantities[i], item_amount, None, s_dist_info))
            # print(f"New order-line created for (Warehouse number, District number, Order number, Order-line number): {(w_id, d_id, new_d_next_o_id, i+1)}")
        except Exception as e:
            print(f"An error {e} occurred while creating new order-line for (Warehouse number, District number, Order number, Order-line number): {(w_id, d_id, new_d_next_o_id, i+1)}")

        
    #6. TOTAL_AMOUNT = TOTAL_AMOUNT × (1 +D_TAX +W_TAX) × (1 − C_DISCOUNT),
    # where W_TAX is the tax rate for warehouse W_ID, D_TAX is the tax rate for district (W_ID,
    # D_ID), and C_DISCOUNT is the discount for customer C_ID.

    ## select w_tax
    w_tax_select_query = session.prepare("SELECT w_tax FROM test_warehouse WHERE w_id=?")

    try:
        w_tax = (session.execute(w_tax_select_query, (w_id,))).one().w_tax
        # print(f"Selected w_tax: {w_tax} for (warehouseId) {(w_id)}")
    except Exception as e:
        print(f"An error {e} occurred while getting w_tax for (warehouseId) {(w_id)}")
        
    ## select d_tax
    # d_tax_select_query = session.prepare("SELECT d_tax FROM test_district WHERE d_w_id=? AND d_id=?")

    # try:
    #     d_tax = (session.execute(d_tax_select_query, (w_id, d_id))).one().d_tax
    #     print(f"Selected d_tax: {d_tax} for (warehouseId,districtId) {(w_id, d_id)}")
    # except Exception as e:
    #     print(f"An error occurred while getting d_tax for (warehouseId,districtId) {(w_id, d_id)}")

    
    # select c_discount
    c_discount_select_query = session.prepare("SELECT c_discount,c_last,c_credit FROM test_customer1 WHERE c_id=? AND c_w_id=? AND c_d_id=?")

    try:
        result = (session.execute(c_discount_select_query, (c_id, w_id, d_id))).one()
        c_discount = result.c_discount
        c_last = result.c_last
        c_credit = result.c_credit
        # print(f"Selected discount: {c_discount} for (customerId,warehouseId,districtId) {(c_id, w_id, d_id)}")
    except Exception as e:
        print(f"An error {e} occurred while getting customer discount for (customerId,warehouseId,districtId) {(c_id, w_id, d_id)}")

    total_amount = total_amount * (1+d_tax+w_tax) * (1 - c_discount)

    print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
    print("Results")
    print(f"Customer identifier (W_ID, D_ID, C_ID): {(w_id, d_id, c_id)}, lastname C_LAST: {c_last}, credit C_CREDIT: {c_credit}, discount C_DISCOUNT: {c_discount}")
    print("----------------------------------------------------------------------------------------")
    print(f"Warehouse tax rate W_TAX: {w_tax}, District tax rate D_TAX: {d_tax}")
    print("----------------------------------------------------------------------------------------")
    print(f"Order number O_ID: {new_d_next_o_id}, entry date O_ENTRY_D: {o_entry_d}")
    print("----------------------------------------------------------------------------------------")
    print(f"Number of items NUM_ITEMS: {num_items}, Total amount for order TOTAL_AMOUNT: {total_amount}")
    print("----------------------------------------------------------------------------------------")
    
    for i in range(num_items):
        print(f"ITEM_NUMBER[i]: {item_numbers[i]}, I_NAME: {item_names[i]}, SUPPLIER_WAREHOUSE[i]: {supplier_warehouses[i]}, QUANTITY[i]: {quantities[i]}, OL_AMOUNT: {item_amounts[i]}, S_QUANTITY: {stock_quantities[i]}")
    
    end_time = time.time()
    execution_time = end_time - start_time
    print("time taken to execute the new order transaction: ",execution_time)
    




new_order_transaction(session, w_id, d_id, c_id, num_items, item_numbers, supplier_warehouses, quantities)
