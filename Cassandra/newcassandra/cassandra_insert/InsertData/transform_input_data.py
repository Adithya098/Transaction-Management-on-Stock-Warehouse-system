import csv
from create_conn import get_conn_details
from cassandra.query import BatchStatement
from decimal import Decimal
from datetime import datetime

data_path = "/home/stuproj/cs4224b/data/"
# data_path = "D:/Outside Admission/NUS(Ms General Track)/Study/Semester3/CS5424DistributedDatabases/Project/project_files/data_files/"
timestamp_format = "%Y-%m-%d %H:%M:%S.%f"


WAREHOUSE_HEADERS = ['W_ID', 'W_NAME', 'W_STREET_1', 'W_STREET_2', 'W_CITY', 'W_STATE', 'W_ZIP', 'W_TAX', 'W_YTD']
DISTRICT_HEADERS = ['D_W_ID', 'D_ID', 'D_NAME', 'D_STREET_1', 'D_STREET_2', 'D_CITY', 'D_STATE', 'D_ZIP', 'D_TAX', 'D_YTD', 'D_NEXT_O_ID']
CUSTOMER_HEADERS = ['C_W_ID', 'C_D_ID', 'C_ID', 'C_FIRST', 'C_MIDDLE', 'C_LAST', 'C_STREET_1', 'C_STREET_2', 'C_CITY', 'C_STATE', 'C_ZIP', 'C_PHONE', 'C_SINCE', 'C_CREDIT', 'C_CREDIT_LIM', 'C_DISCOUNT', 'C_BALANCE', 'C_YTD_PAYMENT', 'C_PAYMENT_CNT', 'C_DELIVERY_CNT', 'C_DATA']
ORDER_HEADERS = ['O_W_ID', 'O_D_ID', 'O_ID', 'O_C_ID', 'O_CARRIER_ID', 'O_OL_CNT', 'O_ALL_LOCAL', 'O_ENTRY_D']
ITEM_HEADERS = ['I_ID', 'I_NAME', 'I_PRICE', 'I_IM_ID', 'I_DATA']
ORDER_LINE_HEADERS = ['OL_W_ID', 'OL_D_ID', 'OL_O_ID', 'OL_NUMBER', 'OL_I_ID', 'OL_DELIVERY_D', 'OL_AMOUNT', 'OL_SUPPLY_W_ID', 'OL_QUANTITY', 'OL_DIST_INFO']
STOCK_HEADERS = ['S_W_ID', 'S_I_ID', 'S_QUANTITY', 'S_YTD', 'S_ORDER_CNT', 'S_REMOTE_CNT', 'S_DIST_01', 'S_DIST_02', 'S_DIST_03', 'S_DIST_04', 'S_DIST_05', 'S_DIST_06', 'S_DIST_07', 'S_DIST_08', 'S_DIST_09', 'S_DIST_10', 'S_DATA']


WAREHOUSE_INSERT_QUERY = "INSERT INTO Warehouses (W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_TAX, W_YTD) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
DISTRICT_INSERT_QUERY = "INSERT INTO Districts (D_W_ID, D_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_TAX, D_YTD, D_NEXT_O_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
CUSTOMER_INSERT_QUERY = "INSERT INTO Customers (C_W_ID, C_D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT, C_DATA, W_NAME, D_NAME) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
ORDER_CUSTOMER_INSERT_QUERY = "INSERT INTO Order_Customer (O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D, C_FIRST, C_MIDDLE, C_LAST) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
ORDER_LINE_INSERT_QUERY = "INSERT INTO Order_Line (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO, I_NAME) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
ITEM_INSERT_QUERY = "INSERT INTO Item (I_ID, I_NAME, I_PRICE, I_IM_ID, I_DATA) VALUES (?, ?, ?, ?, ?)"
STOCK_ITEM_INSERT_QUERY = "INSERT INTO Stock_Item (S_W_ID, S_I_ID, S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10, S_DATA, I_NAME, I_PRICE) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
RELATED_CUSTOMER_INSERT_QUERY = "INSERT INTO Related_Customer_Order_Line_Table (W_ID, D_ID, O_ID, OL_NUMBER, I_ID) VALUES (?, ?, ?, ?, ?)"
CUSTOMER_BALANCE_INSERT_QUERY = "INSERT INTO Customer_Balance_Table (W_ID, D_ID, C_ID, C_BALANCE) VALUES (?, ?, ?, ?)"


# Function to return rows as dictionaries where keys are combination of unique identifiers
def convert_data_to_dict():

    # Reading warehouse.csv
    print('Reading warehouse.csv')
    data_file_path = data_path + ('warehouse.csv')
    with open(data_file_path, 'r') as f:
        reader_obj = csv.DictReader(f, fieldnames=WAREHOUSE_HEADERS)
        warehouse_dict = {row["W_ID"]: row for row in reader_obj}

    # Reading district.csv
    print('Reading district.csv')
    data_file_path = data_path + ('district.csv')
    with open(data_file_path, 'r') as f:
        reader_obj = csv.DictReader(f, fieldnames=DISTRICT_HEADERS)
        district_dict = {(row["D_W_ID"], row["D_ID"]): row for row in reader_obj}

    # Reading customer.csv
    print('Reading customer.csv')
    data_file_path = data_path + ('customer.csv')
    with open(data_file_path, 'r') as f:
        reader_obj = csv.DictReader(f, fieldnames=CUSTOMER_HEADERS)
        customer_dict = {(row["C_W_ID"], row["C_D_ID"], row["C_ID"]): row for row in reader_obj}

    # Reading order.csv
    # print('Reading order.csv')
    # data_file_path = data_path + ('order.csv')
    # with open(data_file_path, 'r') as f:
    #     reader_obj = csv.DictReader(f, fieldnames=ORDER_HEADERS)
    #     order_dict = {(row["O_W_ID"], row["O_D_ID"], row["O_ID"]): row for row in reader_obj}

    # Reading item.csv
    # print('Reading item.csv')
    # data_file_path = data_path + ('item.csv')
    # with open(data_file_path, 'r') as f:
    #     reader_obj = csv.DictReader(f, fieldnames=ITEM_HEADERS)
    #     item_dict = {row["I_ID"]: row for row in reader_obj}

    # Reading order-line.csv
    # print('Reading order-line.csv')
    # data_file_path = data_path + ('order-line.csv')
    # with open(data_file_path, 'r') as f:
    #     reader_obj = csv.DictReader(f, fieldnames=ORDER_LINE_HEADERS)
    #     order_line_dict = {(row["OL_W_ID"], row["OL_D_ID"], row["OL_O_ID"], row["OL_NUMBER"]): row for row in reader_obj}

    # Reading stock.csv
    # print('Reading stock.csv')
    # data_file_path = data_path + ('stock.csv')
    # with open(data_file_path, 'r') as f:
    #     reader_obj = csv.DictReader(f, fieldnames=STOCK_HEADERS)
    #     stock_dict = {(row["S_W_ID"], row["S_I_ID"]): row for row in reader_obj}

    # return warehouse_dict, district_dict, customer_dict, order_dict, item_dict, order_line_dict, stock_dict
    return warehouse_dict, district_dict, customer_dict


def create_order_customer_data(order_dict, customer_dict):
    order_customer_dict = {}
    # customer_balance_dict = {}
    for key, value in order_dict.items():
        key_customer = (value["O_W_ID"], value["O_D_ID"], value["O_C_ID"])
        customer_details = customer_dict[key_customer]
        temp = value
        temp['C_FIRST'] = customer_details['C_FIRST']
        temp['C_MIDDLE'] = customer_details['C_MIDDLE']
        temp['C_LAST'] = customer_details['C_LAST']
        # customer_balance_dict[key_customer] = {"W_ID":value['O_W_ID'],"D_ID":value['O_D_ID'],"C_ID":value['O_C_ID'],"C_BALANCE":value['C_BALANCE']}
        order_customer_dict[key] = temp

    return order_customer_dict

def modify_customer_data(customer_dict, warehouse_dict, district_dict):
    customer_modified_dict = {}
    customer_balance_dict = {}
    for key, value in customer_dict.items():
        key_warehouse = (value["C_W_ID"])
        key_district = (value["C_W_ID"], value["C_D_ID"])
        key_customer = (value["C_W_ID"], value["C_D_ID"], value["C_ID"])
        temp = value
        temp['W_NAME'] = (warehouse_dict[key_warehouse])['W_NAME']
        temp['D_NAME'] = (district_dict[key_district])['D_NAME']
        customer_balance_dict[key_customer] = {"W_ID":value['C_W_ID'],"D_ID":value['C_D_ID'],"C_ID":value['C_ID'],"C_BALANCE":value['C_BALANCE']}
        customer_modified_dict[key] = temp

    return customer_modified_dict,customer_balance_dict

def modify_order_line_data(order_line_dict, item_dict):
    order_line_modified_dict = {}
    created_dict_related_customer = {}
    for key, value in order_line_dict.items():
        key_item = (value["OL_I_ID"])
        temp = value
        temp['I_NAME'] = (item_dict[key_item])['I_NAME']
        order_line_modified_dict[key] = temp

        key_related = (value['OL_W_ID'],value['OL_D_ID'],value['OL_O_ID'], value['OL_NUMBER'], value['OL_I_ID'])
        created_dict_related_customer[key_related] = {"W_ID":value['OL_W_ID'],"D_ID":value['OL_D_ID'],"I_ID":value['OL_I_ID'],"O_ID":value['OL_O_ID']}

    return order_line_modified_dict,created_dict_related_customer


def modify_stock_data(stock_dict, item_dict):
    stock_modified_dict = {}
    for key, value in stock_dict.items():
        key_item = (value["S_I_ID"])
        temp = value
        temp['I_NAME'] = (item_dict[key_item])['I_NAME']
        temp['I_PRICE'] = (item_dict[key_item])['I_PRICE']
        stock_modified_dict[key] = temp

    return stock_modified_dict


def insert_data(session, insert_query_string, data_dict_full, table_name):
    batch_size = 100 
    insert_batch = BatchStatement()
    insert_statement = session.prepare(insert_query_string)
    count = 0

    for key,data_dict in data_dict_full.items():
        if(table_name=='Warehouse'):
            session.execute(insert_statement, (int(data_dict['W_ID']), data_dict['W_NAME'], data_dict['W_STREET_1'], data_dict['W_STREET_2'], data_dict['W_CITY'], data_dict['W_STATE'], data_dict['W_ZIP'], Decimal(data_dict['W_TAX']), Decimal(data_dict['W_YTD'])))
        elif(table_name=='District'):
            session.execute(insert_statement, (int(data_dict['D_W_ID']), int(data_dict['D_ID']), data_dict['D_NAME'], data_dict['D_STREET_1'], data_dict['D_STREET_2'], data_dict['D_CITY'], data_dict['D_STATE'], data_dict['D_ZIP'], Decimal(data_dict['D_TAX']), Decimal(data_dict['D_YTD']), int(data_dict['D_NEXT_O_ID'])))
        elif(table_name=='Customer'):
            session.execute(insert_statement, (int(data_dict['C_W_ID']), int(data_dict['C_D_ID']), int(data_dict['C_ID']), data_dict['C_FIRST'], data_dict['C_MIDDLE'], data_dict['C_LAST'], data_dict['C_STREET_1'], data_dict['C_STREET_2'], data_dict['C_CITY'], data_dict['C_STATE'], data_dict['C_ZIP'], data_dict['C_PHONE'], datetime.strptime(data_dict['C_SINCE'], timestamp_format), data_dict['C_CREDIT'], Decimal(data_dict['C_CREDIT_LIM']), Decimal(data_dict['C_DISCOUNT']), Decimal(data_dict['C_BALANCE']), float(data_dict['C_YTD_PAYMENT']), int(data_dict['C_PAYMENT_CNT']), int(data_dict['C_DELIVERY_CNT']), data_dict['C_DATA'], data_dict['W_NAME'], data_dict['D_NAME']))
        # elif(table_name=='Order_Customer'):
        #     session.execute(insert_statement, (int(data_dict['O_W_ID']), int(data_dict['O_D_ID']), int(data_dict['O_ID']), int(data_dict['O_C_ID']), int(data_dict['O_CARRIER_ID']) if(data_dict['O_CARRIER_ID']!='null') else None, Decimal(data_dict['O_OL_CNT']), Decimal(data_dict['O_ALL_LOCAL']), datetime.strptime(data_dict['O_ENTRY_D'], timestamp_format), data_dict['C_FIRST'], data_dict['C_MIDDLE'], data_dict['C_LAST']))
        # elif(table_name=='Order_Line'):
        #     session.execute(insert_statement, (int(data_dict['OL_W_ID']), int(data_dict['OL_D_ID']), int(data_dict['OL_O_ID']), int(data_dict['OL_NUMBER']), int(data_dict['OL_I_ID']), datetime.strptime(data_dict['OL_DELIVERY_D'], timestamp_format) if(data_dict['OL_DELIVERY_D']!='') else None, Decimal(data_dict['OL_AMOUNT']), int(data_dict['OL_SUPPLY_W_ID']), Decimal(data_dict['OL_QUANTITY']), data_dict['OL_DIST_INFO'], data_dict['I_NAME']))
        # elif(table_name=='Item'):
        #     session.execute(insert_statement, (int(data_dict['I_ID']), data_dict['I_NAME'], Decimal(data_dict['I_PRICE']), int(data_dict['I_IM_ID']), data_dict['I_DATA']))
        # elif(table_name=='Stock_Item'):
        #     session.execute(insert_statement, (int(data_dict['S_W_ID']), int(data_dict['S_I_ID']), Decimal(data_dict['S_QUANTITY']), Decimal(data_dict['S_YTD']), int(data_dict['S_ORDER_CNT']), int(data_dict['S_REMOTE_CNT']), data_dict['S_DIST_01'], data_dict['S_DIST_02'], data_dict['S_DIST_03'], data_dict['S_DIST_04'], data_dict['S_DIST_05'], data_dict['S_DIST_06'], data_dict['S_DIST_07'], data_dict['S_DIST_08'], data_dict['S_DIST_09'], data_dict['S_DIST_10'], data_dict['S_DATA'], data_dict['I_NAME'], data_dict['I_PRICE']))
        # elif(table_name=='Related_Customer_Order_Line_Table'):
        #     session.execute(insert_statement, (int(data_dict['W_ID']), int(data_dict['D_ID']), int(data_dict['O_ID']), int(data_dict['OL_NUMBER']), int(data_dict['I_ID'])))
        # elif(table_name=='Customer_Balance_Table'):
        #     session.execute(insert_statement, (int(data_dict['W_ID']), int(data_dict['D_ID']), int(data_dict['C_ID']), Decimal(data_dict['C_BALANCE'])))
        # count = count+1
        # if (count + 1) % batch_size == 0:
        #     count = 0
        #     session.execute(insert_batch)
        #     insert_batch = BatchStatement()

    # session.execute(insert_batch)


if __name__ == '__main__':
    warehouse_dict, district_dict, customer_dict = convert_data_to_dict()
    # order_customer_dict = create_order_customer_data(order_dict, customer_dict) 
    modified_customer_data,customer_balance_dict = modify_customer_data(customer_dict, warehouse_dict, district_dict)
    # modified_order_line_data,created_dict_related_customer = modify_order_line_data(order_line_dict, item_dict)
    # modified_stock_data = modify_stock_data(stock_dict, item_dict)
    
    cluster, session = get_conn_details()
    print("Inserting Warehouse")
    insert_data(session, WAREHOUSE_INSERT_QUERY, warehouse_dict, 'Warehouse')
    print("Inserting District")
    insert_data(session, DISTRICT_INSERT_QUERY, district_dict, 'District')
    print("Inserting Customer")
    insert_data(session, CUSTOMER_INSERT_QUERY, modified_customer_data, 'Customer')
    # print("Inserting Order_Customer")
    # insert_data(session, ORDER_CUSTOMER_INSERT_QUERY, order_customer_dict, 'Order_Customer')
    # print("Inserting Item")
    # insert_data(session, ITEM_INSERT_QUERY, item_dict, 'Item')
    # print("Inserting Order_Line")
    # insert_data(session, ORDER_LINE_INSERT_QUERY, modified_order_line_data, 'Order_Line')
    # print("Inserting for Related_Customer_Order_Line_Table")
    # insert_data(session, RELATED_CUSTOMER_INSERT_QUERY, created_dict_related_customer, 'Related_Customer_Order_Line_Table')
    # print("Inserting for Customer_Balance_Table")
    # insert_data(session, CUSTOMER_BALANCE_INSERT_QUERY, customer_balance_dict, 'Customer_Balance_Table')
    # print("Inserting Stock_Item")
    # insert_data(session, STOCK_ITEM_INSERT_QUERY, modified_stock_data, 'Stock_Item')


    session.shutdown()
    cluster.shutdown()

