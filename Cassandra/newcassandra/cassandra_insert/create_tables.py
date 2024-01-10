from create_conn import get_conn_details


# Drop all the existing tables
def empty_tables(cluster,session):
    pass
    # session.execute("DROP TABLE IF EXISTS Warehouses")
    # session.execute("DROP TABLE IF EXISTS Districts")
    # session.execute("DROP TABLE IF EXISTS Customers")
    # session.execute("DROP TABLE IF EXISTS Order_Customer")
    # session.execute("DROP TABLE IF EXISTS Item")
    # session.execute("DROP TABLE IF EXISTS Order_Line")
    # session.execute("DROP TABLE IF EXISTS Stock_Item")
    # session.execute("DROP TABLE IF EXISTS Related_Customer_Order_Line_Table")
    # session.execute("DROP TABLE IF EXISTS Customer_Balance_Table")

# Create all tables
def create_tables(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS Warehouses (
            W_ID INT, --Warehouse number
            W_NAME VARCHAR, --Warehouse name
            W_STREET_1 VARCHAR, --Warehouse address
            W_STREET_2 VARCHAR, --Warehouse address
            W_CITY VARCHAR, --Warehouse address
            W_STATE VARCHAR, --Warehouse address
            W_ZIP VARCHAR, --Warehouse address
            W_TAX DECIMAL, --Warehouse sales tax rate
            W_YTD DECIMAL, --Year to date amount paid to warehouse
            PRIMARY KEY (W_ID)
    )""")

    session.execute("""
        CREATE TABLE IF NOT EXISTS Districts (
            D_W_ID INT, --Warehouse number 
            D_ID INT, --District number
            D_NAME VARCHAR, --District name
            D_STREET_1 VARCHAR, --District address
            D_STREET_2 VARCHAR, --District address
            D_CITY VARCHAR, --District address
            D_STATE VARCHAR, --District address
            D_ZIP VARCHAR, --District address
            D_TAX DECIMAL, --District sales tax rate
            D_YTD DECIMAL, --Year to date amount paid to district
            D_NEXT_O_ID INT, --Next available order number for district
            PRIMARY KEY ((D_W_ID), D_ID)
    )""")

    session.execute("""
        CREATE TABLE IF NOT EXISTS Customers (
            C_W_ID INT, --Warehouse number
            C_D_ID INT, --District number

            W_NAME VARCHAR, --Warehouse name
            D_NAME VARCHAR, --District name

            C_ID INT, --Customer number 
            C_FIRST VARCHAR, --Customer name 
            C_MIDDLE VARCHAR, --Customer name 
            C_LAST VARCHAR, --Customer name 
            C_STREET_1 VARCHAR, --Customer address 
            C_STREET_2 VARCHAR, --Customer address 
            C_CITY VARCHAR, --Customer address 
            C_STATE VARCHAR, --Customer address 
            C_ZIP VARCHAR, --Customer address 
            C_PHONE VARCHAR, --Customer phone 
            C_SINCE TIMESTAMP, --Date and time when entry was created 
            C_CREDIT VARCHAR, --Customer credit status 
            C_CREDIT_LIM DECIMAL, --Customer credit limit 
            C_DISCOUNT DECIMAL, --Customer discount rate 
            C_BALANCE DECIMAL, --Balance of customers outstanding payment 
            C_YTD_PAYMENT FLOAT, --Year to date payment by customer 
            C_PAYMENT_CNT INT, --Number of payments made 
            C_DELIVERY_CNT INT, --Number of deliveries made to customer 
            C_DATA VARCHAR, --Miscellaneous data
            PRIMARY KEY ((C_W_ID), C_D_ID, C_ID)
    )""")

    session.execute("""
        CREATE TABLE IF NOT EXISTS Order_Customer (
            O_W_ID INT, --Warehouse number
            O_D_ID INT, --District number 
            O_ID INT, --Order number
            O_C_ID INT, --Customer number 

            C_FIRST VARCHAR, --Customer name 
            C_MIDDLE VARCHAR, --Customer name 
            C_LAST VARCHAR, --Customer name 

            O_CARRIER_ID INT, --Identifier of carrier who delivered the order 
            O_OL_CNT DECIMAL, --Number of items ordered
            O_ALL_LOCAL DECIMAL, --Order status (whether order includes only home order-lines) 
            O_ENTRY_D TIMESTAMP, --Order entry data and time 
            PRIMARY KEY ((O_W_ID), O_D_ID, O_ID)
    )""")

    index_query = """CREATE INDEX IF NOT EXISTS o_c_id_idx ON Order_Customer (O_C_ID)"""
    

    session.execute(index_query)
    

    session.execute("""
        CREATE TABLE IF NOT EXISTS Item (
            I_ID INT, --Item identifier 
            I_NAME VARCHAR, --Item name 
            I_PRICE DECIMAL, --Item price 
            I_IM_ID INT, --Item image identifier 
            I_DATA VARCHAR, --Brand information 
            PRIMARY KEY(I_ID)
    )""")

    session.execute("""
        CREATE TABLE IF NOT EXISTS Order_Line (
            OL_W_ID INT, --Warehouse number 
            OL_D_ID INT, --District number 
            OL_O_ID INT, --Order number 
            OL_NUMBER INT, --Order-line number 
            OL_I_ID INT, --Item number 

            I_NAME VARCHAR, --Item name 

            OL_DELIVERY_D TIMESTAMP, --Date and time of delivery 
            OL_AMOUNT DECIMAL, --Total price for ordered item 
            OL_SUPPLY_W_ID INT, --Supplying warehouse number 
            OL_QUANTITY DECIMAL, --Quantity ordered 
            OL_DIST_INFO VARCHAR, --Miscellaneous data
            PRIMARY KEY ((OL_W_ID), OL_D_ID, OL_O_ID,OL_NUMBER)
    )""")

    session.execute("""
        CREATE TABLE IF NOT EXISTS Stock_Item (
            S_W_ID INT, --Warehouse number 
            S_I_ID INT, --Item number 

            I_NAME VARCHAR, --Item name 
            I_PRICE DECIMAL, --Item price 

            S_QUANTITY DECIMAL, --Quantity in stock for item 
            S_YTD DECIMAL, --Year to date total quantity ordered 
            S_ORDER_CNT INT, --Number of orders 
            S_REMOTE_CNT INT, --Number of remote orders 
            S_DIST_01 VARCHAR, --Information on district 1s stock 
            S_DIST_02 VARCHAR, --Information on district 2s stock 
            S_DIST_03 VARCHAR, --Information on district 3s stock 
            S_DIST_04 VARCHAR, --Information on district 4s stock 
            S_DIST_05 VARCHAR, --Information on district 5s stock 
            S_DIST_06 VARCHAR, --Information on district 6s stock 
            S_DIST_07 VARCHAR, --Information on district 7s stock 
            S_DIST_08 VARCHAR, --Information on district 8s stock 
            S_DIST_09 VARCHAR, --Information on district 9s stock 
            S_DIST_10 VARCHAR, --Information on district 10s stock 
            S_DATA VARCHAR, --Miscellaneous data 
            PRIMARY KEY ((S_W_ID), S_I_ID)
    )""")

    session.execute("""
        CREATE TABLE IF NOT EXISTS Related_Customer_Order_Line_Table (
            W_ID INT, --Warehouse number
            D_ID INT, --District number 
            O_ID INT, --Order number
            OL_NUMBER INT, --Order Line number
            I_ID INT, --Item number
            C_ID INT, --Customer number
             
            PRIMARY KEY ((W_ID), I_ID, D_ID, O_ID, OL_NUMBER)
    )""")

    session.execute("""
        CREATE TABLE IF NOT EXISTS Customer_Balance_Table (
            W_ID INT, --Warehouse number
            D_ID INT, --District number 
            C_ID INT, --Customer number
            C_BALANCE DECIMAL, --Customer balance
            C_FIRST VARCHAR, --Customer name 
            C_MIDDLE VARCHAR, --Customer name 
            C_LAST VARCHAR, --Customer name 
            W_NAME VARCHAR, --Warehouse name
            D_NAME VARCHAR, --District name
             
            PRIMARY KEY ((W_ID), C_BALANCE, D_ID, C_ID)
    )""")

    session.execute("""
        CREATE TABLE IF NOT EXISTS Order_Customer_No_Carrier (
            W_ID INT, --Warehouse number
            D_ID INT, --District number 
            O_ID INT, --Order number
            C_ID INT, --Customer number
             
            PRIMARY KEY ((W_ID), D_ID, O_ID)
    )""")



if __name__ == "__main__":
    cluster, session = get_conn_details()
    empty_tables(cluster,session)
    create_tables(session)
    session.shutdown()
    cluster.shutdown()