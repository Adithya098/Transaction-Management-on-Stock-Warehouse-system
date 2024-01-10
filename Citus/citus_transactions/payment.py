import psycopg2
import os
def payment_transaction(
    w_id, d_id, c_id, payment, conn):
    cur = conn.cursor()
    
    payment = float(payment)
    cur.execute("UPDATE test_warehouses SET w_ytd = w_ytd + %s WHERE w_id = %s RETURNING w_street_1, w_street_2, w_city, w_state, w_zip",
                (payment, w_id))
    
    street_1, street_2, city, state, zip = cur.fetchone()
    cur.execute("UPDATE test_districts SET d_ytd = d_ytd + %s WHERE d_w_id = %s AND d_id = %s RETURNING d_street_1, d_street_2, d_city, d_state, d_zip",
                (payment, w_id, d_id))   
    
    d_street_1, d_street_2, d_city, d_state, d_zip = cur.fetchone()
    
    cur.execute("""UPDATE test_customers SET c_balance = c_balance - %s, c_ytd_payment = c_ytd_payment + %s, c_payment_cnt = c_payment_cnt + 1
                WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s RETURNING c_first, c_middle, c_last, c_street_1, c_street_2,
                c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance""",
                (payment, payment, w_id, d_id, c_id))
    
    c_first , c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance = cur.fetchone()
    
    # print(f"The customer identifier is {w_id}, {d_id}, {c_id},  detais are {c_first} {c_middle} {c_last}, {c_street_1}, {c_street_2}, {c_city}, {c_state}, {c_zip}, {c_phone}, {c_since}, {c_credit}, {c_credit_lim}, {c_discount}, {c_balance}")
    print(f"{w_id}, {d_id}, {c_id}, {c_first}, {c_middle}, {c_last}, {c_street_1}, {c_street_2}, {c_city}, {c_state}, {c_zip}, {c_phone}, {c_since}, {c_credit}, {c_credit_lim}, {c_discount}, {c_balance}")
    # print(f"The warehouse identifier is {w_id}, details are is {street_1}, {street_2}, {city}, {state}, {zip}")
    print(f"{street_1}, {street_2}, {city}, {state}, {zip}")
    print(f"{d_street_1}, {d_street_2}, {d_city}, {d_state}, {d_zip}")
    # print(f"The district identifier is {w_id}, {d_id}, details are {d_street_1}, {d_street_2}, {d_city}, {d_state}, {d_zip}")
    # print(f"The payment is {payment}")
    print(f"{payment}")  
    testing = os.environ.get('TESTING')
    conn.commit()
    cur.close()
    # conn.close()
    
# payment_transaction(1, 2, 3, 100)
    
