import psycopg2

def get_connection(name):
    return psycopg2.connect(
    host="localhost",
    dbname=name,
    user="cs4224b",
    password="",
    port="5098"
    )