import psycopg2

conn = psycopg2.connect(
    dbname="payments",
    user="thor",
    password="th0r*2025",
    host="127.0.0.1",
    port="5435"
)

cursor = conn.cursor()