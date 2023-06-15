import pandas as pd
import csv
from datetime import datetime, timedelta, date
import random
import psycopg2



conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="retail",
    user="postgres",
    password="1234"
)
cursor=conn.cursor()

# Execute the SQL query to count the number of rows in the table
query = f"SELECT stockqty FROM stocks WHERE stockdate=(SELECT MAX(stockdate) FROM stocks);"
cursor.execute(query)


# Fetch the result
results = cursor.fetchall()
s=[]
for result in results:
    s.append(result[0])

query = "SELECT MAX(orderid) from orders;"
cursor.execute(query)
id=cursor.fetchone()[0]
print(id)

query = "SELECT max(orderdate) FROM ORDERS;"
cursor.execute(query)
today = cursor.fetchone()[0]
today+=timedelta(days=1)
print(today)
