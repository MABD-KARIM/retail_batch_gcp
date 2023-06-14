import pandas as pd
import csv
from datetime import datetime, timedelta, date
import random
import psycopg2

today=datetime.today().date()
yesterday = datetime().today().timed
print(today)

conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="retail",
    user="postgres",
    password="1234"
)
cursor=conn.cursor()

# Execute the SQL query to count the number of rows in the table
query = "SELECT stockqty FROM stocks WHERE stockdate=;"
cursor.execute(query)

# Fetch the result
row_count = cursor.fetchone()[0]

header = ["StockDate","ProdId","StockQty"]
with open("stocks.csv", mode="w") as file:
  writer = csv.writer(file)
  writer.writerow(header)
  for i in range(1,41):
    row=[]
    row.append("2019-12-31")
    row.append(i)
    row.append(100)
    writer.writerow(row)