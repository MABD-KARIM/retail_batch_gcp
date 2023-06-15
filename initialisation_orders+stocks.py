import psycopg2
import csv
import time

t0= time.time()
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="retail",
    user="postgres",
    password="1234"
)
cursor=conn.cursor()

create_orders = """
CREATE TABLE IF NOT EXISTS Orders (
    OrderId INT,
    OrderDate DATE,
    ProdId INT,
    CustId INT,
    Qty INT,
    IsHonored BOOLEAN
)
"""

create_stocks = """
CREATE TABLE IF NOT EXISTS Stocks (
    StockDate DATE,
    ProdId INT,
    StockQty INT
)
"""

cursor.execute(create_orders)
cursor.execute(create_stocks)

with open("orders.csv", mode="r") as file:
    reader=csv.reader(file)
    next(reader)

    for row in reader:
        insert_query = "INSERT INTO Orders VALUES (%s, %s, %s, %s, %s, %s)" 
        cursor.execute(insert_query, row)
t1 = time.time()
print(f"Orders took {t1-t0} s")
with open("stocks.csv", mode="r") as file:
    reader=csv.reader(file)
    next(reader)

    for row in reader:
        insert_query = "INSERT INTO Stocks VALUES (%s, %s, %s)" 
        cursor.execute(insert_query, row)

conn.commit()
cursor.close()
conn.close()
print(f"Orders took {time.time()-t1} s")