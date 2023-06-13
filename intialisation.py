import psycopg2
import csv

conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="retail",
    user="postgres",
    password="1234"
)
cursor=conn.cursor()

create_customer = """
CREATE TABLE Customers (
    CustId INT,
    CustFirstName VARCHAR(100),
    CustLastName VARCHAR(100),
    CustCity VARCHAR(100)
)
"""

cursor.execute(create_customer)

with open("customers.csv", mode="r") as file:
    reader=csv.reader(file)
    next(reader)

    for row in reader:
        insert_query = "INSERT INTO Customers VALUES (%s, %s, %s, %s)" 
        cursor.execute(insert_query, row)

conn.commit()
cursor.close()
conn.close()