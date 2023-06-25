import psycopg2
import csv
import time


def create_postgresql_database(database_name):
    # Connect to the default PostgreSQL database
    default_db = psycopg2.connect(
        host="localhost",
        port="5432",
        user="postgres",
        password="1234"
    )
    default_db.autocommit = True
    default_cursor = default_db.cursor()

    # Create the new database
    default_cursor.execute(f'CREATE DATABASE {database_name}')
    default_cursor.close()
    default_db.close()

    print(f"PostgreSQL database '{database_name}' created successfully.")




def postgre_initial_orders_stocks(db):
    t0= time.time()
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database=db,
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

    with open("data_generated/orders.csv", mode="r") as file:
        reader=csv.reader(file)
        next(reader)

        for row in reader:
            insert_query = "INSERT INTO Orders VALUES (%s, %s, %s, %s, %s, %s)" 
            cursor.execute(insert_query, row)
    t1 = time.time()
    print(f"Orders took {t1-t0} s")
    with open("data_generated/stocks.csv", mode="r") as file:
        reader=csv.reader(file)
        next(reader)

        for row in reader:
            insert_query = "INSERT INTO Stocks VALUES (%s, %s, %s)" 
            cursor.execute(insert_query, row)

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Stocks took {time.time()-t1} s")

import psycopg2
import csv



def create_postgresql_database(database_name):
    # Connect to the default PostgreSQL database
    default_db = psycopg2.connect(
        host="localhost",
        port="5432",
        user="postgres",
        password="1234"
    )
    default_db.autocommit = True
    default_cursor = default_db.cursor()

    # Create the new database
    default_cursor.execute(f'CREATE DATABASE {database_name}')
    default_cursor.close()
    default_db.close()

    print(f"PostgreSQL database '{database_name}' created successfully.")



def postgre_initialisaion_customers_products(db):
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database=db,
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

    create_prod = """
    CREATE TABLE Products (
        ProdId INT,
        ProdCode VARCHAR(100),
        ProdBrand VARCHAR(100),
        ProdCost INT,
        ProdPrice INT
    )
    """

    cursor.execute(create_customer)
    cursor.execute(create_prod)

    with open("data_generated/customers.csv", mode="r") as file:
        reader=csv.reader(file)
        next(reader)

        for row in reader:
            insert_query = "INSERT INTO Customers VALUES (%s, %s, %s, %s)" 
            cursor.execute(insert_query, row)

    with open("data_generated/products.csv", mode="r") as file:
        reader=csv.reader(file)
        next(reader)

        for row in reader:
            insert_query = "INSERT INTO Products VALUES (%s, %s, %s, %s, %s)" 
            cursor.execute(insert_query, row)

    conn.commit()
    cursor.close()
    conn.close()
    print("products and customers have been successfully uploaded")