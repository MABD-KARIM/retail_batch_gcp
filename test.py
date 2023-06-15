import psycopg2
import csv

# PostgreSQL connection details
host = 'localhost'
database = 'retail'
user = 'postgres'
password = '1234'

# Establish a connection to the PostgreSQL database
connection = psycopg2.connect(
    host=host,
    database=database,
    user=user,
    password=password
)

# Create a cursor object to interact with the database
cursor = connection.cursor()

# SQL query to fetch data from the table
query = "SELECT * FROM customers"

# Execute the query
cursor.execute(query)

print(cursor.description)

cursor.close()
connection.close()
