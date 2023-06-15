import psycopg2
import csv


def extract_csv(query, file_name):


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

    # Execute the query
    cursor.execute(query)

    # Fetch all the rows returned by the query
    rows = cursor.fetchall()

    # Define the CSV file path
    csv_file_path = 'sources/'+file_name+'.csv'

    # Open the CSV file in write mode
    with open(csv_file_path, 'w', newline='') as csvfile:
        # Create a CSV writer object
        csv_writer = csv.writer(csvfile)

        # Write the column headers to the CSV file
        csv_writer.writerow([desc[0] for desc in cursor.description])

        # Write each row to the CSV file
        csv_writer.writerows(rows)
    print(f"We have ectacted the {file_name} table with {len(rows)} line")
    cursor.close()
    connection.close()

query = 'SELECT * FROM customers'
extract_csv(query, "customers")

query = 'SELECT * FROM products'
extract_csv(query, "products")

query = """SELECT * FROM orders where orderdate = (SELECT MAX(orderdate) FROM orders)-10
"""
extract_csv(query, "orders")

query = """SELECT *
FROM stocks 
where stockdate = (SELECT MAX(stockdate) FROM stocks)-10
"""
extract_csv(query, "stocks")
