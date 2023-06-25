import csv
from datetime import  timedelta
import random
import psycopg2
import smtplib
from email.message import EmailMessage
import ssl
import smtplib
from Z_db import database

db = database

conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database=db,
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


header_stock = ["StockDate","ProdId","StockQty"]
qtes = [1,2,5,6,8,10,12,14,15,20,25]
header=["OrderId","OrderDate","ProdId","CustId","Qty","IsHonored"]
with open("data_generated/orders"+"-"+str(today)+".csv",mode="w") as file:
  writer = csv.writer(file)
  writer.writerow(header)
  for index,item in enumerate(s):
      if item <15:
        s[index]=50
  n=random.randint(500,1000)
  for i in range(n):
    row=[]
    id+=1
    row.append(id)
    row.append(today)
    product=random.randint(1,40)
    row.append(product)
    customer=random.randint(1,100)
    row.append(customer)
    qty=random.choice(qtes)
    row.append(qty)
    if s[product-1]>qty:
      s[product-1]-=qty
      IsHonored=True
    else:
      IsHonored=False
    row.append(IsHonored)
    writer.writerow(row)
    insert_query = "INSERT INTO Orders VALUES (%s, %s, %s, %s, %s, %s)" 
    cursor.execute(insert_query, row)
with open("data_generated/stocks"+"-"+str(today)+".csv", mode="w") as stocks:
  writer_stock = csv.writer(stocks)
  writer_stock.writerow(header_stock)
  for index, product in enumerate(s):
    stock=[]
    stock.append(today)
    stock.append(index+1)
    stock.append(product)
    writer_stock.writerow(stock)
    insert_query = "INSERT INTO Stocks VALUES (%s, %s, %s)" 
    cursor.execute(insert_query, stock)

conn.commit()
cursor.close()
conn.close()


email_sender = "medabdallahi.karim@gmail.com"
with open("gmail.txt", "r") as psw:
   email_psw = psw.read()
   
email_reciever = "mabdkarim@outlook.fr"
subject = f"Data {today}"
body = f"""
Bonjour,

L'exécution est un success nous avons inséré {n} lignes avec la date {today}

Mohamed KARIM
"""
em = EmailMessage()
em["From"]=email_sender
em["To"]=email_reciever
em["Subject"]=subject
em.set_content(body)

context=ssl.create_default_context()

with smtplib.SMTP_SSL("smtp.gmail.com",465, context=context) as smtp:
    smtp.login(email_sender,email_psw)
    smtp.sendmail(email_sender,email_reciever,em.as_string())

print("Success!")
