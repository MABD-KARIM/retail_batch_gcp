import pandas as pd
import csv
from datetime import datetime, timedelta, date
import random

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
s=[100 for i in range(40)]
qtes = [1,2,5,6,8,10,12,14,15,20,25]
debut = datetime(2020,1,1).date()
header=["OrderId","OrderDate","ProdId","CustId","Qty","IsHonored"]
id=0
with open("orders.csv",mode="w") as file:
  writer = csv.writer(file)
  writer.writerow(header)
  while debut!= date.today():
    n=random.randint(500,1000)
    for i in range(n):
      row=[]
      id+=1
      row.append(id)
      row.append(debut)
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
    with open("stocks.csv", mode="a") as stocks:
      writer_stock = csv.writer(stocks)
      for index, product in enumerate(s):
        stock=[]
        stock.append(debut)
        stock.append(index+1)
        if product<30:
          product=100
        stock.append(product)
        writer_stock.writerow(stock)
    debut+=timedelta(days=1)
with open("last_line.csv", mode="w") as file:
  writer=csv.writer(file)
  writer.writerow([id])
