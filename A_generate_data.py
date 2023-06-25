from faker import Faker
from shortuuid import ShortUUID
from random import choice, randint
import csv
from datetime import datetime, timedelta, date


def generate_customers():
  fake=Faker()
  cities = ["Paris", "Nantes","Lyon","Lille","Marseille","Bordeaux","Toulouse","Rennes","Rouen","Paris","Paris","Paris","Lille","Lille","Lyon","Lyon","Paris"]
  header = ["CustId","CustFirstName","CustLastName","CustCity"]
  with open("data_generated/customers.csv", mode="w") as file:
    writer = csv.writer(file)
    writer.writerow(header)
    for id in range(1,101):
      row=[]
      row.append(id)
      fname=fake.first_name()
      row.append(fname)
      lname=fake.last_name()
      row.append(lname)
      city = choice(cities)
      row.append(city)
      writer.writerow(row)




def generate_products():
  brands = ["HP","Acer","Dell","Visio","Thinkpad","Mac Pro","Asus"]
  rams = ["X32","X64","X128"]
  disks=["SSD","HDD"]
  header = ["ProdId","ProdCode","ProdBrand","ProdCost","ProdPrice"]
  costs = [300,400,500,600,700,800]
  prices=[900,1000,1200,1500,2000,2500]

  with open("data_generated/products.csv", mode="w") as file:
    writer = csv.writer(file)
    writer.writerow(header)
    for id in range(1,41):
      row=[]
      row.append(id)
      brand = choice(brands)
      ram = choice(rams)
      disk = choice(disks)
      x = ShortUUID().random(length=5)
      code = brand+"-"+ram+"-"+disk+"-"+str(x)
      row.append(code)
      row.append(brand)
      cost = choice(costs)
      row.append(cost)
      price=choice(prices)
      row.append(price)
      writer.writerow(row)

def generate_orders_stocks():
  header = ["StockDate","ProdId","StockQty"]
  with open("data_generated/stocks.csv", mode="w") as file:
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
  with open("data_generated/orders.csv",mode="w") as file:
    writer = csv.writer(file)
    writer.writerow(header)
    while debut!= date.today():
      for index,item in enumerate(s):
        if item <15:
          s[index]=50
      n=randint(500,1000)
      for i in range(n):
        row=[]
        id+=1
        row.append(id)
        row.append(debut)
        product=randint(1,40)
        row.append(product)
        customer=randint(1,100)
        row.append(customer)
        qty=choice(qtes)
        row.append(qty)
        if s[product-1]>qty:
          IsHonored=True
          s[product-1]-=qty
        else:
          IsHonored=False
        row.append(IsHonored)
        writer.writerow(row)
      with open("data_generated/stocks.csv", mode="a") as stocks:
        writer_stock = csv.writer(stocks)
        for index, product in enumerate(s):
          stock=[]
          stock.append(debut)
          stock.append(index+1)
          stock.append(product)
          writer_stock.writerow(stock)
      debut+=timedelta(days=1)