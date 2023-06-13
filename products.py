from shortuuid import ShortUUID
from random import choice
import csv

brands = ["HP","Acer","Dell","Visio","Thinkpad","Mac Pro","Asus"]
rams = ["X32","X64","X128"]
disks=["SSD","HDD"]
header = ["ProdId","ProdCode","ProdBrand","ProdCost","ProdPrice"]
costs = [300,400,500,600,700,800]
prices=[900,1000,1200,1500,2000,2500]

with open("products.csv", mode="w") as file:
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